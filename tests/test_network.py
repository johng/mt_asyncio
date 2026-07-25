"""Differential networking tests: ``mt_asyncio.asyncio`` must behave like ``asyncio``.

Same contract as ``test_parity.py`` -- every scenario runs against both stdlib
``asyncio`` and ``mt_asyncio.asyncio`` and must produce the same observable
result. A divergence is a bug in us unless it is documented in
``COMPATIBILITY.md``.

These do not need ``compat.install()``: each test drives the backend module
directly, and third-party libraries (which do need it) are covered separately in
``test_netlibs.py``.
"""

import asyncio
import socket
import ssl
import threading
import time

import pytest

import mt_asyncio.asyncio as taio


BACKENDS = [pytest.param(asyncio, id='stdlib'), pytest.param(taio, id='mt_asyncio')]

TIMEOUT = 15


@pytest.fixture(params=BACKENDS)
def aio(request):
    return request.param


@pytest.fixture
def sockpair():
    a, b = socket.socketpair()
    a.setblocking(False)
    yield a, b
    a.close()
    b.close()


@pytest.fixture(scope='module')
def tls_certs():
    trustme = pytest.importorskip('trustme')
    ca = trustme.CA()
    cert = ca.issue_cert('localhost')
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    cert.configure_cert(server_ctx)
    client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ca.configure_trust(client_ctx)
    return server_ctx, client_ctx


# --------------------------------------------------------------------------
# add_reader / add_writer
# --------------------------------------------------------------------------


def test_add_reader_fires_on_data(aio, sockpair):
    a, b = sockpair
    seen = []

    async def main():
        loop = aio.get_running_loop()
        ev = aio.Event()

        def on_read():
            seen.append(a.recv(1024))
            ev.set()

        loop.add_reader(a, on_read)
        try:
            b.sendall(b'hello')
            await aio.wait_for(ev.wait(), TIMEOUT)
        finally:
            loop.remove_reader(a)

    aio.run(main())
    assert seen == [b'hello']


def test_add_reader_passes_args(aio, sockpair):
    a, b = sockpair
    seen = []

    async def main():
        loop = aio.get_running_loop()
        ev = aio.Event()

        def on_read(tag, other):
            a.recv(1024)
            seen.append((tag, other))
            ev.set()

        loop.add_reader(a, on_read, 'tag', 7)
        try:
            b.sendall(b'x')
            await aio.wait_for(ev.wait(), TIMEOUT)
        finally:
            loop.remove_reader(a)

    aio.run(main())
    assert seen == [('tag', 7)]


def test_no_spurious_callback_after_drain(aio, sockpair):
    """The regression the poll() probe exists for.

    Our reactor is edge-triggered with a cached readiness bit. After the
    callback drains the socket through its own syscall -- which we never see --
    the bit is stale, and a naive re-arm fires forever.
    """
    a, b = sockpair
    hits = []

    async def main():
        loop = aio.get_running_loop()
        ev = aio.Event()

        def on_read():
            hits.append(a.recv(1024))
            ev.set()

        loop.add_reader(a, on_read)
        b.sendall(b'one')
        await aio.wait_for(ev.wait(), TIMEOUT)
        loop.remove_reader(a)

        # re-arm with nothing pending: must stay quiet
        ev.clear()
        loop.add_reader(a, on_read)
        try:
            await aio.wait_for(ev.wait(), 0.3)
        except (TimeoutError, asyncio.TimeoutError):
            pass
        quiet = len(hits)

        # ...and must still wake when data really arrives
        b.sendall(b'two')
        await aio.wait_for(ev.wait(), TIMEOUT)
        loop.remove_reader(a)
        return quiet

    quiet = aio.run(main())
    assert quiet == 1, f'{quiet - 1} spurious callback(s) after a full drain'
    assert hits == [b'one', b'two']


def test_reader_is_level_triggered(aio, sockpair):
    """One recv() per callback must keep being called while data remains.

    asyncio's contract is level-triggered. A consumer that reads a fixed chunk
    per callback relies on it, and no further edge is coming.
    """
    a, b = sockpair
    payload = b'x' * 300_000
    chunks = []

    async def main():
        loop = aio.get_running_loop()
        done = aio.Event()

        def on_read():
            try:
                data = a.recv(4096)  # deliberately smaller than what is pending
            except BlockingIOError:
                return
            chunks.append(data)
            if sum(map(len, chunks)) >= len(payload):
                done.set()

        loop.add_reader(a, on_read)
        feeder = threading.Thread(target=lambda: b.sendall(payload))
        feeder.start()
        try:
            await aio.wait_for(done.wait(), TIMEOUT)
        finally:
            loop.remove_reader(a)
            feeder.join()

    aio.run(main())
    assert sum(map(len, chunks)) == len(payload)
    assert len(chunks) > 1, 'callback fired only once: not level-triggered'


def test_add_writer_fires_when_writable(aio, sockpair):
    a, _b = sockpair
    fired = []

    async def main():
        loop = aio.get_running_loop()
        ev = aio.Event()

        def on_write():
            fired.append(1)
            ev.set()

        loop.add_writer(a, on_write)
        try:
            await aio.wait_for(ev.wait(), TIMEOUT)
        finally:
            loop.remove_writer(a)

    aio.run(main())
    assert fired


def test_remove_reader_reports_whether_it_removed(aio, sockpair):
    a, _b = sockpair

    async def main():
        loop = aio.get_running_loop()
        first = loop.remove_reader(a)
        loop.add_reader(a, lambda: None)
        second = loop.remove_reader(a)
        third = loop.remove_reader(a)
        return first, second, third

    assert aio.run(main()) == (False, True, False)


def test_add_reader_replaces_previous_callback(aio, sockpair):
    a, b = sockpair
    calls = []

    async def main():
        loop = aio.get_running_loop()
        ev = aio.Event()

        def first():
            calls.append('first')
            a.recv(1024)
            ev.set()

        def second():
            calls.append('second')
            a.recv(1024)
            ev.set()

        loop.add_reader(a, first)
        loop.add_reader(a, second)
        try:
            b.sendall(b'x')
            await aio.wait_for(ev.wait(), TIMEOUT)
        finally:
            loop.remove_reader(a)

    aio.run(main())
    assert calls == ['second']


def test_removed_reader_stops_firing(aio, sockpair):
    a, b = sockpair
    calls = []

    async def main():
        loop = aio.get_running_loop()
        loop.add_reader(a, lambda: calls.append(1))
        loop.remove_reader(a)
        b.sendall(b'data')
        await aio.sleep(0.25)

    aio.run(main())
    assert calls == []


def test_many_waiting_fds_with_few_workers(aio):
    """More concurrently-waiting fds than worker threads must still progress.

    The regression this pins: a callback that does *not* drain the fd (psycopg's
    `wakeup` only sets an Event; libpq reads later, from the task) leaves it
    readable, so the level-triggered re-arm dispatches again immediately. If that
    re-dispatch lands on the calling worker's own queue it is a closed loop --
    the worker pops it straight back off and the task waiting to consume the data
    never gets scheduled. With as many such fds as workers, the runtime
    deadlocked. Every other add_reader test here uses a single fd and passed
    throughout.
    """
    fds, rounds = 16, 10  # conftest builds a 4-worker runtime
    pairs = [socket.socketpair() for _ in range(fds)]
    for a, _b in pairs:
        a.setblocking(False)

    async def main():
        loop = aio.get_running_loop()

        async def peer(a, b):
            for _ in range(rounds):
                ev = aio.Event()
                loop.add_reader(a, ev.set)  # deliberately does not read
                try:
                    b.sendall(b'ping')
                    await aio.wait_for(ev.wait(), TIMEOUT)
                finally:
                    loop.remove_reader(a)
                a.recv(64)  # the task drains, not the callback
            return True

        return await aio.gather(*[peer(a, b) for a, b in pairs])

    try:
        assert aio.run(main()) == [True] * fds
    finally:
        for a, b in pairs:
            a.close()
            b.close()


def test_add_reader_rejects_fd_owned_by_transport(aio):
    """asyncio refuses to hand an fd to both a transport and a raw callback."""

    class Proto(asyncio.Protocol):
        pass

    async def main():
        loop = aio.get_running_loop()
        server = await loop.create_server(Proto, '127.0.0.1', 0)
        port = server.sockets[0].getsockname()[1]
        transport, _ = await loop.create_connection(Proto, '127.0.0.1', port)
        fd = transport.get_extra_info('socket').fileno()
        try:
            with pytest.raises(RuntimeError, match='used by transport'):
                loop.add_reader(fd, lambda: None)
        finally:
            transport.close()
            server.close()
            await server.wait_closed()

    aio.run(main())


# --------------------------------------------------------------------------
# transports: create_connection / create_server
# --------------------------------------------------------------------------


class Echo(asyncio.Protocol):
    """Upper-cases whatever it receives."""

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self.transport.write(data.upper())


class Collect(asyncio.Protocol):
    def __init__(self, expect, done):
        self.buf = bytearray()
        self.expect = expect
        self.done = done
        self.transport = None
        self.lost = None

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        self.buf.extend(data)
        if len(self.buf) >= self.expect:
            self.done.set()

    def eof_received(self):
        self.done.set()

    def connection_lost(self, exc):
        self.lost = exc
        self.done.set()


async def _serve(aio, factory=Echo, **kwargs):
    loop = aio.get_running_loop()
    server = await loop.create_server(factory, '127.0.0.1', 0, **kwargs)
    return server, server.sockets[0].getsockname()[1]


def test_transport_round_trip(aio):
    payload = b'hello world ' * 500

    async def main():
        loop = aio.get_running_loop()
        server, port = await _serve(aio)
        try:
            done = aio.Event()
            transport, proto = await loop.create_connection(lambda: Collect(len(payload), done), '127.0.0.1', port)
            transport.write(payload)
            await aio.wait_for(done.wait(), TIMEOUT)
            transport.close()
            return bytes(proto.buf)
        finally:
            server.close()
            await server.wait_closed()

    assert aio.run(main()) == payload.upper()


def test_many_concurrent_connections(aio):
    n = 40

    async def main():
        loop = aio.get_running_loop()
        server, port = await _serve(aio)
        try:

            async def one(i):
                msg = f'conn-{i:03d}-'.encode() * 40
                done = aio.Event()
                transport, proto = await loop.create_connection(lambda: Collect(len(msg), done), '127.0.0.1', port)
                transport.write(msg)
                await aio.wait_for(done.wait(), TIMEOUT)
                transport.close()
                return bytes(proto.buf) == msg.upper()

            return await aio.gather(*[one(i) for i in range(n)])
        finally:
            server.close()
            await server.wait_closed()

    assert aio.run(main()) == [True] * n


def test_large_transfer_is_not_reordered(aio):
    """A megabyte through the transport, checked byte for byte."""
    payload = bytes(range(256)) * 4096  # 1 MiB

    async def main():
        loop = aio.get_running_loop()
        server, port = await _serve(aio)
        try:
            done = aio.Event()
            transport, proto = await loop.create_connection(lambda: Collect(len(payload), done), '127.0.0.1', port)
            transport.write(payload)
            await aio.wait_for(done.wait(), TIMEOUT)
            transport.close()
            return bytes(proto.buf)
        finally:
            server.close()
            await server.wait_closed()

    assert aio.run(main()) == payload.upper()


def test_concurrent_writers_do_not_interleave(aio):
    """The property the per-connection lock exists to provide.

    Ten tasks write to one transport at once. Each writes 16-byte runs of a
    single distinguishing byte. asyncio guarantees the bytes of any one write()
    land contiguously and in order; if write() races _write_ready, a partial
    send is spliced apart and a run comes back broken.
    """
    writers, per_writer, run = 10, 40, 16
    total = writers * per_writer * run

    async def main():
        loop = aio.get_running_loop()
        server, port = await _serve(aio)
        try:
            done = aio.Event()
            transport, proto = await loop.create_connection(lambda: Collect(total, done), '127.0.0.1', port)

            async def writer(tag):
                chunk = bytes([tag]) * run
                for _ in range(per_writer):
                    transport.write(chunk)
                    await aio.sleep(0)

            await aio.gather(*[writer(65 + i) for i in range(writers)])
            await aio.wait_for(done.wait(), TIMEOUT)
            transport.close()
            return bytes(proto.buf)
        finally:
            server.close()
            await server.wait_closed()

    data = aio.run(main())
    assert len(data) == total

    # every maximal run of one byte must be a whole number of writes
    splits, i = 0, 0
    while i < len(data):
        byte, start = data[i], i
        while i < len(data) and data[i] == byte:
            i += 1
        if (i - start) % run:
            splits += 1
    assert splits == 0, f'{splits} interleaved/spliced writes'


def test_callback_finishes_before_the_task_it_woke_resumes(aio):
    """The other half of the per-connection contract, and asyncpg's shape exactly.

    Serialising a connection's callbacks is not enough on its own. asyncio also
    guarantees that a callback *completes* before any task it woke takes a step,
    because completing a future only schedules the awaiter. Protocols rely on
    that to finish tidying up after they have handed the result over --
    ``asyncpg``'s ``_push_result`` is::

        self._on_result()               # -> waiter.set_result(...)
        self._set_state(PROTOCOL_IDLE)  # ...only after

    Resume the awaiter the moment the result lands and it issues its next
    command against a protocol that still says a command is in flight. Real
    asyncpg raises ``InternalClientError: cannot switch to state 12`` here; the
    protocol below just records it.
    """
    conns, rounds = 4, 40
    msg = b'ping'
    # a real protocol has work left after handing the result over; this stands
    # in for asyncpg's `_set_state` + `_reset_result` and the rest of its parse
    # loop. Long enough that another worker would get the task if it could.
    tidy_up = 20000

    class PushResult(asyncio.Protocol):
        def __init__(self, loop):
            self.loop = loop
            self.buf = bytearray()
            self.waiter = None
            self.in_flight = False
            self.resumed = False
            self.violations = []
            self.transport = None

        def connection_made(self, transport):
            self.transport = transport

        def request(self):
            if self.in_flight:
                self.violations.append('request issued while the previous one was still in flight')
            self.in_flight = True
            self.waiter = self.loop.create_future()
            self.transport.write(msg)
            return self.waiter

        def data_received(self, data):
            self.buf.extend(data)
            while len(self.buf) >= len(msg):
                del self.buf[: len(msg)]
                waiter, self.waiter = self.waiter, None
                if waiter is None or waiter.done():
                    continue
                self.resumed = False
                waiter.set_result(True)
                for _ in range(tidy_up):
                    if self.resumed:
                        self.violations.append('awaiter resumed while data_received was still running')
                        break
                self.in_flight = False  # the ordering under test

    async def main():
        loop = aio.get_running_loop()
        server, port = await _serve(aio)
        try:

            async def one():
                transport, proto = await loop.create_connection(lambda: PushResult(loop), '127.0.0.1', port)
                try:
                    for _ in range(rounds):
                        await aio.wait_for(proto.request(), TIMEOUT)
                        proto.resumed = True
                finally:
                    transport.close()
                return proto.violations

            return [v for vs in await aio.gather(*[one() for _ in range(conns)]) for v in vs]
        finally:
            server.close()
            await server.wait_closed()

    assert aio.run(main()) == []


def test_reader_does_not_run_between_a_write_and_the_next_suspension(aio):
    """The mirror image, and the one that segfaults asyncpg.

    A write is not the end of a task's dealings with its protocol. asyncpg's
    ``bind_execute`` finishes setting itself up *after* the bytes are on the
    wire::

        self._bind_execute(portal_name, state.name, args_buf, limit)  # network op
        self.last_query = state.query
        self.statement = state          # <- the reply handler needs this

    On a single-threaded loop the reply cannot be processed in that window,
    because the loop only gets control back when the coroutine suspends. Take
    that away and ``data_received`` runs against a half-built request: real
    asyncpg finds ``self.statement`` still ``None`` and, since its guard is
    compiled out of release builds, calls into ``None`` as though it were a
    ``PreparedStatementState``.
    """
    conns, rounds = 6, 8
    msg = b'ping'
    # A duration, deliberately, not an iteration count. The window has two
    # bounds: it must outlast a loopback round trip (or the race is not run at
    # all) and stay well inside the reader's 50ms deferral backstop (or the
    # backstop fires and the read is delivered legitimately). An iteration count
    # cannot honour both -- it is ~1ms on a fast machine and well past 50ms on a
    # loaded 3-core CI runner with every connection spinning at once, which is
    # exactly how this test first failed. 10ms holds the margin on any machine,
    # and errs towards not catching the bug rather than towards a red build.
    settle = 0.010

    class WriteThenFinish(asyncio.Protocol):
        def __init__(self, loop):
            self.loop = loop
            self.buf = bytearray()
            self.waiter = None
            self.request_ready = None
            self.seen_early = False
            self.violations = []
            self.transport = None

        def connection_made(self, transport):
            self.transport = transport

        def request(self):
            waiter = self.waiter = self.loop.create_future()
            self.request_ready = None
            self.transport.write(msg)  # network op
            deadline = time.monotonic() + settle
            while time.monotonic() < deadline:
                if self.seen_early:
                    break
            self.request_ready = 'ready'
            return waiter

        def data_received(self, data):
            self.buf.extend(data)
            while len(self.buf) >= len(msg):
                del self.buf[: len(msg)]
                if self.request_ready is None:
                    self.seen_early = True
                    self.violations.append('reply parsed before the request had finished setting up')
                self.request_ready = None  # asyncpg clears it in `_on_result`
                waiter, self.waiter = self.waiter, None
                if waiter is not None and not waiter.done():
                    waiter.set_result(True)

    async def main():
        loop = aio.get_running_loop()
        server, port = await _serve(aio)
        try:

            async def one():
                transport, proto = await loop.create_connection(lambda: WriteThenFinish(loop), '127.0.0.1', port)
                try:
                    for _ in range(rounds):
                        await aio.wait_for(proto.request(), TIMEOUT)
                finally:
                    transport.close()
                return proto.violations

            return [v for vs in await aio.gather(*[one() for _ in range(conns)]) for v in vs]
        finally:
            server.close()
            await server.wait_closed()

    assert aio.run(main()) == []


def test_connection_lost_is_reported(aio):
    class Dropper(asyncio.Protocol):
        def connection_made(self, transport):
            transport.close()

    async def main():
        loop = aio.get_running_loop()
        server, port = await _serve(aio, Dropper)
        try:
            done = aio.Event()
            transport, proto = await loop.create_connection(lambda: Collect(1 << 30, done), '127.0.0.1', port)
            await aio.wait_for(done.wait(), TIMEOUT)
            transport.close()
            return proto.lost
        finally:
            server.close()
            await server.wait_closed()

    assert aio.run(main()) is None  # clean close, not an error


def test_create_connection_refused(aio):
    async def main():
        loop = aio.get_running_loop()
        # bind and immediately close, so the port is almost certainly dead
        s = socket.socket()
        s.bind(('127.0.0.1', 0))
        port = s.getsockname()[1]
        s.close()
        with pytest.raises(OSError):
            await loop.create_connection(asyncio.Protocol, '127.0.0.1', port)

    aio.run(main())


def test_server_stops_accepting_after_close(aio):
    async def main():
        loop = aio.get_running_loop()
        server, port = await _serve(aio)
        server.close()
        await server.wait_closed()
        with pytest.raises(OSError):
            await loop.create_connection(asyncio.Protocol, '127.0.0.1', port)

    aio.run(main())


def test_pause_and_resume_reading(aio):
    payload = b'z' * 200_000

    class Paused(asyncio.Protocol):
        def __init__(self, done):
            self.buf = bytearray()
            self.done = done
            self.resumed = False

        def connection_made(self, transport):
            self.transport = transport
            transport.pause_reading()

        def data_received(self, data):
            self.buf.extend(data)
            if len(self.buf) >= len(payload):
                self.done.set()

    async def main():
        loop = aio.get_running_loop()
        server, port = await _serve(aio)
        try:
            done = aio.Event()
            proto_ref = []

            def factory():
                p = Paused(done)
                proto_ref.append(p)
                return p

            transport, proto = await loop.create_connection(factory, '127.0.0.1', port)
            transport.write(payload)
            await aio.sleep(0.2)
            paused_len = len(proto.buf)
            proto.transport.resume_reading()
            await aio.wait_for(done.wait(), TIMEOUT)
            transport.close()
            return paused_len, len(proto.buf)
        finally:
            server.close()
            await server.wait_closed()

    paused_len, final = aio.run(main())
    assert paused_len == 0, 'data delivered while reading was paused'
    assert final == len(payload)


# --------------------------------------------------------------------------
# streams
# --------------------------------------------------------------------------


def test_streams_round_trip(aio):
    async def main():
        async def handle(reader, writer):
            while True:
                data = await reader.read(4096)
                if not data:
                    break
                writer.write(data.upper())
                await writer.drain()
            writer.close()

        server = await aio.start_server(handle, '127.0.0.1', 0)
        port = server.sockets[0].getsockname()[1]
        try:

            async def client(i):
                reader, writer = await aio.open_connection('127.0.0.1', port)
                msg = f'msg-{i}-'.encode() * 150
                writer.write(msg)
                await writer.drain()
                got = await reader.readexactly(len(msg))
                writer.close()
                return got == msg.upper()

            return await aio.gather(*[client(i) for i in range(25)])
        finally:
            server.close()
            await server.wait_closed()

    assert all(aio.run(main()))


def test_streams_readexactly_larger_than_limit(aio):
    """readexactly(n) for n > limit exercises the pause/resume + wait path."""
    size = 300_000

    async def main():
        async def handle(reader, writer):
            writer.write(b'q' * size)
            await writer.drain()
            writer.close()

        server = await aio.start_server(handle, '127.0.0.1', 0, limit=4096)
        port = server.sockets[0].getsockname()[1]
        try:
            reader, writer = await aio.open_connection('127.0.0.1', port, limit=4096)
            data = await aio.wait_for(reader.readexactly(size), TIMEOUT)
            writer.close()
            return data
        finally:
            server.close()
            await server.wait_closed()

    assert aio.run(main()) == b'q' * size


def test_streams_readline(aio):
    lines = [f'line-{i}\n'.encode() for i in range(200)]

    async def main():
        async def handle(reader, writer):
            for line in lines:
                writer.write(line)
            await writer.drain()
            writer.close()

        server = await aio.start_server(handle, '127.0.0.1', 0)
        port = server.sockets[0].getsockname()[1]
        try:
            reader, writer = await aio.open_connection('127.0.0.1', port)
            got = [await aio.wait_for(reader.readline(), TIMEOUT) for _ in lines]
            writer.close()
            return got
        finally:
            server.close()
            await server.wait_closed()

    assert aio.run(main()) == lines


def test_streams_eof(aio):
    async def main():
        async def handle(reader, writer):
            writer.write(b'bye')
            await writer.drain()
            writer.close()

        server = await aio.start_server(handle, '127.0.0.1', 0)
        port = server.sockets[0].getsockname()[1]
        try:
            reader, writer = await aio.open_connection('127.0.0.1', port)
            data = await aio.wait_for(reader.read(-1), TIMEOUT)
            writer.close()
            return data
        finally:
            server.close()
            await server.wait_closed()

    assert aio.run(main()) == b'bye'


# --------------------------------------------------------------------------
# TLS
# --------------------------------------------------------------------------


def test_tls_round_trip(aio, tls_certs):
    server_ctx, client_ctx = tls_certs
    payload = b'secret payload ' * 100

    async def main():
        async def handle(reader, writer):
            data = await reader.readexactly(len(payload))
            writer.write(data.upper())
            await writer.drain()
            writer.close()

        server = await aio.start_server(handle, '127.0.0.1', 0, ssl=server_ctx)
        port = server.sockets[0].getsockname()[1]
        try:

            async def client():
                reader, writer = await aio.open_connection(
                    '127.0.0.1', port, ssl=client_ctx, server_hostname='localhost'
                )
                writer.write(payload)
                await writer.drain()
                got = await reader.readexactly(len(payload))
                writer.close()
                return got == payload.upper()

            return await aio.gather(*[client() for _ in range(15)])
        finally:
            server.close()
            await server.wait_closed()

    assert all(aio.run(main()))


def test_tls_rejects_untrusted_certificate(aio, tls_certs):
    server_ctx, _ = tls_certs

    async def main():
        async def handle(reader, writer):
            writer.close()

        server = await aio.start_server(handle, '127.0.0.1', 0, ssl=server_ctx)
        port = server.sockets[0].getsockname()[1]
        try:
            strict = ssl.create_default_context()  # does not trust our test CA
            with pytest.raises(ssl.SSLError):
                await aio.wait_for(
                    aio.open_connection('127.0.0.1', port, ssl=strict, server_hostname='localhost'), TIMEOUT
                )
        finally:
            server.close()
            await server.wait_closed()

    aio.run(main())


def test_tls_large_transfer(aio, tls_certs):
    server_ctx, client_ctx = tls_certs
    size = 1 << 20

    async def main():
        async def handle(reader, writer):
            writer.write(b'k' * size)
            await writer.drain()
            writer.close()

        server = await aio.start_server(handle, '127.0.0.1', 0, ssl=server_ctx)
        port = server.sockets[0].getsockname()[1]
        try:
            reader, writer = await aio.open_connection('127.0.0.1', port, ssl=client_ctx, server_hostname='localhost')
            data = await aio.wait_for(reader.readexactly(size), TIMEOUT)
            writer.close()
            return data
        finally:
            server.close()
            await server.wait_closed()

    assert aio.run(main()) == b'k' * size


def test_tls_survives_repeated_read_pauses(aio, tls_certs):
    """``sslproto`` defers two callbacks straight back into its state machine.

    ``_resume_reading`` schedules a closure and ``_do_read__buffered`` schedules
    ``self._do_read``; neither goes through a method the connection lock wraps,
    so both land on an arbitrary worker with nothing held. What they reach is::

        data = self._outgoing.read()   # take what the BIO has
        self._transport.write(data)    # and put it on the wire

    Two threads in there take a slice each and write them back in whichever
    order they reach the socket, and the peer reports ``RECORD_LAYER_FAILURE``
    or reads short where a record boundary was lost.

    A small stream limit makes the reader pause and resume constantly, which is
    what schedules those callbacks in the first place. The window is still
    narrow, hence the repeats: one round failed maybe one time in six.
    """
    server_ctx, client_ctx = tls_certs
    size = 1 << 20
    rounds = 6
    limit = 1 << 14  # a quarter of the stream default, so pauses are frequent

    async def main():
        async def handle(reader, writer):
            writer.write(b'k' * size)
            await writer.drain()
            writer.close()

        server = await aio.start_server(handle, '127.0.0.1', 0, ssl=server_ctx, limit=limit)
        port = server.sockets[0].getsockname()[1]
        try:
            for _ in range(rounds):
                reader, writer = await aio.open_connection(
                    '127.0.0.1', port, ssl=client_ctx, server_hostname='localhost', limit=limit
                )
                assert await aio.wait_for(reader.readexactly(size), TIMEOUT) == b'k' * size
                writer.close()
        finally:
            server.close()
            await server.wait_closed()

    aio.run(main())
