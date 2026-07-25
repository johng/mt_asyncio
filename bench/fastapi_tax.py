"""FastAPI over a real socket: stdlib asyncio vs mt_asyncio vs TonIO.

``pg_tax.py`` asks what the runtime costs a driver. This asks the question a
user actually has: **I have a FastAPI service, its handler does a little work
per request, what happens if I change the loop underneath it?**

Three arms, all serving the *same* FastAPI application over HTTP/1.1 with
keep-alive, all driven by the same load generator:

  stdlib   stdlib asyncio                         -- the reference
  mt       mt_asyncio + ``compat.install()``      -- what we ship
  tonio    tonio.colored + ``tonio_monkey``       -- upstream's answer

FastAPI itself runs unmodified on all three, by the two routes the projects
take. Ours is ``compat.install()``: shadow the stdlib asyncio namespace and
starlette's few anyio touchpoints keep working because the loop underneath is
asyncio-shaped. Upstream's is ``tonio-monkey``, which rewrites those touchpoints
against tonio primitives. Neither project patches the *handler* -- the endpoint
below is one ``async def`` written once and imported by every arm.

**The server is ours, on purpose.** tonio-monkey patches fastapi and starlette
but ships no ASGI server, and granian's loop registry is asyncio-only, so there
is no server all three arms could share off the shelf. Rather than let uvicorn
serve two arms and something hand-rolled serve the third -- which would measure
the servers, not the runtimes -- this file contains one minimal HTTP/1.1 server
whose parsing, scope construction, ASGI call and response encoding are a single
shared code path (``_serve_conn``). Only the four lines that move bytes differ:
``StreamReader.read``/``StreamWriter.write`` on the asyncio arms,
``SocketStream.receive_some``/``send_all`` on tonio's. Both sit on their
runtime's real connection machinery -- asyncio streams are transports and
protocols underneath, which is the path uvicorn uses and the one mt_asyncio
serialises per connection.

It is minimal in the ways a benchmark can afford (GET only, no request body, no
chunked encoding, the response written in one go) and identical for everyone, so
what is left in the gaps is the runtime.

The ``uvicorn`` arms are a calibration check, not a comparison: they run the same
app under a real server on the two arms that can host one, so the hand-written
server can be shown to be in the right ballpark rather than a strawman. tonio
cannot appear there at all.

Workloads -- the "tiny realistic bit of work" is the point:

  ping      an empty handler. The floor: HTTP parse, routing, response encode.
  compute   the realistic one. Path and query params validated, ~32 order lines
            aggregated in Python, a pydantic response model serialised. This is
            what a JSON endpoint does between awaits, and it is the thing that
            queues behind a single thread on stdlib asyncio.
  pg        the same handler with the rows coming from Postgres instead of
            memory, so a real driver and a real socket are in the request path.

Setup::

    uv pip install fastapi uvicorn psycopg tonio==0.8.3 'tonio-monkey[fastapi]'
    brew install oha            # or run with --client python

    # for the `pg` workload only
    docker run -d --rm --name mtaio-bench-pg -e POSTGRES_PASSWORD=bench \\
        -e POSTGRES_DB=bench -p 55432:5432 postgres:17-alpine
    python bench/fastapi_tax.py --setup-db

Usage::

    python bench/fastapi_tax.py
    python bench/fastapi_tax.py -w compute --threads 1 2 4 8
    python bench/fastapi_tax.py --arms stdlib mt --conns 16
    python bench/fastapi_tax.py --json bench/results/fastapi_tax.json
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time
import traceback


DEFAULT_DSN = 'postgresql://postgres:bench@127.0.0.1:55432/bench'

#: rows per order, and how many customers the fixture holds. 32 lines is a
#: plausible order and keeps the per-request Python work in the tens of
#: microseconds -- small enough that the HTTP and framework cost still dominates
#: (which is the honest shape of a real service) and large enough to be there.
LINES = 32
CUSTOMERS = 512

STATUSES = ('picked', 'packed', 'shipped', 'held')


# ---------------------------------------------------------------------------
# the work
#
# One function, imported by every arm, and the only thing in this file that is
# meant to be recognisable as application code. It is deliberately ordinary: a
# loop over rows, a couple of accumulators, a dict out. No numpy, no C.
# ---------------------------------------------------------------------------


def _fixture_rows(customer_id: int) -> list[tuple[str, int, int, str]]:
    """The order lines for a customer -- deterministic, so every arm sees the same."""
    rows = []
    for i in range(LINES):
        seed = customer_id * 7919 + i * 104729
        rows.append(
            (
                f'SKU-{seed % 100000:05d}',
                1 + seed % 7,
                100 + seed % 9900,
                STATUSES[seed % len(STATUSES)],
            )
        )
    return rows


FIXTURE = {c: _fixture_rows(c) for c in range(CUSTOMERS)}


def summarise(customer_id: int, rows) -> dict:
    """Aggregate order lines. The tiny realistic bit of work."""
    total = 0
    by_status: dict[str, int] = {}
    largest_sku = ''
    largest = -1
    for sku, qty, unit_price, status in rows:
        amount = qty * unit_price
        total += amount
        by_status[status] = by_status.get(status, 0) + 1
        if amount > largest:
            largest = amount
            largest_sku = sku
    return {
        'customer_id': customer_id,
        'lines': len(rows),
        'total_cents': total,
        'by_status': by_status,
        'largest_sku': largest_sku,
        'largest_cents': max(largest, 0),
    }


def build_app(workload: str, pool=None):
    """The FastAPI app. Identical on every arm; only `pool` differs (pg only)."""
    from fastapi import FastAPI
    from pydantic import BaseModel

    class Summary(BaseModel):
        customer_id: int
        lines: int
        total_cents: int
        by_status: dict[str, int]
        largest_sku: str
        largest_cents: int

    app = FastAPI()

    @app.get('/ping')
    async def ping():
        return {'ok': True}

    if workload == 'pg':

        @app.get('/orders/{customer_id}', response_model=Summary)
        async def orders(customer_id: int, limit: int = LINES):
            async with pool.borrow() as conn:
                cur = await conn.execute(
                    'select sku, qty, unit_price, status from bench_orders where customer_id = %s limit %s',
                    (customer_id, limit),
                )
                rows = await cur.fetchall()
            return summarise(customer_id, rows)

    else:

        @app.get('/orders/{customer_id}', response_model=Summary)
        async def orders(customer_id: int, limit: int = LINES):
            rows = FIXTURE[customer_id % CUSTOMERS][:limit]
            return summarise(customer_id, rows)

    return app


# ---------------------------------------------------------------------------
# the connection pool (pg workload)
#
# psycopg_pool's async pool is asyncio-only, so it cannot serve the tonio arm.
# Rather than give one arm a pool and another a hand-rolled one, every arm gets
# this: connections opened up front, handed out under a `threading.Lock`.
#
# It never has to *wait*, which is what lets it be this simple and identical
# across three runtimes with three different async primitives: a client
# connection has at most one request in flight, so sizing the pool to the client
# concurrency bounds the number of borrowers. Underflow raises rather than
# blocking, so a wrong assumption here shows up as a failed run and not as a
# silently different benchmark.
# ---------------------------------------------------------------------------


class Pool:
    def __init__(self, conns):
        self._free = list(conns)
        self._all = list(conns)
        self._lock = threading.Lock()

    def borrow(self):
        return _Borrow(self)

    def _take(self):
        with self._lock:
            if not self._free:
                raise RuntimeError('pg pool underflow: size the pool to the client concurrency')
            return self._free.pop()

    def _give(self, conn):
        with self._lock:
            self._free.append(conn)


class _Borrow:
    __slots__ = ('_pool', '_conn')

    def __init__(self, pool):
        self._pool = pool
        self._conn = None

    async def __aenter__(self):
        self._conn = self._pool._take()
        return self._conn

    async def __aexit__(self, *exc):
        self._pool._give(self._conn)
        self._conn = None


# ---------------------------------------------------------------------------
# the HTTP server
#
# Everything below `_serve_conn` is shared by all three arms; the arms supply
# `recv`/`send` and nothing else.
# ---------------------------------------------------------------------------

_HEAD_END = b'\r\n\r\n'
_READ = 65536

_REASON = {200: b'OK', 404: b'Not Found', 400: b'Bad Request', 422: b'Unprocessable Entity', 500: b'Server Error'}


async def _receive():
    # GET only: the body is empty and already complete. Handlers that ask for it
    # (starlette does, when a route declares a body) get one message and no more.
    return {'type': 'http.request', 'body': b'', 'more_body': False}


def _parse_head(head: bytes):
    lines = head.split(b'\r\n')
    method, target, _ = lines[0].split(b' ', 2)
    headers = []
    for line in lines[1:]:
        name, _, value = line.partition(b':')
        headers.append((name.strip().lower(), value.strip()))
    return method, target, headers


def _encode(status: int, headers, body: bytes) -> bytes:
    out = [b'HTTP/1.1 ', str(status).encode(), b' ', _REASON.get(status, b'OK'), b'\r\n']
    for name, value in headers:
        if name == b'content-length':
            continue
        out += [name, b': ', value, b'\r\n']
    out += [b'content-length: ', str(len(body)).encode(), b'\r\n\r\n', body]
    return b''.join(out)


async def _serve_conn(recv, send, app, server_addr, client_addr):
    """One keep-alive connection: parse, call the ASGI app, write the response.

    The response is buffered and written once. A real server streams the head as
    soon as it has it; doing so here would add a write syscall per request to
    every arm equally, and buffering keeps the code the runtimes share as small
    as possible.
    """
    buf = bytearray()
    while True:
        idx = buf.find(_HEAD_END)
        while idx < 0:
            data = await recv()
            if not data:
                return
            start = max(0, len(buf) - 3)  # a split \r\n\r\n straddles the join
            buf += data
            idx = buf.find(_HEAD_END, start)
        head = bytes(buf[:idx])
        del buf[: idx + 4]

        try:
            method, target, headers = _parse_head(head)
        except ValueError:
            await send(_encode(400, [], b'bad request'))
            return

        path, _, query = target.partition(b'?')
        scope = {
            'type': 'http',
            'asgi': {'version': '3.0', 'spec_version': '2.3'},
            'http_version': '1.1',
            'method': method.decode('latin-1'),
            'scheme': 'http',
            'path': path.decode('latin-1'),
            'raw_path': path,
            'query_string': query,
            'root_path': '',
            'headers': headers,
            'client': client_addr,
            'server': server_addr,
        }

        state = {'status': 500, 'headers': []}
        chunks: list[bytes] = []

        async def _send(message, _state=state, _chunks=chunks):
            kind = message['type']
            if kind == 'http.response.start':
                _state['status'] = message['status']
                _state['headers'] = message.get('headers') or []
            elif kind == 'http.response.body':
                body = message.get('body')
                if body:
                    _chunks.append(body)

        await app(scope, _receive, _send)
        await send(_encode(state['status'], state['headers'], b''.join(chunks)))

        for name, value in headers:
            if name == b'connection' and value.lower() == b'close':
                return


def _announce(port: int) -> None:
    print(f'READY {port}', flush=True)


# ---------------------------------------------------------------------------
# the arms
# ---------------------------------------------------------------------------


def _run(aio, coro, threads):
    """`asyncio.run`, with mt_asyncio's extra argument where it exists."""
    return aio.run(coro) if threads is None else aio.run(coro, threads=threads)


def _serve_asyncio(aio, make_app, port, threads):
    """stdlib asyncio and mt_asyncio: same code, different `aio` module."""
    server_addr = ('127.0.0.1', port)
    app = None

    async def client(reader, writer):
        peer = writer.get_extra_info('peername') or ('127.0.0.1', 0)

        async def recv():
            return await reader.read(_READ)

        async def send(data):
            writer.write(data)
            await writer.drain()

        try:
            await _serve_conn(recv, send, app, server_addr, peer)
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass

    async def main():
        nonlocal app
        app = await make_app()
        server = await aio.start_server(client, '127.0.0.1', port, backlog=1024)
        _announce(port)
        await server.serve_forever()

    _run(aio, main(), threads)


def _serve_tonio(make_app, port, threads):
    import tonio.colored as tonio
    import tonio.colored.net as net

    server_addr = ('127.0.0.1', port)
    app = None

    async def client(stream):
        try:
            peer = stream.socket.getpeername()
        except OSError:
            peer = ('127.0.0.1', 0)

        async def recv():
            return await stream.receive_some(_READ)

        async def send(data):
            await stream.send_all(data)

        try:
            await _serve_conn(recv, send, app, server_addr, peer)
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception:  # `without_tracking` swallows these otherwise
            traceback.print_exc()
        finally:
            try:
                stream.close()
            except Exception:
                pass

    async def accept_loop(listener):
        with listener:
            while True:
                tonio.spawn.without_tracking(client(await listener.accept()))

    async def main():
        nonlocal app
        app = await make_app()
        # The accept loop is written out rather than delegated to
        # `net.serve_listeners`, which in tonio 0.8.3 ends with
        # `return spawn.without_results(...)` inside an `async def` -- so awaiting
        # it hands back the join handle instead of joining, and the server
        # returns immediately having served nothing. (`net.serve_tcp` inherits
        # the same.) What is left here is what that function does: accept, spawn,
        # repeat -- the same shape asyncio's `start_server` runs internally.
        listeners = await net.open_tcp_listeners(port, host='127.0.0.1', backlog=1024)
        _announce(port)
        await tonio.spawn(*[accept_loop(listener) for listener in listeners])

    tonio.run(main(), threads=threads)


async def _open_pool(dsn, size):
    """Open the psycopg connections the `pg` workload hands out.

    Called from inside the server's own runtime invocation, which is not a
    stylistic choice: tonio's runtime is a per-process singleton, so a second
    `run()` to open connections first raises `RuntimeAlreadyInitializedError`.
    """
    import psycopg

    conns = []
    for _ in range(size):
        conns.append(await psycopg.AsyncConnection.connect(dsn, autocommit=True))
    return Pool(conns)


def serve(arm: str, workload: str, port: int, threads: int, dsn: str, pool_size: int) -> None:
    """Run one server arm in the foreground until killed. Prints `READY <port>`."""
    if arm.startswith('mt'):
        import mt_asyncio.asyncio as maio

        maio.compat.install()  # before fastapi/starlette/psycopg import anything
        aio = maio
    elif arm.startswith('tonio'):
        from tonio_monkey.colored import fastapi as _fastapi_patch  # noqa: F401  (patches on import)

        if workload == 'pg':
            from tonio_monkey.colored import psycopg as _psycopg_patch  # noqa: F401

        aio = None
    else:
        import asyncio

        aio = asyncio
        threads = None

    async def make_app():
        pool = await _open_pool(dsn, pool_size) if workload == 'pg' else None
        return build_app(workload, pool=pool)

    if arm.endswith('-uvicorn'):
        _serve_uvicorn(aio, make_app, port, threads)
    elif arm.startswith('tonio'):
        _serve_tonio(make_app, port, threads)
    else:
        _serve_asyncio(aio, make_app, port, threads)


def _serve_uvicorn(aio, make_app, port, threads):
    """Calibration arm: the same app under uvicorn, on the arms that can host it."""
    import threading as _t

    import uvicorn

    async def main():
        config = uvicorn.Config(
            await make_app(),
            host='127.0.0.1',
            port=port,
            log_level='error',
            access_log=False,
            loop='none',
            http='h11',
            lifespan='off',
            backlog=1024,
        )
        server = uvicorn.Server(config)
        server.install_signal_handlers = lambda *a, **k: None
        _t.Thread(target=_wait_and_announce, args=(server, port), daemon=True).start()
        await server.serve()

    _run(aio, main(), threads)


def _wait_and_announce(server, port):
    while not getattr(server, 'started', False):
        time.sleep(0.005)
    _announce(port)


# ---------------------------------------------------------------------------
# load generation
# ---------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def _load_oha(url, requests, conns, timeout):
    proc = subprocess.run(  # noqa: S603 - fixed argv
        [  # noqa: S607 - `oha` comes off PATH by design; checked with shutil.which before we get here
            'oha',
            '-n',
            str(requests),
            '-c',
            str(conns),
            '--no-tui',
            '--output-format',
            'json',
            url,
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
    )
    if proc.returncode != 0:
        raise RuntimeError(f'oha failed: {(proc.stderr or proc.stdout).strip()[-400:]}')
    data = json.loads(proc.stdout)
    summary = data['summary']
    codes = data.get('statusCodeDistribution', {})
    pct = data.get('latencyPercentiles', {})
    return {
        'rps': summary['requestsPerSec'],
        'p50': pct.get('p50', 0.0) * 1000,
        'p99': pct.get('p99', 0.0) * 1000,
        'total': summary['total'],
        'ok': codes.get('200', 0),
        'sent': requests,
        'errors': dict(data.get('errorDistribution') or {}),
        'codes': {k: v for k, v in codes.items() if k != '200'},
    }


def _load_python(url, requests, conns, timeout):
    """Fallback client: threads on blocking sockets, keep-alive, no parsing.

    Costs real CPU on the same box as the server, so it is the less trustworthy
    of the two -- reported as such.
    """
    from urllib.parse import urlsplit

    parts = urlsplit(url)
    addr = (parts.hostname, parts.port)
    req = (
        f'GET {parts.path or "/"}{"?" + parts.query if parts.query else ""} HTTP/1.1\r\n'
        f'Host: {parts.hostname}:{parts.port}\r\n\r\n'
    ).encode()

    per = requests // conns
    lat: list[list[float]] = [[] for _ in range(conns)]
    errs: list[BaseException] = []

    def worker(i):
        try:
            sock = socket.create_connection(addr, timeout=timeout)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            buf = b''
            mine = lat[i]
            for _ in range(per):
                t0 = time.perf_counter()
                sock.sendall(req)
                while True:
                    end = buf.find(b'\r\n\r\n')
                    if end >= 0:
                        head = buf[:end]
                        length = 0
                        for line in head.split(b'\r\n')[1:]:
                            name, _, value = line.partition(b':')
                            if name.strip().lower() == b'content-length':
                                length = int(value)
                        if len(buf) >= end + 4 + length:
                            if not head.startswith(b'HTTP/1.1 200'):
                                raise RuntimeError(f'bad status: {head[:40]!r}')
                            buf = buf[end + 4 + length :]
                            break
                    chunk = sock.recv(_READ)
                    if not chunk:
                        raise RuntimeError('connection closed')
                    buf += chunk
                mine.append((time.perf_counter() - t0) * 1000)
            sock.close()
        except BaseException as exc:
            errs.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(conns)]
    t0 = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - t0
    if errs:
        raise errs[0]

    samples = sorted(x for chunk in lat for x in chunk)
    done = len(samples)
    return {
        'rps': done / elapsed,
        'p50': samples[done // 2],
        'p99': samples[min(done - 1, int(done * 0.99))],
        'total': elapsed,
        'ok': done,
        'sent': per * conns,
        'errors': {},
        'codes': {},
    }


CLIENTS = {'oha': _load_oha, 'python': _load_python}


# ---------------------------------------------------------------------------
# harness
# ---------------------------------------------------------------------------


class Server:
    """A server arm in a subprocess, up and accepting by the time __enter__ returns."""

    def __init__(self, arm, workload, threads, dsn, pool_size, port):
        self.arm = arm
        self.workload = workload
        self.threads = threads
        self.dsn = dsn
        self.pool_size = pool_size
        self.port = port
        self.proc = None
        self.errlines = []
        self._drain = None

    def __enter__(self):
        self.proc = subprocess.Popen(  # noqa: S603 - fixed argv, our own worker
            [
                sys.executable,
                __file__,
                '--serve',
                '--arm',
                self.arm,
                '--workload',
                self.workload,
                '--port',
                str(self.port),
                '--wthreads',
                str(self.threads),
                '--dsn',
                self.dsn,
                '--pool-size',
                str(self.pool_size),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        # Drained on a thread rather than read at teardown: a server that logs a
        # traceback per dropped connection would otherwise fill the pipe buffer
        # and block mid-benchmark, which would look like a throughput result.
        self._drain = threading.Thread(target=self._drain_stderr, daemon=True)
        self._drain.start()

        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            line = self.proc.stdout.readline()
            if line.startswith('READY'):
                return self
            if not line and self.proc.poll() is not None:
                break
        self.stop()
        raise RuntimeError(
            f'{self.arm}/{self.workload}@{self.threads}t did not start:\n  ' + '\n  '.join(self.stderr()[-15:])
        )

    def _drain_stderr(self):
        try:
            for line in self.proc.stderr:
                if len(self.errlines) < 400:
                    self.errlines.append(line.rstrip('\n'))
        except (ValueError, OSError):
            pass

    def stderr(self):
        return list(self.errlines)

    def __exit__(self, *exc):
        self.stop()

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait(timeout=10)
        if self._drain is not None:
            self._drain.join(timeout=5)
        for stream in (self.proc.stdout, self.proc.stderr):
            try:
                stream.close()
            except Exception:
                pass


WORKLOADS = {
    'ping': ('/ping', 'empty handler -- the HTTP + framework floor'),
    'compute': ('/orders/{c}', 'validate, aggregate 32 lines, serialise a response model'),
    'pg': ('/orders/{c}', 'the same handler, rows from Postgres'),
}

ARMS = {
    'stdlib': 'stdlib asyncio',
    'mt': 'mt_asyncio + compat.install()',
    'tonio': 'tonio.colored + tonio-monkey',
    'stdlib-uvicorn': 'stdlib asyncio, uvicorn (calibration)',
    'mt-uvicorn': 'mt_asyncio, uvicorn (calibration)',
}


def _url(workload, port):
    path = WORKLOADS[workload][0].replace('{c}', '7')
    return f'http://127.0.0.1:{port}{path}'


def _probe(url):
    """Fetch the benchmarked URL once and return the response body.

    Every arm must answer byte-for-byte the same thing, or the throughput
    numbers are of different work. oha only checks the status, and a 200 with a
    short body would look like a fast arm.
    """
    from urllib.parse import urlsplit

    parts = urlsplit(url)
    target = parts.path + (f'?{parts.query}' if parts.query else '')
    with socket.create_connection((parts.hostname, parts.port), timeout=30) as sock:
        sock.sendall(f'GET {target} HTTP/1.1\r\nHost: {parts.hostname}\r\nConnection: close\r\n\r\n'.encode())
        buf = b''
        while chunk := sock.recv(65536):
            buf += chunk
    head, _, body = buf.partition(b'\r\n\r\n')
    if not head.startswith(b'HTTP/1.1 200'):
        raise RuntimeError(f'probe got {head.splitlines()[0]!r}')
    return body


def _measure(arm, workload, threads, args, client):
    port = _free_port()
    server = Server(arm, workload, threads, args.dsn, args.conns, port)
    with server:
        url = _url(workload, port)
        try:
            body = _probe(url)
            for _ in range(args.warmup):
                client(url, max(args.conns, args.requests // 10), args.conns, args.timeout)
            runs = [client(url, args.requests, args.conns, args.timeout) for _ in range(args.repeat)]
        except Exception as exc:
            noise = server.stderr()
            if noise:
                raise RuntimeError(f'{exc}\n  server said:\n  ' + '\n  '.join(noise[-15:]))
            raise
        noise = server.stderr()

    # Lost requests are reported, not thrown away. mt_asyncio at 8 workers drops
    # roughly one connection per 20k requests here (never at 1-4 workers, never
    # on stdlib or tonio) -- a real defect, but discarding those runs would both
    # hide it and bias the surviving numbers towards the lucky ones. So the run
    # counts, the loss is carried into the table as a `!n`, and only a loss big
    # enough to move the throughput number (>1%) fails the measurement.
    sent = sum(r['sent'] for r in runs)
    lost = sent - sum(r['ok'] for r in runs)
    errors = {}
    for run in runs:
        for kind, count in list(run['errors'].items()) + list(run['codes'].items()):
            errors[kind] = errors.get(kind, 0) + count
    if lost > sent * 0.01:
        raise RuntimeError(f'lost {lost}/{sent} requests: {errors}')

    rps = [r['rps'] for r in runs]
    return {
        'runs': runs,
        'server_stderr': noise[-15:],
        'body': body.decode('utf-8', 'replace'),
        'lost': lost,
        'sent': sent,
        'errors': errors,
        'rps': statistics.median(rps),
        'rps_max': max(rps),
        'spread': (max(rps) - min(rps)) / min(rps) if min(rps) else 0.0,
        'p50': statistics.median(r['p50'] for r in runs),
        'p99': statistics.median(r['p99'] for r in runs),
    }


def _versions():
    import fastapi
    import starlette
    import tonio

    import mt_asyncio
    from mt_asyncio import _mt_asyncio

    vers = {
        'python': platform.python_version(),
        'freethreaded': not getattr(sys, '_is_gil_enabled', lambda: True)(),
        'platform': platform.platform(),
        'cpus': os.cpu_count(),
        'mt_asyncio': mt_asyncio.__version__,
        'mt_asyncio_build': getattr(_mt_asyncio, '__build_profile__', 'unknown'),
        'tonio': tonio.__version__,
        'fastapi': fastapi.__version__,
        'starlette': starlette.__version__,
    }
    try:
        from tonio_monkey.__version__ import __version__ as tm

        vers['tonio_monkey'] = tm
    except ImportError:
        vers['tonio_monkey'] = 'missing'
    return vers


def _emit(lines, out):
    for line in lines:
        print(line)
        out.append(line)


def setup_db(dsn):
    """Create the `pg` workload's fixture table."""
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute('drop table if exists bench_orders')
        conn.execute(
            'create table bench_orders ('
            ' customer_id int not null, sku text not null,'
            ' qty int not null, unit_price int not null, status text not null)'
        )
        with conn.cursor().copy('copy bench_orders (customer_id, sku, qty, unit_price, status) from stdin') as copy:
            for customer_id, rows in FIXTURE.items():
                for row in rows:
                    copy.write_row((customer_id, *row))
        conn.execute('create index on bench_orders (customer_id)')
        conn.execute('analyze bench_orders')
    print(f'created bench_orders: {CUSTOMERS} customers x {LINES} lines')


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-w', '--workload', nargs='+', default=['ping', 'compute'], metavar='NAME')
    parser.add_argument('-t', '--threads', nargs='+', type=int, default=[1, 2, 4, 8], metavar='N')
    parser.add_argument('-a', '--arms', nargs='+', default=['stdlib', 'mt', 'tonio'], metavar='NAME')
    parser.add_argument('-c', '--conns', type=int, default=16, help='client connections (also the pg pool size)')
    parser.add_argument('-n', '--requests', type=int, default=20000)
    parser.add_argument('-r', '--repeat', type=int, default=3)
    parser.add_argument('--warmup', type=int, default=1)
    parser.add_argument('--client', choices=list(CLIENTS), default=None)
    parser.add_argument('--timeout', type=int, default=300)
    parser.add_argument('--dsn', default=os.environ.get('MT_ASYNCIO_BENCH_DSN', DEFAULT_DSN))
    parser.add_argument('--json', metavar='PATH')
    parser.add_argument('--setup-db', action='store_true', help='create the pg fixture table and exit')
    parser.add_argument('--allow-debug-build', action='store_true')
    # server-side (spawned by the harness)
    parser.add_argument('--serve', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--arm', help=argparse.SUPPRESS)
    parser.add_argument('--port', type=int, help=argparse.SUPPRESS)
    parser.add_argument('--wthreads', type=int, default=1, help=argparse.SUPPRESS)
    parser.add_argument('--pool-size', type=int, default=16, help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.serve:
        serve(args.arm, args.workload[0], args.port, args.wthreads, args.dsn, args.pool_size)
        return

    if args.setup_db:
        setup_db(args.dsn)
        return

    unknown = [w for w in args.workload if w not in WORKLOADS]
    if unknown:
        parser.error(f'unknown workload(s): {", ".join(unknown)}')
    unknown = [a for a in args.arms if a not in ARMS]
    if unknown:
        parser.error(f'unknown arm(s): {", ".join(unknown)}')

    client_name = args.client
    if client_name is None:
        client_name = 'oha' if shutil.which('oha') else 'python'
    if client_name == 'oha' and not shutil.which('oha'):
        parser.error('oha not found on PATH; `brew install oha` or pass --client python')
    client = CLIENTS[client_name]

    vers = _versions()
    if vers['mt_asyncio_build'] == 'debug' and not args.allow_debug_build:
        parser.error(
            'mt_asyncio is a DEBUG build, which is several times slower than release and '
            'invalidates every number here. Run `make build-release`, or pass --allow-debug-build.'
        )

    report = []
    _emit(
        [
            '# FastAPI on stdlib asyncio vs mt_asyncio vs TonIO',
            '',
            f'- fastapi {vers["fastapi"]}, starlette {vers["starlette"]}, one app, three runtimes',
            (
                f'- mt_asyncio {vers["mt_asyncio"]} (this tree, {vers["mt_asyncio_build"]} build), '
                f'tonio {vers["tonio"]} + tonio-monkey {vers["tonio_monkey"]} (PyPI)'
            ),
            f'- python {vers["python"]} (free-threaded: {vers["freethreaded"]}), {vers["platform"]}, {vers["cpus"]} cpus',
            (
                f'- {args.requests} requests over {args.conns} keep-alive connections, client `{client_name}`, '
                f'median of {args.repeat} run(s) after {args.warmup} warmup'
            ),
            '',
            'One hand-written HTTP/1.1 server, shared by every arm bar the `-uvicorn` ones;',
            'only the calls that move bytes differ. Higher is better.',
        ],
        report,
    )

    results = {}
    for workload in args.workload:
        path, blurb = WORKLOADS[workload]
        _emit(
            [
                '',
                f'## {workload} — {blurb}',
                '',
                '| arm | ' + ' | '.join(f'{t}t' for t in args.threads) + ' | best | p50 | p99 |',
                '| --- |' + ' --- |' * (len(args.threads) + 3),
            ],
            report,
        )
        base = None
        losses = []
        bodies = {}
        for arm in args.arms:
            # stdlib has one loop thread by construction; running it at every
            # thread count would be four measurements of the same thing.
            thread_list = [args.threads[0]] if arm.startswith('stdlib') else args.threads
            cells, best, best_row = [], 0.0, None
            for threads in args.threads:
                if threads not in thread_list:
                    cells.append('·')
                    continue
                try:
                    row = _measure(arm, workload, threads, args, client)
                except Exception as exc:  # one arm failing must not kill the run
                    cells.append('fail')
                    results[f'{workload}/{arm}/{threads}'] = {'error': str(exc)[:400]}
                    print(f'  ! {arm}@{threads}t: {str(exc)[:200]}', file=sys.stderr)
                    continue
                results[f'{workload}/{arm}/{threads}'] = row
                bodies.setdefault(row['body'], []).append(f'{arm}@{threads}t')
                cells.append(f'{row["rps"]:,.0f}' + (f' !{row["lost"]}' if row['lost'] else ''))
                if row['lost']:
                    losses.append(f'`{arm}`@{threads}t lost {row["lost"]}/{row["sent"]}: {row["errors"]}')
                if row['rps'] > best:
                    best, best_row = row['rps'], row
            if base is None and best:
                base = best
            ratio = f' ({best / base:.2f}×)' if base and best else ''
            lat = (f'{best_row["p50"]:.2f} ms', f'{best_row["p99"]:.2f} ms') if best_row else ('—', '—')
            _emit(
                [f'| `{arm}` | ' + ' | '.join(cells) + f' | **{best:,.0f}**{ratio} | ' + ' | '.join(lat) + ' |'],
                report,
            )
        _emit(['', f'req/s at each worker-thread count; `·` = not applicable. Path `{path}`.'], report)
        if len(bodies) > 1:
            _emit(
                ['', '**Arms did not answer identically — these numbers are not comparable:**']
                + [f'- {", ".join(who)} → `{body[:160]}`' for body, who in bodies.items()],
                report,
            )
        if losses:
            _emit(
                ['', '**Requests lost** (`!n` above) — the run still counts, see below:'] + [f'- {x}' for x in losses],
                report,
            )

    if args.json:
        os.makedirs(os.path.dirname(args.json) or '.', exist_ok=True)
        with open(args.json, 'w') as fh:
            json.dump(
                {'versions': vers, 'args': vars(args), 'client': client_name, 'results': results},
                fh,
                indent=2,
            )
        md = os.path.splitext(args.json)[0] + '.md'
        with open(md, 'w') as fh:
            fh.write('\n'.join(report) + '\n')
        print(f'\nwrote {args.json} and {md}')


if __name__ == '__main__':
    main()
