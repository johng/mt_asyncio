"""What does mt_asyncio's asyncio layer cost on a real Postgres round trip?

mt_asyncio is a fork of TonIO: same Rust core, new Python surface. Both projects
can now run psycopg, by opposite routes. Ours is ``compat.install()``: shadow the
stdlib asyncio namespace once, and psycopg's ``waiting.wait_async`` finds *our*
loop and calls *our* ``add_reader``/``add_writer`` -- no per-library work, but
every wait goes through the loop. Upstream's is a separate project,
``tonio-monkey``, which replaces ``psycopg.waiting.wait_async`` outright with a
tonio-native coroutine over ``io.register``, plus psycopg's internal ``_acompat``
Lock/Queue/spawn/gather -- no asyncio at all, but hand-written per library.

So "did we add a tax?" has to be asked in layers. Every arm below drives the SAME
libpq work against the same server and differs only in who waits on the socket:

  tonio-io      tonio.colored  + hand-written waiter on ``tonio.io.register``
  mt-io         mt_asyncio     + the same hand-written waiter on ``mt_asyncio.io``
  mt-raw        mt_asyncio     + psycopg's ``waiting.wait_async`` (compat installed)
  stdlib-raw    stdlib asyncio + psycopg's ``waiting.wait_async``
  tonio-monkey  tonio.colored  + tonio-monkey's patched psycopg, full async API
  mt-monkey     mt_asyncio     + a port of that patch onto ``mt_asyncio.io``
  mt-api        mt_asyncio     + compat.install(), full async API -- what we ship
  stdlib-api    stdlib asyncio + full async API -- the reference
  mt-asyncpg    mt_asyncio     + asyncpg, i.e. a driver that uses *transports*
  stdlib-asyncpg stdlib asyncio + asyncpg -- its reference

Read the gaps, not the absolute numbers:

  tonio-io -> mt-io           core tax: identical waiter code, identical libpq
                              calls, different runtime + async layer.
  tonio-monkey -> mt-monkey   fork tax: upstream's strategy on either runtime,
                              which is the honest like-for-like pair.
  mt-monkey -> mt-api         wait-path tax: what routing psycopg's waits through
                              the asyncio loop costs versus going straight at
                              the reactor (an Event, a wait_for and an
                              add_reader/remove_reader pair per wait -- and our
                              add_reader emulates level-triggered persistence
                              with a poll(2) per dispatch, see _loop._FdHandle).
  tonio-monkey -> mt-api      what upstream's answer to psycopg costs against
                              ours, end to end.
  stdlib-asyncpg -> mt-asyncpg  the same question for a driver that never touches
                              `add_reader`: asyncpg goes through the loop's
                              transports, where readiness parks a Future from the
                              poll thread with no user callback in between.

The ``-raw``/``-io`` arms drive psycopg's ``generators.execute`` directly, with no
cursor or row machinery, so runtime differences show undiluted. The ``-monkey``
and ``-api`` arms use the full ``AsyncConnection``/cursor path -- what a user
actually writes.

Setup::

    docker run -d --rm --name mtaio-bench-pg -e POSTGRES_PASSWORD=bench \\
        -e POSTGRES_DB=bench -p 55432:5432 postgres:17-alpine
    uv pip install psycopg asyncpg tonio==0.8.3 'tonio-monkey[psycopg]'

Usage::

    python bench/pg_tax.py
    python bench/pg_tax.py --tier io          # just the regression pair
    python bench/pg_tax.py --threads 1 4 --repeat 5
    python bench/pg_tax.py --json bench/results/pg_tax.json
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import time


DEFAULT_DSN = 'postgresql://postgres:bench@127.0.0.1:55432/bench'

SQL = b'select $1::int'


def _burn(n):
    x = 0
    for i in range(n):
        x += i * i
    return x


# ---------------------------------------------------------------------------
# waiting strategies
#
# `_wait_io` is the hand-written one: byte-for-byte the same code against either
# project's `io.register`, which is what makes tonio-io vs mt-io a real
# regression test. `waiting.wait_async` is psycopg's own, and only runs where an
# asyncio loop exists.
# ---------------------------------------------------------------------------


#: WAIT_RW seen (psycopg's `_send` yields it when a flush would block). We treat
#: it as "wait for writable", which is what the flush loop is asking for; the
#: read half only matters once the server's reply buffer fills, which `select
#: $1::int` never does. Reported so a workload that does hit it is not silently
#: measured on a shortcut.
_RW_SEEN = [0]


async def _wait_io(gen, sched, wait_w, ready_w, ready_r):
    """Drive a psycopg generator over a raw runtime IO registration.

    ``arm_*`` returns ``None`` when the reactor's cached readiness bit is already
    set. That bit is an *edge* the reactor once saw, not the current level, so it
    goes stale the moment libpq drains the socket; leave it set and the next
    ``arm_*`` returns ``None`` again and this loop spins on ``consume_input()``
    instead of parking -- a busy-wait, and roughly 3x slower here.

    So the bit is drained (``consume_*``) *before* the generator is resumed, i.e.
    before the syscall that may return EAGAIN. That order is what makes it
    race-free: any edge the reactor sees after the drain re-sets the bit, so the
    next ``arm_*`` cannot park on data that already arrived. Draining *after*
    EAGAIN instead -- the obvious way round -- loses exactly that wakeup and
    deadlocks under more than one runtime thread. mt_asyncio also ships a
    tick-guarded ``clear_r``/``clear_w`` that is safe in either order; upstream
    TonIO's ``ScheduledIO`` has only ``consume_*``, so ``consume_*`` is what this
    function uses -- it has to be byte-for-byte the same code on both runtimes
    for the comparison to mean anything.
    """
    s = next(gen)
    while True:
        want_w = s & wait_w
        if want_w and s != wait_w:
            _RW_SEEN[0] += 1
        if want_w:
            waiter = sched.arm_w()
            if waiter is not None:
                await waiter
            sched.consume_w()
        else:
            waiter = sched.arm_r()
            if waiter is not None:
                await waiter
            sched.consume_r()
        try:
            s = gen.send(ready_w if want_w else ready_r)
        except StopIteration as exc:
            return exc.value


# ---------------------------------------------------------------------------
# backends
#
# Each one: import order first (compat before psycopg), then `run(main, threads)`
# and the three pieces a workload needs -- `gather`, `connect`, `query`.
# ---------------------------------------------------------------------------


class _RawBackend:
    """Raw libpq: psycopg's generators, no cursor/row machinery."""

    def connect(self, dsn, n):
        """Blocking connect, outside the timed region, on every backend."""
        from psycopg import pq

        conns = []
        for _ in range(n):
            pgconn = pq.PGconn.connect(dsn.encode())
            if pgconn.status != pq.ConnStatus.OK:
                raise RuntimeError(pgconn.error_message.decode())
            pgconn.nonblocking = 1
            conns.append(pgconn)
        return conns

    def close(self, conns):
        for pgconn in conns:
            pgconn.finish()


class IoBackend(_RawBackend):
    """`io.register` + `arm_r`/`arm_w` by hand -- no asyncio anywhere."""

    tier = 'io'

    def __init__(self, pkg):
        self._pkg = pkg

    def setup(self, threads):
        from psycopg import generators, waiting

        self._generators = generators
        self._wait_w = waiting.WAIT_W
        self._ready_w = waiting.READY_W
        self._ready_r = waiting.READY_R

        if self._pkg == 'tonio':
            import tonio
            import tonio.colored as tc
            import tonio.io as tio

            self._runtime = tonio.runtime(threads=threads)
            self._io = tio
            self._gather = lambda coros: tc.spawn.without_results(*coros)
            self._run = lambda coro: self._runtime.run_until_complete(coro)
        else:
            import mt_asyncio
            import mt_asyncio.asyncio as aio
            import mt_asyncio.io as mio

            self._io = mio
            self._gather = lambda coros: aio.gather(*coros)
            self._run = lambda coro: aio.run(coro, threads=threads)
            self._mt_asyncio = mt_asyncio

    def run(self, coro):
        return self._run(coro)

    def gather(self, coros):
        return self._gather(coros)

    def register(self, pgconn):
        return self._io.register(pgconn.socket)

    async def query(self, pgconn, sched, n):
        pgconn.send_query_params(SQL, [b'%d' % n])
        results = await _wait_io(self._generators.execute(pgconn), sched, self._wait_w, self._ready_w, self._ready_r)
        return int(results[-1].get_value(0, 0))


class RawAsyncioBackend(_RawBackend):
    """Same libpq work, waited on by psycopg's own `waiting.wait_async`."""

    tier = 'raw'

    def __init__(self, impl):
        self._impl = impl

    def setup(self, threads):
        if self._impl == 'mt_asyncio':
            import mt_asyncio.asyncio as aio

            # before psycopg is imported: `waiting.py` opens with
            # `from asyncio import Event, get_event_loop, wait_for`
            aio.compat.install()
            self._aio = aio
            self._run = lambda coro: aio.run(coro, threads=threads)
        else:
            import asyncio as aio

            self._aio = aio
            self._run = aio.run

        from psycopg import generators, waiting

        self._generators = generators
        self._wait_async = waiting.wait_async

    def run(self, coro):
        return self._run(coro)

    def gather(self, coros):
        return self._aio.gather(*coros)

    def register(self, pgconn):
        return None  # the loop owns fd registration

    async def query(self, pgconn, _sched, n):
        pgconn.send_query_params(SQL, [b'%d' % n])
        # interval=0.1 is what psycopg's own AsyncConnection passes
        # (`connection_async._WAIT_INTERVAL`); the parameter's 0.0 default turns
        # every wait into an immediate `wait_for` timeout and busy-spins.
        results = await self._wait_async(self._generators.execute(pgconn), pgconn.socket, interval=0.1)
        return int(results[-1].get_value(0, 0))


class ApiBackend:
    """The full `psycopg.AsyncConnection` path -- what a user actually writes."""

    tier = 'api'

    def __init__(self, impl):
        self._impl = impl

    def setup(self, threads):
        if self._impl == 'mt_asyncio':
            import mt_asyncio.asyncio as aio

            aio.compat.install()
            self._aio = aio
            self._run = lambda coro: aio.run(coro, threads=threads)
        else:
            import asyncio as aio

            self._aio = aio
            self._run = aio.run

        import psycopg

        self._psycopg = psycopg

    def run(self, coro):
        return self._run(coro)

    def gather(self, coros):
        return self._aio.gather(*coros)

    async def aconnect(self, dsn, n):
        return [await self._psycopg.AsyncConnection.connect(dsn, autocommit=True) for _ in range(n)]

    async def aclose(self, conns):
        for conn in conns:
            await conn.close()

    async def query(self, conn, _sched, n):
        async with conn.cursor() as cur:
            await cur.execute('select %s::int', (n,))
            return (await cur.fetchone())[0]


# ---------------------------------------------------------------------------
# the `-monkey` arms: patch psycopg's own wait functions, don't shadow asyncio
#
# This is what upstream ships as a separate project, `tonio-monkey`
# (github.com/gi0baro/tonio-monkey): `psycopg.waiting.wait_async` is replaced by
# a tonio-native coroutine over `io.register`, and psycopg's internal `_acompat`
# Lock/Queue/spawn/gather are replaced by tonio's. No asyncio anywhere -- the
# opposite trade to `compat.install()`, which supplies a whole asyncio namespace
# and needs no per-library work.
#
# `mt-monkey` is that same strategy on mt_asyncio: the wait function below is a
# port of tonio-monkey's, with `mt_asyncio.io` in place of `tonio.io`. It runs
# with compat installed, so psycopg's `_acompat` primitives are already ours and
# only the wait needs replacing. That makes `tonio-monkey` vs `mt-monkey` the
# like-for-like pair, and `mt-monkey` vs `mt-api` the price of routing psycopg's
# waits through the asyncio loop instead of straight at the reactor.
# ---------------------------------------------------------------------------


def _mt_wait_async_factory(aio, mio, waiting, errors):
    """Port of tonio-monkey's `_wait_async` onto mt_asyncio's `io.register`."""
    WAIT_R, WAIT_W = waiting.WAIT_R, waiting.WAIT_W  # noqa: N806
    READY_R, READY_W = waiting.READY_R, waiting.READY_W  # noqa: N806

    async def _wait_rw(reg):
        # tonio-monkey awaits `tonio.select(w_r, w_w)` here, which spawns a scope
        # and two tasks. mt_asyncio has the callback form instead (`arm_*_cb`, a
        # fork addition), so both directions land on one future -- cheaper, and
        # the only place the two ports differ. Unreachable for `select $1::int`:
        # psycopg yields RW only when a flush would block, so `_RW_SEEN` stays 0
        # and this cannot flatter either arm.
        _RW_SEEN[0] += 1
        loop = aio.get_running_loop()
        fut = loop.create_future()

        def wake():
            if not fut.done():
                fut.set_result(None)

        ready = reg.arm_r_cb(wake)
        ready = reg.arm_w_cb(wake) or ready
        if not ready:
            await fut

    async def wait_async(gen, fileno, interval=0.0):
        timeout = interval if interval else None
        reg = mio.register(fileno)
        try:
            s = next(gen)
            while True:
                reader, writer = s & WAIT_R, s & WAIT_W
                if not (reader or writer):
                    raise errors.InternalError(f'bad poll status: {s}')

                if reader and writer:
                    await _wait_rw(reg)
                elif reader:
                    if (waiter := reg.arm_r(timeout)) is not None:
                        await waiter
                elif (waiter := reg.arm_w(timeout)) is not None:
                    await waiter

                ready = 0
                if reader and reg.consume_r():
                    ready |= READY_R
                if writer and reg.consume_w():
                    ready |= READY_W

                s = gen.send(ready)
        except OSError as exc:
            raise errors.OperationalError('connection socket closed') from exc
        except StopIteration as exc:
            return exc.value
        finally:
            reg.close()

    return wait_async


class MonkeyBackend(ApiBackend):
    """psycopg's own wait function replaced by a runtime-native one."""

    tier = 'monkey'

    def setup(self, threads):
        if self._impl == 'tonio':
            import tonio
            import tonio.colored as tc
            from tonio_monkey.colored import psycopg  # applies the patches

            self._runtime = tonio.runtime(threads=threads)
            self._psycopg = psycopg
            self._run = lambda coro: self._runtime.run_until_complete(coro)
            self._gather = lambda coros: tc.spawn.without_results(*coros)
            return

        import mt_asyncio.asyncio as aio
        import mt_asyncio.io as mio

        aio.compat.install()
        import psycopg
        from psycopg import errors, waiting

        # Only the per-query wait is replaced. `wait_conn_async` (connect) keeps
        # psycopg's asyncio version, which compat already routes to our loop;
        # connections are built outside the timed region on every arm.
        waiting.wait_async = _mt_wait_async_factory(aio, mio, waiting, errors)

        self._aio = aio
        self._psycopg = psycopg
        self._run = lambda coro: aio.run(coro, threads=threads)
        self._gather = lambda coros: aio.gather(*coros)

    def gather(self, coros):
        return self._gather(coros)


class FastWaitForBackend(ApiBackend):
    """`mt-api`, with `wait_for` rewritten in CPython 3.12's shape.

    Our `wait_for` (`_ctl.wait_for`) follows the pre-3.12 pattern: wrap the
    awaitable in a Task, arm a `call_later`, park on the Task. CPython dropped
    that in 3.12 for `async with timeouts.timeout(t): return await fut`, which
    has no intermediate task -- and the intermediate task is what hurts here,
    because on a multi-worker scheduler its completion is a cross-worker wake
    (~9.6us measured) where stdlib's is a same-thread callback.

    psycopg calls `wait_for` once per wait, so this arm measures what fixing
    `_ctl.wait_for` would be worth to the shipped path, with nothing else
    changed. It patches `psycopg.waiting.wait_for` because `waiting.py` binds
    the name at import (`from asyncio import ... wait_for`), which compat has
    already pointed at ours.
    """

    tier = 'api'

    def setup(self, threads):
        super().setup(threads)
        from psycopg import waiting

        aio = self._aio

        async def wait_for(aw, timeout):
            if timeout is None:
                return await aw
            async with aio.timeout(timeout):
                return await aw

        waiting.wait_for = wait_for


class AsyncpgBackend(ApiBackend):
    """asyncpg -- the *other* integration shape, and the interesting control.

    psycopg opens its own socket and waits on the raw fd through ``add_reader``,
    the one asyncio API that forces a callback→task bridge, and that bridge is
    the whole `mt-api` gap. asyncpg instead asks the loop for the connection
    (``create_connection``) and drives a ``Protocol``/``Transport`` pair, so the
    waiting happens inside *our* transport code -- which parks a Future straight
    from the poll thread (``_loop._wait_readable``) and never dispatches a user
    callback.

    So this arm separates "compat mode costs" from "`add_reader` costs". It has
    no upstream counterpart: tonio-monkey ships no asyncpg patch, and asyncpg
    could not run on tonio anyway -- it needs a real loop to build transports
    on. `stdlib-asyncpg` is the reference instead.
    """

    tier = 'asyncpg'

    def setup(self, threads):
        if self._impl == 'mt_asyncio':
            import mt_asyncio.asyncio as aio

            aio.compat.install()
            self._aio = aio
            self._run = lambda coro: aio.run(coro, threads=threads)
        else:
            import asyncio as aio

            self._aio = aio
            self._run = aio.run

        import asyncpg

        self._asyncpg = asyncpg

    async def aconnect(self, dsn, n):
        return [await self._asyncpg.connect(dsn) for _ in range(n)]

    async def aclose(self, conns):
        for conn in conns:
            await conn.close()

    async def query(self, conn, _sched, n):
        return await conn.fetchval('select $1::int', n)


BACKENDS = {
    'tonio-io': (IoBackend, ('tonio',)),
    'mt-io': (IoBackend, ('mt_asyncio',)),
    'mt-raw': (RawAsyncioBackend, ('mt_asyncio',)),
    'stdlib-raw': (RawAsyncioBackend, ('stdlib',)),
    'tonio-monkey': (MonkeyBackend, ('tonio',)),
    'mt-monkey': (MonkeyBackend, ('mt_asyncio',)),
    'mt-api': (ApiBackend, ('mt_asyncio',)),
    'mt-api-wf': (FastWaitForBackend, ('mt_asyncio',)),
    'stdlib-api': (ApiBackend, ('stdlib',)),
    'mt-asyncpg': (AsyncpgBackend, ('mt_asyncio',)),
    'stdlib-asyncpg': (AsyncpgBackend, ('stdlib',)),
}

#: pairs whose gap answers one question each; reported under every workload
COMPARISONS = (
    # identical waiter code and libpq calls, different runtime and async layer
    ('core tax', 'tonio-io', 'mt-io'),
    # tonio-monkey's strategy on either runtime: the honest like-for-like, since
    # both replace psycopg's wait function rather than the asyncio namespace
    ('fork tax', 'tonio-monkey', 'mt-monkey'),
    # our shipped path vs that strategy. NOT setup cost: `compat.install()` and
    # every import happen once in `_run_worker` before the first run, warmups are
    # discarded, and the clock in `_make_main` starts after the connections are
    # open. This is per-wait steady state.
    ('wait-path tax', 'mt-monkey', 'mt-api'),
    # what upstream's answer to psycopg costs against ours, end to end
    ('vs upstream', 'tonio-monkey', 'mt-api'),
)


# ---------------------------------------------------------------------------
# workloads
# ---------------------------------------------------------------------------


def _make_main(be, dsn, *, conns, queries, cpu):
    """`conns` connections, `queries` round trips each, `cpu` Python work between.

    One connection per coroutine: no pool, no lock contention, so what is being
    timed is the wait itself. `cpu` is the request-handler shape -- the work a
    server does with a row once it has it, and the only reason more than one
    runtime thread can help.
    """
    is_api = isinstance(be, ApiBackend)

    async def worker(conn, sched):
        for n in range(queries):
            got = await be.query(conn, sched, n)
            if got != n:
                raise AssertionError(f'expected {n}, got {got!r}')
            if cpu:
                _burn(cpu)

    if is_api:

        async def main():
            pool = await be.aconnect(dsn, conns)
            try:
                t0 = time.monotonic()
                await be.gather([worker(c, None) for c in pool])
                return time.monotonic() - t0
            finally:
                await be.aclose(pool)

        return main

    pool = be.connect(dsn, conns)

    async def main():
        scheds = [be.register(c) for c in pool]
        t0 = time.monotonic()
        await be.gather([worker(c, s) for c, s in zip(pool, scheds, strict=True)])
        return time.monotonic() - t0

    return main, pool


WORKLOADS = {
    'rtt': ({'conns': 8, 'queries': 250, 'cpu': 0}, 'round trips, nothing else'),
    'mixed': ({'conns': 8, 'queries': 250, 'cpu': 5_000}, 'round trips + a little Python work per row'),
    'handler': ({'conns': 8, 'queries': 250, 'cpu': 20_000}, 'round trips + Python work per row'),
}


# ---------------------------------------------------------------------------
# worker / harness
# ---------------------------------------------------------------------------


def _run_worker(backend, workload, params, dsn, threads, repeat, warmup):
    cls, args = BACKENDS[backend]
    be = cls(*args)
    be.setup(threads)

    def run_once():
        built = _make_main(be, dsn, **params)
        if isinstance(built, tuple):
            main, pool = built
            try:
                return be.run(main())
            finally:
                be.close(pool)
        return be.run(built())

    for _ in range(warmup):
        run_once()
    times = [run_once() for _ in range(repeat)]
    print(json.dumps({'times': times, 'rw_waits': _RW_SEEN[0]}))


def _measure(backend, workload, params, dsn, threads, repeat, warmup):
    proc = subprocess.run(  # noqa: S603 - fixed argv, our own worker
        [
            sys.executable,
            __file__,
            '--worker',
            '--backend',
            backend,
            '--workload',
            workload,
            '--params',
            json.dumps(params),
            '--dsn',
            dsn,
            '--wthreads',
            str(threads),
            '--repeat',
            str(repeat),
            '--warmup',
            str(warmup),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout).strip().splitlines()[-8:]
        raise RuntimeError(f'{backend}/{workload}@{threads}t failed:\n  ' + '\n  '.join(tail))
    payload = json.loads(proc.stdout.strip().splitlines()[-1])
    times = payload['times']
    return {
        'times': times,
        'median': statistics.median(times),
        'min': min(times),
        'spread': (max(times) - min(times)) / min(times) if min(times) else 0.0,
        'rw_waits': payload.get('rw_waits', 0),
    }


def _versions():
    import psycopg
    import tonio
    from tonio_monkey.__version__ import __version__ as tonio_monkey_version

    import mt_asyncio
    from mt_asyncio import _mt_asyncio

    return {
        'tonio_monkey': tonio_monkey_version,
        'python': platform.python_version(),
        'freethreaded': not getattr(sys, '_is_gil_enabled', lambda: True)(),
        'platform': platform.platform(),
        'tonio': tonio.__version__,
        'mt_asyncio': mt_asyncio.__version__,
        'mt_asyncio_build': getattr(_mt_asyncio, '__build_profile__', 'unknown'),
        'psycopg': psycopg.__version__,
        'libpq_impl': psycopg.pq.__impl__,
    }


def _emit(lines, out):
    for line in lines:
        print(line)
        out.append(line)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-w', '--workload', nargs='+', default=list(WORKLOADS), metavar='NAME')
    parser.add_argument('-t', '--threads', nargs='+', type=int, default=[1, 4], metavar='N')
    parser.add_argument('--tier', choices=['io', 'raw', 'monkey', 'api', 'asyncpg', 'all'], default='all')
    parser.add_argument('-b', '--backends', nargs='+', metavar='NAME', help='explicit arms, overrides --tier')
    parser.add_argument('--conns', type=int, metavar='N', help="override every workload's connection count")
    parser.add_argument('--dsn', default=os.environ.get('MT_ASYNCIO_BENCH_DSN', DEFAULT_DSN))
    parser.add_argument('-r', '--repeat', type=int, default=5)
    parser.add_argument('--warmup', type=int, default=1)
    parser.add_argument('--scale', type=float, default=1.0)
    parser.add_argument('--json', metavar='PATH')
    parser.add_argument('--allow-debug-build', action='store_true')
    parser.add_argument('--worker', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--backend', help=argparse.SUPPRESS)
    parser.add_argument('--params', help=argparse.SUPPRESS)
    parser.add_argument('--wthreads', type=int, default=1, help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.worker:
        _run_worker(
            args.backend,
            args.workload[0],
            json.loads(args.params),
            args.dsn,
            args.wthreads,
            args.repeat,
            args.warmup,
        )
        return

    unknown = [w for w in args.workload if w not in WORKLOADS]
    if unknown:
        parser.error(f'unknown workload(s): {", ".join(unknown)}')

    vers = _versions()
    if vers['mt_asyncio_build'] == 'debug' and not args.allow_debug_build:
        parser.error(
            'mt_asyncio is a DEBUG build and the tonio wheel from PyPI is a release build; '
            'that gap is the build profile, not the fork. Rebuild with '
            '`maturin develop --release`, or pass --allow-debug-build.'
        )

    if args.backends:
        unknown = [b for b in args.backends if b not in BACKENDS]
        if unknown:
            parser.error(f'unknown backend(s): {", ".join(unknown)}')
        backends = args.backends
    else:
        backends = [b for b, (cls, _) in BACKENDS.items() if args.tier == 'all' or cls.tier == args.tier]

    report = []
    _emit(
        [
            '# psycopg on mt_asyncio vs upstream TonIO',
            '',
            (
                f'- tonio {vers["tonio"]} (PyPI, release) vs mt_asyncio {vers["mt_asyncio"]} '
                f'(this tree, {vers["mt_asyncio_build"]} build)'
            ),
            f'- psycopg {vers["psycopg"]}, libpq impl `{vers["libpq_impl"]}`, dsn `{args.dsn}`',
            f'- python {vers["python"]} (free-threaded: {vers["freethreaded"]}), {vers["platform"]}',
            f'- median of {args.repeat} run(s) after {args.warmup} warmup, scale {args.scale}',
            '',
            f'- tonio-monkey {vers["tonio_monkey"]} (PyPI) for the `tonio-monkey` arm',
            '',
            'Every arm drives the same libpq work against the same server and differs only',
            'in who waits on the socket. The gaps under each table are the answer; the',
            'absolute numbers are hostage to the libpq binding above.',
        ],
        report,
    )

    results = {}
    for name in args.workload:
        base_params, blurb = WORKLOADS[name]
        params = dict(base_params)
        if args.conns:
            params['conns'] = args.conns
        if args.scale != 1.0:
            params['queries'] = max(1, int(params['queries'] * args.scale))
        total = params['conns'] * params['queries']
        _emit(
            [
                '',
                f'## {name} — {blurb}',
                '',
                f'params={params} ({total} queries per run)',
                '',
                '| backend | ' + ' | '.join(f'{t}t (ms)' for t in args.threads) + ' | q/s (best) |',
                '| --- | ' + ' | '.join(['---'] * (len(args.threads) + 1)) + ' |',
            ],
            report,
        )
        for backend in backends:
            cells, best = [], None
            for threads in args.threads:
                r = _measure(backend, name, params, args.dsn, threads, args.repeat, args.warmup)
                results[f'{name}/{backend}/{threads}'] = r
                cells.append(f'{r["median"] * 1000:.1f}')
                best = r['median'] if best is None else min(best, r['median'])
            _emit([f'| {backend} | ' + ' | '.join(cells) + f' | {total / best:,.0f} |'], report)

        _emit([''], report)
        for label, base, other in COMPARISONS:
            if base not in backends or other not in backends:
                continue
            for threads in args.threads:
                b = results.get(f'{name}/{base}/{threads}')
                o = results.get(f'{name}/{other}/{threads}')
                if not b or not o:
                    continue
                delta = (o['median'] - b['median']) / b['median'] * 100
                _emit(
                    [
                        (
                            f'- {label} @{threads}t: `{other}` is {delta:+.1f}% vs `{base}` '
                            f'({o["median"] * 1000:.1f}ms vs {b["median"] * 1000:.1f}ms)'
                        )
                    ],
                    report,
                )

    rw = sum(r['rw_waits'] for r in results.values())
    if rw:
        _emit(['', f'note: {rw} WAIT_RW wait(s) were served as write-only waits (see `_RW_SEEN`)'], report)

    if args.json:
        with open(args.json, 'w') as f:
            json.dump({'versions': vers, 'dsn': args.dsn, 'results': results}, f, indent=2, sort_keys=True)


if __name__ == '__main__':
    main()
