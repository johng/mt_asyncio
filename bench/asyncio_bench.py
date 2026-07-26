"""Performance suite: stdlib ``asyncio`` vs ``mt_asyncio.asyncio``.

Every workload is written ONCE against the shared asyncio API and run under both
backends by passing in the module (``asyncio`` or ``mt_asyncio.asyncio``), so the two
backends execute byte-for-byte the same coroutines. The only difference is which
loop steps them: CPython's single-threaded loop, or mt_asyncio's work-stealing runtime
across N worker threads.

Each (backend, workload, threads) configuration runs in a **fresh subprocess** --
mt_asyncio's runtime is a per-process singleton, so a thread count only takes effect
in a process that has not started one yet. Inside that process the workload is
run ``--warmup`` times unmeasured and ``--repeat`` times measured; the reported
figure is the median (min and spread are kept in the JSON output).

Usage (a free-threaded venv with mt_asyncio installed)::

    python bench/asyncio_bench.py                       # everything, 1/2/4/8 threads
    python bench/asyncio_bench.py --list
    python bench/asyncio_bench.py -w mixed churn sock_work
    python bench/asyncio_bench.py --threads 1 4 8 16 --repeat 5
    python bench/asyncio_bench.py --scale 0.25 --repeat 1 # quick smoke
    python bench/asyncio_bench.py --json bench/results/asyncio.json

The workloads are chosen to show both sides of the trade-off honestly: patterns
that carry real per-task work scale across cores, and pure-scheduling patterns
pay mt_asyncio's heavier per-task machinery without anything to parallelize.
"""

from __future__ import annotations

import argparse
import atexit
import json
import os
import platform
import socket
import statistics
import subprocess
import sys
import time


def _burn(n):
    """Deterministic CPU kernel: pure-Python integer work, no allocation churn.

    No allocation means nothing for free-threading to contend on, so this is the
    easiest work there is to parallelise and the speedups it produces are a
    ceiling rather than a forecast -- roughly double what an allocating kernel of
    the same wall-clock weight gets. `bench/fastapi_tax.py --cpu-kind` measures
    both; `bench/README.md` section 4 has the comparison.
    """
    x = 0
    for i in range(n):
        x += i * i
    return x


# ---------------------------------------------------------------------------
# workloads
#
# Each builder takes the backend module as `aio` plus its parameters and returns
# (main, n_ops) WITHOUT running anything -- so the parent process can ask for the
# op count using stdlib asyncio while the worker process runs the real thing.
# ---------------------------------------------------------------------------


def wl_mixed(aio, *, tasks, steps, cpu):
    """The headline case: request-handler shape, CPU work between awaits."""

    async def worker():
        for _ in range(steps):
            _burn(cpu)
            await aio.sleep(0)

    async def main():
        await aio.gather(*[worker() for _ in range(tasks)])

    return main, tasks * steps


def wl_churn(aio, *, tasks, steps):
    """The honest loss case: bare awaits, no work at all -- pure scheduling."""

    async def worker():
        for _ in range(steps):
            await aio.sleep(0)

    async def main():
        await aio.gather(*[worker() for _ in range(tasks)])

    return main, tasks * steps


def wl_gather_fanout(aio, *, tasks, steps, cpu):
    async def worker():
        for _ in range(steps):
            if cpu:
                _burn(cpu)
            await aio.sleep(0)

    async def main():
        await aio.gather(*[worker() for _ in range(tasks)])

    return main, tasks


def wl_taskgroup_fanout(aio, *, tasks, steps, cpu):
    async def worker():
        for _ in range(steps):
            if cpu:
                _burn(cpu)
            await aio.sleep(0)

    async def main():
        async with aio.TaskGroup() as tg:
            for _ in range(tasks):
                tg.create_task(worker())

    return main, tasks


def wl_queue_pipeline(aio, *, items, consumers, cpu):
    async def main():
        q = aio.Queue(maxsize=consumers * 4)

        async def producer():
            for i in range(items):
                await q.put(i)
            for _ in range(consumers):
                await q.put(None)

        async def consumer():
            while True:
                x = await q.get()
                if x is None:
                    return
                if cpu:
                    _burn(cpu)

        await aio.gather(producer(), *[consumer() for _ in range(consumers)])

    return main, items


def wl_lock_contention(aio, *, tasks, cpu):
    """One shared lock: the critical section serializes by construction.

    Only the work OUTSIDE the lock can parallelize, so this is a ceiling test --
    it is not expected to scale linearly, and a regression here means the lock's
    own handoff got more expensive.
    """

    async def main():
        lock = aio.Lock()

        async def worker():
            if cpu:
                _burn(cpu)
            async with lock:
                _burn(cpu // 4 if cpu else 200)

        await aio.gather(*[worker() for _ in range(tasks)])

    return main, tasks


def wl_semaphore_bounded(aio, *, tasks, limit, cpu):
    """Bounded concurrency: `limit` tasks in the region at once, each doing work."""

    async def main():
        sem = aio.Semaphore(limit)

        async def worker():
            async with sem:
                _burn(cpu)

        await aio.gather(*[worker() for _ in range(tasks)])

    return main, tasks


def wl_wait_for_overhead(aio, *, tasks, cpu):
    """Every unit wrapped in wait_for (the timer never fires): scaffolding cost."""

    async def unit():
        _burn(cpu)

    async def main():
        await aio.gather(*[aio.wait_for(unit(), timeout=10) for _ in range(tasks)])

    return main, tasks


def wl_timer_sleep(aio, *, tasks, steps, delay):
    """Real (non-zero) sleeps: timer arm/disarm and wheel throughput."""

    async def worker():
        for _ in range(steps):
            await aio.sleep(delay)

    async def main():
        await aio.gather(*[worker() for _ in range(tasks)])

    return main, tasks * steps


def wl_cancel_heavy(aio, *, tasks):
    """Create many parked tasks, cancel them all, join: cancel/unwind throughput."""

    async def main():
        async def w():
            await aio.Event().wait()  # parks until cancelled

        ts = [aio.create_task(w()) for _ in range(tasks)]
        await aio.sleep(0)  # let them all park
        for t in ts:
            t.cancel()
        await aio.gather(*ts, return_exceptions=True)

    return main, tasks


def wl_executor_offload(aio, *, calls, cpu):
    """to_thread offload: stdlib's ThreadPoolExecutor vs mt_asyncio's blocking pool.

    On free-threaded builds both sides genuinely parallelize, so this measures
    dispatch cost rather than the GIL.
    """

    def blocking():
        return _burn(cpu)

    async def main():
        await aio.gather(*[aio.to_thread(blocking) for _ in range(calls)])

    return main, calls


class Skipped(Exception):
    """A workload that cannot run here -- no DSN, no driver installed, etc.

    Raised from the *builder*, which the parent calls before measuring anything,
    so an unrunnable workload costs one exception and the rest of the suite still
    runs. A bare `raise` would take the whole run down with it and silently drop
    every workload after this one.
    """


class _SleepDriver:
    """Stand-in for a sync DB driver: one blocking call, latency-dominated."""

    def __init__(self, latency):
        self._latency = latency

    def execute(self, n):
        time.sleep(self._latency)
        return n

    def close(self):
        pass


class _PsycopgDriver:
    """A real sync psycopg connection pool, one round-trip per query."""

    def __init__(self, dsn, size):
        from psycopg_pool import ConnectionPool

        self._pool = ConnectionPool(dsn, min_size=size, max_size=size, open=True)
        self._pool.wait()

    def execute(self, n):
        with self._pool.connection() as conn:
            return conn.execute('select %s::int', (n,)).fetchone()[0]

    def close(self):
        self._pool.close()


# built once per worker process and reused across repeat runs, so connection
# setup is not charged to every measurement
_DRIVER = None


def _db_driver(size, latency):
    global _DRIVER
    if _DRIVER is None:
        dsn = os.environ.get('MT_ASYNCIO_BENCH_DSN')
        _DRIVER = _PsycopgDriver(dsn, size) if dsn else _SleepDriver(latency)
        atexit.register(_DRIVER.close)
    return _DRIVER


def wl_db_query(aio, *, queries, pool_size, latency, cpu):
    """Third-party database driver behind ``to_thread``: stdlib's
    ``ThreadPoolExecutor`` vs mt_asyncio's native blocking pool.

    psycopg's async API also works here now (see ``db_query_async``); this
    workload measures the other half of the choice. On a free-threaded build the
    pool threads run Python concurrently, so N queries are genuinely in flight --
    unlike stdlib, where they take turns under the GIL. The cost is one OS thread
    per in-flight query.

    A semaphore bounds in-flight queries to ``pool_size`` on both backends, the
    way a real connection pool would.

    Set ``MT_ASYNCIO_BENCH_DSN`` to measure a real server (requires
    ``psycopg[pool]``). Without it a stand-in driver sleeps for ``latency``
    seconds per query: the dispatch and pool-contention shape is identical, so
    the workload stays runnable anywhere, but the absolute numbers are then
    about the offload path rather than about Postgres.
    """

    async def main():
        driver = _db_driver(pool_size, latency)
        sem = aio.Semaphore(pool_size)

        async def query(n):
            # `cpu` either side is the request-handler shape: deserialize/validate
            # before the query, build the response after. Without it this measures
            # round-trip latency, where there is nothing to parallelize.
            if cpu:
                _burn(cpu)
            async with sem:
                rv = await aio.to_thread(driver.execute, n)
            if cpu:
                _burn(cpu)
            return rv

        await aio.gather(*[query(n) for n in range(queries)])

    return main, queries


def wl_db_query_async(aio, *, queries, pool_size, latency, cpu):
    """psycopg's **async** API over the loop's own ``add_reader``/``add_writer``.

    The counterpart to ``db_query``: same queries, same bounded concurrency, but
    libpq's socket is waited on by the reactor instead of a pool thread. No
    thread per in-flight query, at the cost of a reactor hop per round trip --
    which is exactly the trade the README describes.

    Requires ``MT_ASYNCIO_BENCH_DSN`` and psycopg; the sleep stand-in used by
    ``db_query`` would measure nothing here, so this workload skips rather than
    inventing a number.
    """
    dsn = os.environ.get('MT_ASYNCIO_BENCH_DSN')
    if not dsn:
        raise Skipped('needs MT_ASYNCIO_BENCH_DSN set (the db_query sleep stand-in would measure nothing here)')

    # compat must be installed before psycopg binds `from asyncio import ...`
    if getattr(aio, 'compat', None) is not None:
        aio.compat.install()
    try:
        import psycopg
    except ImportError:
        raise Skipped('needs psycopg installed') from None

    async def main():
        sem = aio.Semaphore(pool_size)
        conns = [await psycopg.AsyncConnection.connect(dsn) for _ in range(pool_size)]
        try:
            slots = aio.Queue()
            for conn in conns:
                slots.put_nowait(conn)

            async def query(n):
                if cpu:
                    _burn(cpu)
                async with sem:
                    conn = await slots.get()
                    try:
                        async with conn.cursor() as cur:
                            await cur.execute('select %s::int', (n,))
                            rv = (await cur.fetchone())[0]
                    finally:
                        slots.put_nowait(conn)
                if cpu:
                    _burn(cpu)
                return rv

            await aio.gather(*[query(n) for n in range(queries)])
        finally:
            for conn in conns:
                await conn.close()

    return main, queries


def wl_block_inline(aio, *, calls, delay):
    """A blocking call made *directly* in the coroutine, no offload.

    Under stdlib this is the classic sin: the one loop thread is gone and every
    other task waits, so N calls cost N*delay however many are outstanding.

    Here it is survivable -- it occupies one worker and the others keep stepping
    -- but only up to a point, and the point is the worker count. The workers
    *are* the scheduler, and a blocked one cannot be reclaimed or replaced, so
    concurrency is pinned at ``threads`` no matter how many tasks are ready.
    Compare with ``block_offload``: same work, on a pool sized for it.
    """

    async def main():
        async def one(_n):
            time.sleep(delay)

        await aio.gather(*[one(n) for n in range(calls)])

    return main, calls


def wl_block_offload(aio, *, calls, delay):
    """The same blocking call through ``to_thread``.

    Concurrency is bounded by the blocking pool (128 by default here, 32 for
    stdlib's ThreadPoolExecutor) rather than by the worker count, and the
    schedulers stay free the whole time.
    """

    async def main():
        async def one(_n):
            await aio.to_thread(time.sleep, delay)

        await aio.gather(*[one(n) for n in range(calls)])

    return main, calls


def wl_sock_echo(aio, *, conns, rounds, msgsize, cpu):
    """socketpair round-trips over sock_recv/sock_sendall.

    ``cpu > 0`` simulates a handler doing per-request processing (the real server
    shape); ``cpu == 0`` is a latency-bound ping-pong with nothing to parallelize.
    """

    async def main():
        loop = aio.get_running_loop()
        msg = b'x' * msgsize
        pairs = []
        for _ in range(conns):
            a, b = socket.socketpair()
            for s in (a, b):
                s.setblocking(False)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, max(msgsize * 2, 65536))
            pairs.append((a, b))

        async def ponger(sock):
            with sock:
                for _ in range(rounds):
                    data = await loop.sock_recv(sock, msgsize)
                    if not data:
                        return
                    if cpu:
                        _burn(cpu)
                    await loop.sock_sendall(sock, data)

        async def pinger(sock):
            with sock:
                for _ in range(rounds):
                    await loop.sock_sendall(sock, msg)
                    need = len(msg)
                    while need:
                        chunk = await loop.sock_recv(sock, need)
                        if not chunk:
                            return
                        need -= len(chunk)

        tasks = []
        for a, b in pairs:
            tasks.append(aio.create_task(ponger(b)))
            tasks.append(aio.create_task(pinger(a)))
        await aio.gather(*tasks)

    return main, conns * rounds


def _tls_pair():
    """Self-signed context pair for the TLS workload, or None if trustme is absent."""
    try:
        import trustme
    except ImportError:
        return None
    import ssl

    ca = trustme.CA()
    cert = ca.issue_cert('localhost')
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    cert.configure_cert(server_ctx)
    client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ca.configure_trust(client_ctx)
    return server_ctx, client_ctx


def _stream_echo(aio, conns, rounds, msgsize, cpu, ssl_pair=None):
    """Request/response over real TCP through Transport/Protocol + streams.

    Unlike ``sock_echo``, which drives the raw ``sock_*`` helpers, this goes
    through the whole transport stack: the per-connection lock is held across
    every read callback and every write, so this is what measures its cost --
    and whether parallelism *across* connections pays for it. ``cpu`` is the
    per-request handler work; at ``cpu == 0`` there is nothing to parallelize
    and the result is pure overhead.
    """
    server_ctx, client_ctx = ssl_pair if ssl_pair else (None, None)

    async def main():
        msg = b'x' * msgsize

        async def handle(reader, writer):
            try:
                for _ in range(rounds):
                    data = await reader.readexactly(msgsize)
                    if cpu:
                        _burn(cpu)
                    writer.write(data)
                    await writer.drain()
            except Exception:
                pass
            finally:
                writer.close()

        server = await aio.start_server(handle, '127.0.0.1', 0, ssl=server_ctx)
        port = server.sockets[0].getsockname()[1]

        async def client():
            kwargs = {'ssl': client_ctx, 'server_hostname': 'localhost'} if client_ctx else {}
            reader, writer = await aio.open_connection('127.0.0.1', port, **kwargs)
            try:
                for _ in range(rounds):
                    writer.write(msg)
                    await writer.drain()
                    await reader.readexactly(msgsize)
            finally:
                writer.close()

        await aio.gather(*[client() for _ in range(conns)])
        # not wait_closed(): both aiohttp and websockets show that shutdown
        # paths race, and teardown is not what is being timed here
        server.close()

    return main, conns * rounds


def wl_stream_echo(aio, *, conns, rounds, msgsize, cpu):
    return _stream_echo(aio, conns, rounds, msgsize, cpu)


def wl_tls_echo(aio, *, conns, rounds, msgsize, cpu):
    pair = _tls_pair()
    if pair is None:
        raise RuntimeError('the tls_echo workload needs `trustme` installed')
    return _stream_echo(aio, conns, rounds, msgsize, cpu, ssl_pair=pair)


# name -> (builder, params, unit, one-line description)
WORKLOADS = {
    'mixed': (
        wl_mixed,
        {'tasks': 32, 'steps': 4, 'cpu': 1_500_000},
        'steps',
        'CPU work between awaits (request-handler shape)',
    ),
    'churn': (
        wl_churn,
        {'tasks': 32, 'steps': 2000},
        'steps',
        'bare awaits, no work (pure scheduling)',
    ),
    'gather_fanout': (
        wl_gather_fanout,
        {'tasks': 2000, 'steps': 10, 'cpu': 2000},
        'tasks',
        'fan-out with gather + light CPU',
    ),
    'taskgroup_fanout': (
        wl_taskgroup_fanout,
        {'tasks': 2000, 'steps': 10, 'cpu': 2000},
        'tasks',
        'same fan-out through TaskGroup',
    ),
    'queue_pipeline': (
        wl_queue_pipeline,
        {'items': 20000, 'consumers': 8, 'cpu': 3000},
        'items',
        'producer -> Queue -> consumers',
    ),
    'lock_contention': (
        wl_lock_contention,
        {'tasks': 4000, 'cpu': 4000},
        'tasks',
        'one shared Lock (serialized critical section)',
    ),
    'semaphore_bounded': (
        wl_semaphore_bounded,
        {'tasks': 2000, 'limit': 8, 'cpu': 30000},
        'tasks',
        'bounded concurrency + CPU',
    ),
    'wait_for_overhead': (
        wl_wait_for_overhead,
        {'tasks': 4000, 'cpu': 20000},
        'tasks',
        'each unit wrapped in wait_for',
    ),
    'timer_sleep': (
        wl_timer_sleep,
        {'tasks': 500, 'steps': 20, 'delay': 0.001},
        'sleeps',
        'many real timers (sleep arm/fire)',
    ),
    'cancel_heavy': (
        wl_cancel_heavy,
        {'tasks': 4000},
        'tasks',
        'create + cancel + unwind, no work',
    ),
    'executor_offload': (
        wl_executor_offload,
        {'calls': 2000, 'cpu': 20000},
        'calls',
        'to_thread offload to the blocking pool',
    ),
    'db_query': (
        wl_db_query,
        {'queries': 2000, 'pool_size': 16, 'latency': 0.002, 'cpu': 20000},
        'queries',
        'sync DB driver via to_thread (set MT_ASYNCIO_BENCH_DSN for real Postgres)',
    ),
    'db_query_async': (
        wl_db_query_async,
        {'queries': 200, 'pool_size': 16, 'latency': 0.0, 'cpu': 20000},
        'queries',
        'psycopg async API + per-query CPU (needs a DSN)',
    ),
    'block_inline': (
        wl_block_inline,
        {'calls': 64, 'delay': 0.01},
        'calls',
        'blocking call made directly in the coroutine (occupies a worker)',
    ),
    'block_offload': (
        wl_block_offload,
        {'calls': 64, 'delay': 0.01},
        'calls',
        'same blocking call through to_thread (occupies a pool thread)',
    ),
    'sock_echo': (
        wl_sock_echo,
        {'conns': 64, 'rounds': 200, 'msgsize': 1024, 'cpu': 0},
        'round-trips',
        'socketpair ping-pong, no per-request work',
    ),
    'sock_work': (
        wl_sock_echo,
        {'conns': 64, 'rounds': 100, 'msgsize': 1024, 'cpu': 20000},
        'requests',
        'socket I/O + per-request CPU (server handler)',
    ),
    'stream_echo': (
        wl_stream_echo,
        {'conns': 64, 'rounds': 100, 'msgsize': 1024, 'cpu': 0},
        'round-trips',
        'TCP transport + streams ping-pong, no per-request work',
    ),
    'stream_work': (
        wl_stream_echo,
        {'conns': 64, 'rounds': 50, 'msgsize': 1024, 'cpu': 20000},
        'requests',
        'TCP transport + streams with per-request CPU (server shape)',
    ),
    'tls_work': (
        wl_tls_echo,
        {'conns': 32, 'rounds': 25, 'msgsize': 1024, 'cpu': 20000},
        'requests',
        'same over TLS (sslproto under the connection lock)',
    ),
}

# knobs that describe "how much work", i.e. the ones --scale multiplies
_SCALABLE = ('tasks', 'items', 'rounds', 'conns', 'calls', 'steps', 'queries')


# ---------------------------------------------------------------------------
# worker side: run one configuration, print timings as JSON
# ---------------------------------------------------------------------------


def _run_worker(backend, workload, params, threads, repeat, warmup):
    builder = WORKLOADS[workload][0]
    if backend == 'stdlib':
        import asyncio as aio

        def run_once():
            main, _ = builder(aio, **params)
            t0 = time.monotonic()
            aio.run(main())
            return time.monotonic() - t0
    else:
        import mt_asyncio.asyncio as aio

        def run_once():
            main, _ = builder(aio, **params)
            t0 = time.monotonic()
            aio.run(main(), threads=threads)
            return time.monotonic() - t0

    for _ in range(warmup):
        run_once()
    times = [run_once() for _ in range(repeat)]
    print(json.dumps({'times': times}))


# ---------------------------------------------------------------------------
# parent side: orchestrate, aggregate, report
# ---------------------------------------------------------------------------


def _measure(backend, workload, params, threads, repeat, warmup):
    proc = subprocess.run(  # noqa: S603
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
        tail = (proc.stderr or proc.stdout).strip().splitlines()[-6:]
        raise RuntimeError(f'{backend}/{workload}@{threads} failed:\n  ' + '\n  '.join(tail))
    times = json.loads(proc.stdout.strip().splitlines()[-1])['times']
    return {
        'times': times,
        'median': statistics.median(times),
        'min': min(times),
        'spread': (max(times) - min(times)) / min(times) if min(times) else 0.0,
    }


def _scaled(params, scale):
    if scale == 1.0:
        return dict(params)
    return {k: max(1, int(v * scale)) if k in _SCALABLE and isinstance(v, int) else v for k, v in params.items()}


def _build_profile():
    """'debug' if mt_asyncio was built by a bare `maturin develop`, else 'release'.

    A debug build is several times slower than a release one, which would make
    every number here meaningless. Older builds predate the flag; report unknown
    rather than guessing.
    """
    from mt_asyncio import _mt_asyncio

    return getattr(_mt_asyncio, '__build_profile__', 'unknown')


def _env():
    import mt_asyncio

    return {
        'python': platform.python_version(),
        'freethreaded': not getattr(sys, '_is_gil_enabled', lambda: True)(),
        'platform': platform.platform(),
        'machine': platform.machine(),
        'cpus': _cpu_count(),
        'mt_asyncio': mt_asyncio.__version__,
        'build': _build_profile(),
    }


def _cpu_count():
    import os

    return os.cpu_count() or 1


def _emit(lines, out):
    for line in lines:
        print(line)
        out.append(line)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-w', '--workload', nargs='+', default=list(WORKLOADS), metavar='NAME')
    parser.add_argument('-t', '--threads', nargs='+', type=int, default=[1, 2, 4, 8], metavar='N')
    parser.add_argument('-r', '--repeat', type=int, default=3, help='measured runs per config (median reported)')
    parser.add_argument('--warmup', type=int, default=1, help='unmeasured runs before measuring')
    parser.add_argument('--scale', type=float, default=1.0, help='multiply the work sizes')
    parser.add_argument('--list', action='store_true', help='list workloads and exit')
    parser.add_argument('--json', metavar='PATH', help='write raw results here')
    parser.add_argument('--markdown', metavar='PATH', help='write the report here as well as stdout')
    parser.add_argument('--allow-debug-build', action='store_true', help='benchmark a debug build anyway')
    parser.add_argument('--worker', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--backend', help=argparse.SUPPRESS)
    parser.add_argument('--params', help=argparse.SUPPRESS)
    parser.add_argument('--wthreads', type=int, default=1, help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.worker:
        _run_worker(args.backend, args.workload[0], json.loads(args.params), args.wthreads, args.repeat, args.warmup)
        return

    if args.list:
        width = max(len(n) for n in WORKLOADS)
        for name, (_, params, unit, blurb) in WORKLOADS.items():
            print(f'{name:<{width}}  {blurb}  [{unit}; {params}]')
        return

    unknown = [w for w in args.workload if w not in WORKLOADS]
    if unknown:
        parser.error(f'unknown workload(s): {", ".join(unknown)} (see --list)')

    env = _env()
    if env['build'] == 'debug' and not args.allow_debug_build:
        parser.error(
            'mt_asyncio is a debug build, which is several times slower than release -- '
            'these numbers would be meaningless. Rebuild with `maturin develop --release`, '
            'or pass --allow-debug-build if you really mean it.'
        )

    report = []
    _emit(
        [
            '# asyncio vs mt_asyncio.asyncio',
            '',
            f'- python: {env["python"]} (free-threaded: {env["freethreaded"]}), '
            f'mt_asyncio {env["mt_asyncio"]} ({env["build"]} build)',
            f'- machine: {env["platform"]}, {env["cpus"]} cpus',
            f'- median of {args.repeat} run(s) after {args.warmup} warmup, scale {args.scale}',
        ],
        report,
    )

    import asyncio as _stdlib

    results, skipped = {}, {}
    for name in args.workload:
        builder, base_params, unit, blurb = WORKLOADS[name]
        params = _scaled(base_params, args.scale)
        try:
            _, ops = builder(_stdlib, **params)
        except Skipped as exc:
            skipped[name] = str(exc)
            _emit(['', f'## {name} — {blurb}', '', f'**skipped** — {exc}', ''], report)
            continue

        _emit(['', f'## {name} — {blurb}', '', f'{ops:,} {unit}; params={params}', ''], report)

        base = _measure('stdlib', name, params, 1, args.repeat, args.warmup)
        entry = results[name] = {'unit': unit, 'ops': ops, 'params': params, 'stdlib': base, 'mt_asyncio': {}}

        _emit(
            [
                '| backend | time (ms) | ' + f'{unit}/s' + ' | vs stdlib |',
                '| --- | --- | --- | --- |',
                f'| stdlib asyncio | {base["median"] * 1000:.1f} | {ops / base["median"]:,.0f} | 1.00x |',
            ],
            report,
        )
        for th in args.threads:
            res = _measure('mt_asyncio', name, params, th, args.repeat, args.warmup)
            entry['mt_asyncio'][th] = res
            _emit(
                [
                    f'| mt_asyncio, {th} thread{"s" if th != 1 else ""} '
                    f'| {res["median"] * 1000:.1f} | {ops / res["median"]:,.0f} '
                    f'| {base["median"] / res["median"]:.2f}x |'
                ],
                report,
            )

    if len(results) > 1:
        _emit(['', '## summary — speedup vs stdlib asyncio', ''], report)
        _emit(
            [
                '| workload | ' + ' | '.join(f'{t}t' for t in args.threads) + ' | best |',
                '| --- | ' + ' | '.join(['---'] * (len(args.threads) + 1)) + ' |',
            ],
            report,
        )
        for name in results:
            entry = results[name]
            base = entry['stdlib']['median']
            speeds = {t: base / entry['mt_asyncio'][t]['median'] for t in args.threads}
            best_t = max(speeds, key=speeds.get)
            cells = ' | '.join(f'{speeds[t]:.2f}x' for t in args.threads)
            _emit([f'| `{name}` | {cells} | **{speeds[best_t]:.2f}x** @{best_t} |'], report)

    # loud enough to notice, so a partial run is never mistaken for a full one
    if skipped:
        _emit(['', '## skipped', ''] + [f'- `{n}` — {why}' for n, why in skipped.items()], report)

    if args.json:
        with open(args.json, 'w') as f:
            json.dump(
                {'env': env, 'repeat': args.repeat, 'scale': args.scale, 'results': results, 'skipped': skipped},
                f,
                indent=2,
            )
        print(f'\nwrote {args.json}')
    if args.markdown:
        with open(args.markdown, 'w') as f:
            f.write('\n'.join(report) + '\n')
        print(f'wrote {args.markdown}')


if __name__ == '__main__':
    main()
