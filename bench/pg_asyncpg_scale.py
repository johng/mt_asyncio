"""asyncpg thread-scaling: mt_asyncio at N workers vs stdlib asyncio.

There is no upstream TonIO arm here on purpose. tonio-monkey ships no asyncpg
patch, so the only honest question left is the one this file asks: **does the
same asyncpg program go faster when the loop can use more than one core?**
stdlib asyncio is the baseline (it has exactly one), and mt_asyncio is measured
at 1, 2, 4, 8, 12 workers. The 1-worker column is the compat-mode overhead with
no parallelism to pay for it; everything to its right is the speedup.

The workload is the one where the answer can be yes: a wide read, decoded and
then *handled* in Python. ``--tasks`` connections each fetch ``--rows`` rows
concurrently (default 20 x 50,000 = 1,000,000 rows per repeat) and run a
per-row Python loop over the result. The row handling is real work on real
objects, which is what a single-threaded loop has to serialise and a
multi-threaded one does not.

Setup::

    docker run -d --rm --name mtaio-bench-pg -e POSTGRES_PASSWORD=bench \\
        -e POSTGRES_DB=bench -p 55432:5432 postgres:17-alpine
    uv pip install asyncpg

Usage::

    python bench/pg_asyncpg_scale.py
    python bench/pg_asyncpg_scale.py --rows 20000 --repeat 1   # quick
    python bench/pg_asyncpg_scale.py --json bench/results/pg_asyncpg_scale.json
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

TABLE = 'bench_scale_rows'
QUERY = f'select id, v, t from {TABLE}'  # noqa: S608 - fixed table name, no user input


def ensure_table(dsn, rows):
    """Create the fixture table (once, outside any measurement)."""
    import asyncio

    import asyncpg

    async def go():
        conn = await asyncpg.connect(dsn)
        try:
            have = await conn.fetchval(f"select to_regclass('{TABLE}')")
            if have is not None:
                n = await conn.fetchval(f'select count(*) from {TABLE}')  # noqa: S608 - ours
                if n == rows:
                    return
                await conn.execute(f'drop table {TABLE}')
            ddl = (
                f'create table {TABLE} as '  # noqa: S608 - fixed table name, `rows` is an int
                f'select g as id, (g * 7919)::bigint as v, '
                f"'row-' || g || '-payload' as t "
                f'from generate_series(1, {rows}) g'
            )
            await conn.execute(ddl)
        finally:
            await conn.close()

    asyncio.run(go())


# -- the worker (runs in a subprocess, one per measured configuration) --------


def handle(rows, cpu):
    """Per-row Python work. This is the part that can use more than one core."""
    acc = 0
    for rec in rows:
        rid = rec[0]
        v = rec[1]
        t = rec[2]
        acc += rid + v + len(t)
        for _ in range(cpu):
            acc = (acc * 31 + v) & 0xFFFFFFFF
    return acc


def worker(args):
    if args.backend == 'mt':
        import mt_asyncio.asyncio as aio

        aio.compat.install()
        run = lambda coro: aio.run(coro, threads=args.threads)  # noqa: E731
    else:
        import asyncio as aio

        run = aio.run

    import asyncpg

    async def one(conn):
        rows = await conn.fetch(QUERY)
        return handle(rows, args.cpu)

    async def main():
        conns = [await asyncpg.connect(args.dsn) for _ in range(args.tasks)]
        try:
            # warmup: prepares the statement and fills the caches, untimed
            for _ in range(args.warmup):
                await aio.gather(*[one(c) for c in conns])

            times = []
            for _ in range(args.repeat):
                t0 = time.monotonic()
                out = await aio.gather(*[one(c) for c in conns])
                times.append((time.monotonic() - t0) * 1000.0)
                assert len(out) == args.tasks
            return times
        finally:
            for c in conns:
                await c.close()

    times = run(main())
    print(json.dumps({'times': times}))


# -- driver ------------------------------------------------------------------


def measure(args, backend, threads):
    argv = [
        sys.executable,
        os.path.abspath(__file__),
        '--worker',
        '--backend',
        backend,
        '--threads',
        str(threads),
        '--dsn',
        args.dsn,
        '--rows',
        str(args.rows),
        '--tasks',
        str(args.tasks),
        '--cpu',
        str(args.cpu),
        '--repeat',
        str(args.repeat),
        '--warmup',
        str(args.warmup),
    ]
    proc = subprocess.run(argv, capture_output=True, text=True, check=False)  # noqa: S603 - fixed argv, our own worker
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout).strip().splitlines()
        return None, f'exit {proc.returncode}: ' + (tail[-1] if tail else '(no output)')
    line = proc.stdout.strip().splitlines()[-1]
    return json.loads(line)['times'], None


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--dsn', default=os.environ.get('MT_ASYNCIO_BENCH_DSN', DEFAULT_DSN))
    p.add_argument('--rows', type=int, default=50_000, help='rows per query')
    p.add_argument('--tasks', type=int, default=20, help='concurrent connections/queries')
    p.add_argument('--cpu', type=int, default=8, help='per-row Python work')
    p.add_argument('--threads', type=int, nargs='+', default=[1, 2, 4, 8, 12])
    p.add_argument('--repeat', type=int, default=3)
    p.add_argument('--warmup', type=int, default=1)
    p.add_argument('--json', default=None)
    p.add_argument('--worker', action='store_true', help=argparse.SUPPRESS)
    p.add_argument('--backend', default='mt', help=argparse.SUPPRESS)
    args = p.parse_args()

    if args.worker:
        # --threads is a list in the driver and a scalar here
        args.threads = int(args.threads if isinstance(args.threads, int) else args.threads[0])
        worker(args)
        return

    ensure_table(args.dsn, args.rows)
    total_rows = args.rows * args.tasks

    import asyncpg

    print(f'# asyncpg {asyncpg.__version__}, dsn {args.dsn}')
    print(f'# python {platform.python_version()} (free-threaded: {not sys._is_gil_enabled()}), {platform.platform()}')
    print(f'# {args.tasks} concurrent queries x {args.rows:,} rows = {total_rows:,} rows per repeat, cpu={args.cpu}')
    print(f'# median of {args.repeat} repeat(s) after {args.warmup} warmup\n')

    results = {}
    base, err = measure(args, 'stdlib', 1)
    if err:
        print(f'stdlib asyncio: FAILED -- {err}')
        base_ms = None
    else:
        base_ms = statistics.median(base)
        results['stdlib'] = base_ms
        print(f'stdlib asyncio        {base_ms:9.1f} ms   {total_rows / base_ms * 1000:12,.0f} rows/s')

    for t in args.threads:
        times, err = measure(args, 'mt', t)
        if err:
            print(f'mt_asyncio {t:>2}t         {"FAILED":>9}   {err}')
            results[f'mt-{t}t'] = None
            continue
        ms = statistics.median(times)
        results[f'mt-{t}t'] = ms
        speed = f'{base_ms / ms:5.2f}x vs stdlib' if base_ms else ''
        print(f'mt_asyncio {t:>2}t         {ms:9.1f} ms   {total_rows / ms * 1000:12,.0f} rows/s   {speed}')

    ok = [t for t in args.threads if results.get(f'mt-{t}t')]
    if len(ok) > 1:
        one = results[f'mt-{ok[0]}t']
        print(f'\n# self-scaling, {ok[0]}t = 1.00x')
        for t in ok:
            print(f'#   {t:>2}t  {one / results[f"mt-{t}t"]:5.2f}x')

    if args.json:
        os.makedirs(os.path.dirname(args.json) or '.', exist_ok=True)
        with open(args.json, 'w') as fh:
            json.dump({'params': vars(args), 'results': results}, fh, indent=2)
        print(f'\nwrote {args.json}')


if __name__ == '__main__':
    main()
