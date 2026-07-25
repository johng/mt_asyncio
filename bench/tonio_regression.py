"""Regression check: mt_asyncio vs upstream TonIO (from PyPI) on comparable tasks.

mt_asyncio is a fork of TonIO that keeps the Rust runtime core but replaced the whole
Python surface. The question this answers is "did we make the shared core
slower?", which needs care, because the two projects no longer ship the same API:
TonIO ships a native ``async``/``await`` layer (``tonio.colored``) and a
``yield``-based one; mt_asyncio ships only ``mt_asyncio.asyncio``.

So there are two comparisons here, and only the first is a regression test:

**core** -- ``tonio-core`` vs ``mt-asyncio-core`` drive the Rust runtime through its
raw primitives (``Runtime._spawn_*``, ``Event``, ``Waiter``, ``Result``) with
byte-for-byte identical Python. The only difference is which ``.so`` is loaded,
so a gap here IS a change in the Rust core. **This is the regression test.**

**api** -- ``tonio-async`` (``tonio.colored``) vs ``mt-asyncio-async``
(``mt_asyncio.asyncio``) compare what each project actually ships. mt_asyncio is expected
to be slower here and that is not a regression: ``mt_asyncio.asyncio`` implements
cancellation, contextvars, exception groups and the asyncio ``Future`` protocol,
none of which ``tonio.colored`` does. ``tonio-async-ctx`` is the same TonIO layer
with ``context=True``, which prices in the per-step contextvar copy that
``mt_asyncio.asyncio`` always pays, and is the fairer of the two API comparisons.

Every workload is written ONCE against a five-method adapter protocol
(``run``/``yield_now``/``sleep``/``spawn_join``/``offload``) that all backends
implement, so no backend gets a hand-tuned version of the same workload.

Setup::

    uv pip install tonio==0.8.3   # into the same free-threaded venv as mt_asyncio

Usage::

    python bench/tonio_regression.py                    # core + api
    python bench/tonio_regression.py --tier core        # just the regression test
    python bench/tonio_regression.py --threads 1 4 --repeat 5
    python bench/tonio_regression.py --json bench/results/regression.json
"""

from __future__ import annotations

import argparse
import importlib
import json
import platform
import statistics
import subprocess
import sys
import threading
import time


def _burn(n):
    x = 0
    for i in range(n):
        x += i * i
    return x


# ---------------------------------------------------------------------------
# adapters
#
# Five methods, implemented by every backend. `run` is sync; the rest are used
# from inside a coroutine and return awaitables.
# ---------------------------------------------------------------------------


# Both runtimes are per-process singletons that raise if constructed twice, so
# warmup + repeated runs in one process must share one runtime. Every backend
# reuses its runtime the same way, so no backend gets a fresh-runtime advantage.
_RUNTIME = None


class CoreAdapter:
    """Raw runtime primitives, identical Python against either `.so`.

    Deliberately does NOT use anything either project added on top -- notably not
    TonIO's native ``Barrier``/``Lock``/``Semaphore``, which mt_asyncio removed from
    Rust. ``spawn_join`` is a plain Python counter on both sides so the two
    backends run the same instructions.
    """

    tier = 'core'

    def __init__(self, pkg_name):
        self._pkg_name = pkg_name
        # tonio spawns coroutines through `_spawn_pyasyncgen`; mt_asyncio renamed it
        # `_spawn_coro` when the generator-based path was deleted. Same call.
        self._spawn_attr = '_spawn_pyasyncgen' if pkg_name == 'tonio' else '_spawn_coro'

    def run(self, main, threads):
        global _RUNTIME

        pkg = importlib.import_module(self._pkg_name)
        core = importlib.import_module(f'{self._pkg_name}._{self._pkg_name}')
        self._core = core
        if _RUNTIME is None:
            _RUNTIME = pkg.runtime(threads=threads)
        rt = self._rt = _RUNTIME
        self._spawn = getattr(rt, self._spawn_attr)

        done = core.Event()
        state = {}

        async def runner():
            try:
                state['ret'] = await main()
            except BaseException as exc:
                state['exc'] = exc
            finally:
                done.set()

        async def watcher():
            await done.waiter(None)
            rt.stop()

        self._spawn(watcher())
        self._spawn(runner())
        rt.run_forever()

        if 'exc' in state:
            raise state['exc']
        return state.get('ret')

    def yield_now(self):
        ev = self._core.Event()
        ev.set()
        return ev.waiter(None)

    def sleep(self, secs):
        return self._core.Event().waiter(round(max(0, secs) * 1_000_000))

    def spawn_join(self, coros):
        n = len(coros)
        done = self._core.Event()
        lock = threading.Lock()
        left = [n]

        async def wrapper(coro):
            try:
                await coro
            finally:
                with lock:
                    left[0] -= 1
                    last = left[0] == 0
                if last:
                    done.set()

        for coro in coros:
            self._spawn(wrapper(coro))
        return done.waiter(None)

    async def offload(self, fn, *args):
        _ctl, event, res = self._rt._spawn_blocking(fn, *args)
        await event.waiter(None)
        err, val = res.fetch()
        if err is True:
            raise val
        return val


class TonioAsyncAdapter:
    """TonIO's shipped native async API (`tonio.colored`)."""

    tier = 'api'

    def __init__(self, context):
        self._context = context

    def run(self, main, threads):
        global _RUNTIME

        import tonio
        import tonio.colored as tc

        self._tc = tc
        if _RUNTIME is None:
            _RUNTIME = tonio.runtime(threads=threads, context=self._context)
        # `tonio.colored.run` would construct a second runtime; go through the
        # runtime object directly so every repeat takes the same path
        return _RUNTIME.run_until_complete(main())

    def yield_now(self):
        return self._tc.yield_now()

    def sleep(self, secs):
        return self._tc.sleep(secs)

    def spawn_join(self, coros):
        return self._tc.spawn.without_results(*coros)

    def offload(self, fn, *args):
        return self._tc.spawn_blocking(fn, *args)


class MtAsyncioAdapter:
    """mt_asyncio's shipped API (`mt_asyncio.asyncio`)."""

    tier = 'api'

    def run(self, main, threads):
        import mt_asyncio.asyncio as aio

        self._aio = aio
        return aio.run(main(), threads=threads)

    def yield_now(self):
        return self._aio.sleep(0)

    def sleep(self, secs):
        return self._aio.sleep(secs)

    def spawn_join(self, coros):
        return self._aio.gather(*coros)

    def offload(self, fn, *args):
        return self._aio.to_thread(fn, *args)


BACKENDS = {
    'tonio-core': (CoreAdapter, ('tonio',)),
    'mt-asyncio-core': (CoreAdapter, ('mt_asyncio',)),
    'tonio-async': (TonioAsyncAdapter, (False,)),
    'tonio-async-ctx': (TonioAsyncAdapter, (True,)),
    'mt-asyncio-async': (MtAsyncioAdapter, ()),
}

# the pair whose gap means "the Rust core changed"; everything else is context
REGRESSION_PAIR = ('tonio-core', 'mt-asyncio-core')


# ---------------------------------------------------------------------------
# workloads -- written once against the adapter protocol
# ---------------------------------------------------------------------------


def wl_yield_churn(rt, *, steps):
    """One coroutine, N suspensions: the per-await cost with no parallelism."""

    async def main():
        for _ in range(steps):
            await rt.yield_now()

    return main, steps


def wl_spawn_join(rt, *, tasks, steps):
    """Spawn N coroutines that each suspend K times, join them all."""

    async def worker():
        for _ in range(steps):
            await rt.yield_now()

    async def main():
        await rt.spawn_join([worker() for _ in range(tasks)])

    return main, tasks


def wl_cpu_scale(rt, *, tasks, steps, cpu):
    """The case parallelism is for: real work between suspensions."""

    async def worker():
        for _ in range(steps):
            _burn(cpu)
            await rt.yield_now()

    async def main():
        await rt.spawn_join([worker() for _ in range(tasks)])

    return main, tasks * steps


def wl_sleep_timers(rt, *, tasks, steps, delay):
    """Timer arm/fire throughput through the reactor."""

    async def worker():
        for _ in range(steps):
            await rt.sleep(delay)

    async def main():
        await rt.spawn_join([worker() for _ in range(tasks)])

    return main, tasks * steps


def wl_offload(rt, *, calls, cpu):
    """Blocking-pool dispatch."""

    async def main():
        await rt.spawn_join([rt.offload(_burn, cpu) for _ in range(calls)])

    return main, calls


WORKLOADS = {
    'yield_churn': (wl_yield_churn, {'steps': 50_000}, 'awaits', 'one coroutine, N suspensions'),
    'spawn_join': (wl_spawn_join, {'tasks': 2000, 'steps': 20}, 'tasks', 'spawn N coroutines, join all'),
    'cpu_scale': (wl_cpu_scale, {'tasks': 256, 'steps': 4, 'cpu': 100_000}, 'steps', 'CPU work between suspensions'),
    'sleep_timers': (wl_sleep_timers, {'tasks': 500, 'steps': 20, 'delay': 0.001}, 'sleeps', 'real timers'),
    'offload': (wl_offload, {'calls': 2000, 'cpu': 20_000}, 'calls', 'blocking-pool dispatch'),
}

_SCALABLE = ('tasks', 'steps', 'calls')


# ---------------------------------------------------------------------------
# worker / harness
# ---------------------------------------------------------------------------


def _run_worker(backend, workload, params, threads, repeat, warmup):
    cls, args = BACKENDS[backend]
    builder = WORKLOADS[workload][0]

    def run_once():
        rt = cls(*args)
        main, _ = builder(rt, **params)
        t0 = time.monotonic()
        rt.run(main, threads)
        return time.monotonic() - t0

    # NOTE: unlike bench/asyncio_bench.py, warmup+repeat all happen in ONE
    # process, and both runtimes are per-process singletons -- so every run here
    # reuses the runtime the first one created. That is the same for all
    # backends, so the comparison holds.
    for _ in range(warmup):
        run_once()
    times = [run_once() for _ in range(repeat)]
    print(json.dumps({'times': times}))


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


def _versions():
    import tonio

    import mt_asyncio
    from mt_asyncio import _mt_asyncio

    return {
        'python': platform.python_version(),
        'freethreaded': not getattr(sys, '_is_gil_enabled', lambda: True)(),
        'platform': platform.platform(),
        'tonio': tonio.__version__,
        'mt_asyncio': mt_asyncio.__version__,
        # the PyPI tonio wheel is always a release build; ours is whatever was
        # last built, and `maturin develop` defaults to debug. Comparing the two
        # shows a multi-fold "regression" that is purely the build profile.
        'mt_asyncio_build': getattr(_mt_asyncio, '__build_profile__', 'unknown'),
    }


def _emit(lines, out):
    for line in lines:
        print(line)
        out.append(line)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-w', '--workload', nargs='+', default=list(WORKLOADS), metavar='NAME')
    parser.add_argument('-t', '--threads', nargs='+', type=int, default=[1, 4], metavar='N')
    parser.add_argument('--tier', choices=['core', 'api', 'both'], default='both')
    parser.add_argument('-r', '--repeat', type=int, default=3)
    parser.add_argument('--warmup', type=int, default=1)
    parser.add_argument('--scale', type=float, default=1.0)
    parser.add_argument('--json', metavar='PATH')
    parser.add_argument('--markdown', metavar='PATH')
    parser.add_argument('--allow-debug-build', action='store_true', help='compare a debug build anyway')
    parser.add_argument('--worker', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--backend', help=argparse.SUPPRESS)
    parser.add_argument('--params', help=argparse.SUPPRESS)
    parser.add_argument('--wthreads', type=int, default=1, help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.worker:
        _run_worker(args.backend, args.workload[0], json.loads(args.params), args.wthreads, args.repeat, args.warmup)
        return

    unknown = [w for w in args.workload if w not in WORKLOADS]
    if unknown:
        parser.error(f'unknown workload(s): {", ".join(unknown)}')

    backends = [b for b, (cls, _) in BACKENDS.items() if args.tier == 'both' or cls.tier == args.tier]

    vers = _versions()
    if vers['mt_asyncio_build'] == 'debug' and not args.allow_debug_build:
        parser.error(
            'mt_asyncio is a DEBUG build and the tonio wheel from PyPI is a release build. '
            'That comparison shows a multi-fold "regression" that is entirely the build '
            'profile. Rebuild with `maturin develop --release`, or pass --allow-debug-build.'
        )

    report = []
    _emit(
        [
            '# mt_asyncio vs upstream TonIO',
            '',
            f'- tonio {vers["tonio"]} (PyPI, release) vs mt_asyncio {vers["mt_asyncio"]} '
            f'(this tree, {vers["mt_asyncio_build"]} build)',
            f'- python {vers["python"]} (free-threaded: {vers["freethreaded"]}), {vers["platform"]}',
            f'- median of {args.repeat} run(s) after {args.warmup} warmup, scale {args.scale}',
            '',
            f'`{REGRESSION_PAIR[1]}` vs `{REGRESSION_PAIR[0]}` is the regression test: identical Python,',
            "different `.so`. The `-async` rows compare each project's shipped API and are",
            'not like-for-like — `mt_asyncio.asyncio` implements cancellation, contextvars and',
            'the asyncio Future protocol that `tonio.colored` does not.',
        ],
        report,
    )

    results = {}
    for name in args.workload:
        builder, base_params, unit, blurb = WORKLOADS[name]
        params = _scaled(base_params, args.scale)
        _emit(['', f'## {name} — {blurb}', '', f'params={params}', ''], report)
        _emit(
            [
                '| backend | ' + ' | '.join(f'{t}t (ms)' for t in args.threads) + ' |',
                '| --- | ' + ' | '.join(['---'] * len(args.threads)) + ' |',
            ],
            report,
        )
        entry = results[name] = {'unit': unit, 'params': params, 'backends': {}}
        for backend in backends:
            per_thread = {}
            for th in args.threads:
                per_thread[th] = _measure(backend, name, params, th, args.repeat, args.warmup)
            entry['backends'][backend] = per_thread
            cells = ' | '.join(f'{per_thread[t]["median"] * 1000:.1f}' for t in args.threads)
            _emit([f'| `{backend}` | {cells} |'], report)

        if all(b in entry['backends'] for b in REGRESSION_PAIR):
            up, ours = (entry['backends'][b] for b in REGRESSION_PAIR)
            ratios = ' | '.join(f'{up[t]["median"] / ours[t]["median"]:.2f}x' for t in args.threads)
            _emit(['', f'core, mt_asyncio relative to tonio (>1.00x = mt_asyncio faster): {ratios}'], report)

    if 'core' in {BACKENDS[b][0].tier for b in backends}:
        _emit(
            [
                '',
                '## regression summary — `mt-asyncio-core` vs `tonio-core`',
                '',
                'Ratio > 1.00x means mt_asyncio is faster.',
                '',
            ],
            report,
        )
        _emit(
            [
                '| workload | ' + ' | '.join(f'{t}t' for t in args.threads) + ' |',
                '| --- | ' + ' | '.join(['---'] * len(args.threads)) + ' |',
            ],
            report,
        )
        for name in args.workload:
            entry = results[name]
            up, ours = (entry['backends'][b] for b in REGRESSION_PAIR)
            cells = ' | '.join(f'{up[t]["median"] / ours[t]["median"]:.2f}x' for t in args.threads)
            _emit([f'| `{name}` | {cells} |'], report)

    if args.json:
        with open(args.json, 'w') as f:
            json.dump({'versions': vers, 'repeat': args.repeat, 'results': results}, f, indent=2)
        print(f'\nwrote {args.json}')
    if args.markdown:
        with open(args.markdown, 'w') as f:
            f.write('\n'.join(report) + '\n')
        print(f'wrote {args.markdown}')


if __name__ == '__main__':
    main()
