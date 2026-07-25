"""Keep ``bench/asyncio_bench.py`` honest.

The benchmark suite is not imported by anything else, so it rots silently -- its
predecessor sat broken with an undefined name in the workload registry until
someone tried to run it. These tests build and run every workload at a
micro-scale under *both* backends, which costs a fraction of a second and
catches registry typos, signature drift, and any API the suite uses that
``mt_asyncio.asyncio`` stops providing.
"""

import asyncio
import importlib.util
import os
import sys
from pathlib import Path

import pytest

import mt_asyncio.asyncio as taio


BENCH = Path(__file__).resolve().parent.parent / 'bench' / 'asyncio_bench.py'

# shrink every workload to "runs at all" size; keys absent from a workload's
# params are ignored, so one table covers all of them
TINY = {
    'tasks': 4,
    'items': 8,
    'steps': 2,
    'rounds': 2,
    'conns': 2,
    'calls': 2,
    'consumers': 2,
    'limit': 2,
    'cpu': 100,
    'delay': 0.001,
    'msgsize': 64,
    'queries': 4,
    'pool_size': 2,
    'latency': 0.001,
}


def _load():
    spec = importlib.util.spec_from_file_location('asyncio_bench', BENCH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules['asyncio_bench'] = mod
    spec.loader.exec_module(mod)
    return mod


bench = _load()


def _tiny(params):
    return {k: TINY[k] if k in TINY else v for k, v in params.items()}


def test_registry_is_wellformed():
    assert bench.WORKLOADS
    for name, entry in bench.WORKLOADS.items():
        builder, params, unit, blurb = entry
        assert callable(builder), name
        assert isinstance(params, dict) and params, name
        assert isinstance(unit, str) and unit, name
        assert isinstance(blurb, str) and blurb, name


#: workloads that cannot run without external infrastructure
_NEEDS_DSN = {'db_query_async'}


def _skip_without_dsn(name):
    if name in _NEEDS_DSN and not os.environ.get('MT_ASYNCIO_BENCH_DSN'):
        pytest.skip(f'{name} needs MT_ASYNCIO_BENCH_DSN')


@pytest.mark.parametrize('name', list(bench.WORKLOADS))
def test_workload_runs_on_stdlib(name):
    _skip_without_dsn(name)
    builder, params, _unit, _blurb = bench.WORKLOADS[name]
    main, ops = builder(asyncio, **_tiny(params))
    assert ops > 0
    asyncio.run(main())


@pytest.mark.parametrize('name', list(bench.WORKLOADS))
def test_workload_runs_on_mt_asyncio(name):
    _skip_without_dsn(name)
    builder, params, _unit, _blurb = bench.WORKLOADS[name]
    main, ops = builder(taio, **_tiny(params))
    assert ops > 0
    taio.run(main())


def test_scaled_only_touches_size_knobs():
    scaled = bench._scaled({'tasks': 100, 'cpu': 5000, 'delay': 0.5}, 0.1)
    assert scaled == {'tasks': 10, 'cpu': 5000, 'delay': 0.5}
    # never scales below a single unit of work
    assert bench._scaled({'tasks': 3}, 0.001) == {'tasks': 1}
