"""Third-party networking libraries must behave identically on both backends.

Each case in ``tests/_netcase.py`` is run twice -- once driving stdlib
``asyncio``, once driving ``mt_asyncio.asyncio`` with ``compat.install()`` -- and
the two results must be equal. This is the acceptance test for the transport
layer: real HTTP, real TLS, real WebSocket framing, real libpq waiting, exercised
by libraries that know nothing about us.

Each arm runs in its own process. ``compat.install()`` is process-global, and
libraries bind names at import time (psycopg's ``waiting.py`` starts with
``from asyncio import Event, get_event_loop, wait_for``), so whichever backend is
installed first would decide what every later import sees.
"""

import json
import os
import subprocess
import sys

import pytest


TIMEOUT = 180

CASES = [
    pytest.param('aiohttp_http', 'aiohttp', id='aiohttp-http'),
    pytest.param('aiohttp_tls', 'aiohttp', id='aiohttp-tls'),
    pytest.param('websockets_echo', 'websockets', id='websockets'),
    pytest.param('psycopg_wait_pattern', 'psycopg', id='psycopg-wait-pattern'),
    pytest.param('stdlib_streams_via_compat', None, id='streams-via-compat'),
]

# Not covered, deliberately:
#   httpx  -- drives anyio, whose cancel-scope machinery re-schedules
#             _deliver_cancellation until it observes the target cancelled and
#             livelocks against our cooperative cancellation.
# The aiohttp and websockets cases stop short of server shutdown; see their
# docstrings and "Shared state across tasks" in COMPATIBILITY.md.


def _run(backend, case):
    proc = subprocess.run(  # noqa: S603 - fixed argv, our own test runner
        [sys.executable, '-m', 'tests._netcase', backend, case],
        capture_output=True,
        text=True,
        timeout=TIMEOUT,
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    )
    if proc.returncode != 0:
        pytest.fail(
            f'{case} [{backend}] exited {proc.returncode}\n--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}'
        )
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError:
        pytest.fail(
            f'{case} [{backend}] produced no JSON result\n--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}'
        )


@pytest.mark.parametrize(('case', 'requires'), CASES)
def test_library_parity(case, requires):
    if requires is not None:
        pytest.importorskip(requires)
    if case.endswith('_tls'):
        pytest.importorskip('trustme')

    stdlib_result = _run('stdlib', case)
    mt_result = _run('mt_asyncio', case)
    assert mt_result == stdlib_result


@pytest.mark.skipif(
    not os.environ.get('MT_ASYNCIO_TEST_DSN'),
    reason='set MT_ASYNCIO_TEST_DSN to run psycopg against a real Postgres',
)
def test_psycopg_against_postgres():
    pytest.importorskip('psycopg')
    assert _run('mt_asyncio', 'psycopg_db') == _run('stdlib', 'psycopg_db')
