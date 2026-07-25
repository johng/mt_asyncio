"""Compatibility mode: let code that does ``import asyncio`` run on this loop.

``import mt_asyncio.asyncio as asyncio`` only redirects the names *you* write. A
third-party library does ``import asyncio`` internally and gets CPython's, which
this loop cannot drive correctly -- not because the C ``Future``/``Task`` are
single-thread-bound (on 3.14t they are not: the current-task slot lives in thread
state and ``Future`` is internally locked) but because the *pure-Python* layer
above them has no synchronisation at all::

    asyncio.tasks       threading.Lock refs: 0
    asyncio.locks       threading.Lock refs: 0
    asyncio.taskgroups  threading.Lock refs: 0

``gather._done_callback`` does an unsynchronised ``nfinished += 1``; lose one
increment and the outer future never resolves, so the failure is a **hang**.
``Lock.acquire`` check-then-sets ``_locked`` over a bare ``deque``.

This module points those names at mt_asyncio's own implementations, which are
locked for exactly this reason.

Install **before** importing the libraries that should see it -- shadowing only
rebinds module attributes, so a module that already did ``from asyncio import
Lock`` holds the original forever::

    import mt_asyncio.asyncio as asyncio
    asyncio.compat.install()

    import some_library          # its `import asyncio` now resolves to us
    asyncio.run(some_library.main())

That ordering is the whole contract, and it is what keeps this module small: with
it, every route to a genuine stdlib future is closed (``asyncio.Future``,
``asyncio.futures.Future`` and the ``asyncio.tasks`` internals are all rebound;
subclassing after install subclasses ours; no stdlib ``Task`` is ever constructed
because ``Task`` is shadowed too). So the runtime does not need to know how to
park on a foreign awaitable, and awaiting one stays a ``TypeError`` naming the
problem. The only construct that would slip through is a hand-rolled
``_asyncio_future_blocking`` awaitable, which appears nowhere in the stdlib
outside ``asyncio`` itself.

What this does *not* supply is transports: ``create_connection``,
``create_server`` and ``add_reader``/``add_writer`` are still missing, so
libraries that open their own sockets (asyncpg, psycopg's async API, aiohttp)
remain out of reach. See ``COMPATIBILITY.md``.
"""

from __future__ import annotations

import sys
import threading
from typing import Any


# Names shadowed in the stdlib asyncio namespace. Every one of these exists in
# `mt_asyncio.asyncio` and means the same operation there. Deliberately absent:
# `CancelledError`/`InvalidStateError`/`TimeoutError` (we re-export the stdlib
# objects, so patching is a no-op) and anything transport-shaped, which we cannot
# honour and must keep failing loudly.
_SHADOWED = (
    'ALL_COMPLETED',
    'BoundedSemaphore',
    'Condition',
    'Event',
    'FIRST_COMPLETED',
    'FIRST_EXCEPTION',
    'Future',
    'LifoQueue',
    'Lock',
    'PriorityQueue',
    'Queue',
    'QueueEmpty',
    'QueueFull',
    'Semaphore',
    'StreamReader',
    'StreamReaderProtocol',
    'StreamWriter',
    'Task',
    'TaskGroup',
    'Timeout',
    'all_tasks',
    'as_completed',
    'create_task',
    'current_task',
    'ensure_future',
    'gather',
    'get_event_loop',
    'get_running_loop',
    'iscoroutine',
    'iscoroutinefunction',
    'isfuture',
    'open_connection',
    'run',
    'run_coroutine_threadsafe',
    'shield',
    'sleep',
    'start_server',
    'timeout',
    'timeout_at',
    'to_thread',
    'wait',
    'wait_for',
    'wrap_future',
)

# `from asyncio.locks import Lock` binds through the submodule, so patch the
# top-level package *and* the submodules that re-export these names.
_TARGET_MODULES = (
    'asyncio',
    'asyncio.events',
    'asyncio.futures',
    'asyncio.locks',
    'asyncio.queues',
    'asyncio.runners',
    'asyncio.streams',
    'asyncio.taskgroups',
    'asyncio.tasks',
    'asyncio.threads',
    'asyncio.timeouts',
)

_lock = threading.Lock()
_installed: list[tuple[Any, str, Any]] | None = None


def install() -> None:
    """Point the stdlib asyncio namespace at mt_asyncio's implementations.

    Process-global and idempotent. Call it before importing the libraries that
    should see it -- a module that already did ``from asyncio import Lock`` holds
    the original and will not be updated.
    """
    global _installed

    with _lock:
        if _installed is not None:
            return

        import mt_asyncio.asyncio as mt

        saved: list[tuple[Any, str, Any]] = []
        for mod_name in _TARGET_MODULES:
            mod = sys.modules.get(mod_name)
            if mod is None:
                continue
            for name in _SHADOWED:
                replacement = getattr(mt, name, None)
                if replacement is None or not hasattr(mod, name):
                    continue
                saved.append((mod, name, getattr(mod, name)))
                setattr(mod, name, replacement)

        _installed = saved


def uninstall() -> None:
    """Undo :func:`install`, restoring every patched name."""
    global _installed

    with _lock:
        if _installed is None:
            return
        for mod, name, original in reversed(_installed):
            setattr(mod, name, original)
        _installed = None


def is_installed() -> bool:
    return _installed is not None


class _Installed:
    """Context manager form, mostly for tests."""

    def __enter__(self) -> None:
        install()

    def __exit__(self, *exc: object) -> None:
        uninstall()


def installed() -> _Installed:
    return _Installed()
