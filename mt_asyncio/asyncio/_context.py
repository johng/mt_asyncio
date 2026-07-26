"""Per-task context: the running loop and current task.

These are ContextVars (not thread-locals) because tasks migrate between worker
threads as they resume. They live in their own module so both ``_futures`` (whose
``Future.__await__`` arms the current task for cancellation) and ``_tasks`` can
import them without a cycle. Correct propagation requires the mt_asyncio runtime be
created with ``context=True``.
"""

from __future__ import annotations

import contextvars


_current_task: contextvars.ContextVar = contextvars.ContextVar('mt_asyncio_current_task', default=None)
_running_loop: contextvars.ContextVar = contextvars.ContextVar('mt_asyncio_running_loop', default=None)


def current_task(loop=None):
    return _current_task.get()


def get_running_loop():
    loop = _running_loop.get()
    if loop is None:
        raise RuntimeError('no running event loop')
    return loop
