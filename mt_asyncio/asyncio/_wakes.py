"""Deferred wakeups: asyncio's "a callback finishes before anything it woke runs".

asyncio never states this guarantee because a single-threaded loop cannot break
it. ``fut.set_result()`` inside a callback only *schedules* the awaiting task;
the task's next step is a separate iteration of the loop, so the callback always
runs to completion first. Protocol authors rely on that, and the reliance is
invisible until a loop steps tasks on other threads.

asyncpg is the case that names the cost. ``coreproto.pyx``::

    cdef _push_result(self):
        try:
            self._on_result()               # -> waiter.set_result(...)
        finally:
            self._set_state(PROTOCOL_IDLE)  # ...only after
            self._reset_result()

Resume the waiter the instant ``set_result`` lands and the awaiting task issues
its next query from another worker while ``_on_result`` is still unwinding --
before the state is returned to IDLE and before the result slots are cleared.
The loud half is ``InternalClientError: cannot switch to state 12; another
operation (12) is in progress``. The quiet half is ``_reset_result`` racing the
next command's result fields, which is data corruption rather than an exception.

So the guarantee has to be put back. While a worker is inside a callback that
may enter user protocol code, wakes it produces are collected and flushed on the
way out, after the callback returns and after the connection lock is dropped.
Nothing is skipped and nothing is reordered relative to other wakes; they are
simply not visible until the callback that caused them is finished.

The region is deliberately not applied to every callback the loop runs -- only
to the transport and TLS entry points, where the protocol contract lives. The
cost on the path that is *not* deferring is one thread-local attribute lookup
per completion.
"""

from __future__ import annotations

import threading


__all__ = ['defer_wakes', 'pending_wakes']

_tls = threading.local()


def pending_wakes():
    """The current thread's pending-wake list, or ``None`` to wake immediately.

    Callers append a zero-argument callable that performs the wake; it runs when
    the innermost :func:`defer_wakes` region exits.
    """
    return getattr(_tls, 'pending', None)


class _DeferWakes:
    """Reentrant per-thread wake-deferral region.

    A single shared instance: all the state is thread-local, so there is nothing
    to allocate per callback. Nesting is counted -- only the outermost exit
    flushes, which is what makes it safe to wrap an entry point that another
    wrapped entry point calls into.
    """

    __slots__ = ()

    def __call__(self):
        return self

    def __enter__(self):
        depth = getattr(_tls, 'depth', 0)
        _tls.depth = depth + 1
        if depth == 0:
            _tls.pending = []
        return self

    def __exit__(self, *exc):
        depth = _tls.depth - 1
        _tls.depth = depth
        if depth:
            return False
        pending, _tls.pending = _tls.pending, None
        # a wake must not be lost because an earlier one raised: every waiter
        # here is parked, and a dropped wake is a hang rather than an error
        for wake in pending:
            try:
                wake()
            except BaseException:  # reported, never swallowed silently
                import traceback

                traceback.print_exc()
        return False


defer_wakes = _DeferWakes()
