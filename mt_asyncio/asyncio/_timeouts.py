"""asyncio.timeout / timeout_at, ported from CPython's ``Lib/asyncio/timeouts.py``.

Derived from CPython, Copyright (c) 2001-2026 Python Software Foundation;
All Rights Reserved. Used under the PSF License Agreement (see NOTICE).

Changes from the original: uses this package's loop/task objects
(``get_running_loop``/``current_task``), drops the debug-mode assertions, and
serialises the state transitions under a lock.

That last one is not cosmetic. CPython can leave the handoff between
``_on_timeout`` and ``__aexit__`` unsynchronised because timer callbacks and
task steps are both serialised on the loop thread, so "the body finished" and
"the deadline fired" can never be observed at once. Here they run on different
workers, and unsynchronised the two interleave: ``__aexit__`` cancels the timer
handle (a no-op if the callback is already running elsewhere), leaves the scope,
and ``_on_timeout`` then cancels a task that is no longer inside it -- a stray
``CancelledError`` surfacing somewhere unrelated later. The lock makes "am I
still in the scope?" and "cancel the task" one atomic step against the exit.
"""

from __future__ import annotations

import threading
from asyncio import CancelledError, TimeoutError as _TimeoutError

from ._context import current_task, get_running_loop


_CREATED = 'created'
_ENTERED = 'entered'
_EXPIRING = 'expiring'
_EXPIRED = 'expired'
_EXITED = 'finished'


class Timeout:
    def __init__(self, when):
        self._when = when
        self._state = _CREATED
        self._timeout_handler = None
        self._task = None
        # guards `_state`/`_timeout_handler` between the task exiting the scope
        # and the deadline firing on another worker (see the module docstring)
        self._lock = threading.Lock()

    def when(self):
        return self._when

    def expired(self):
        return self._state in (_EXPIRING, _EXPIRED)

    def reschedule(self, when):
        loop = get_running_loop()
        with self._lock:
            if self._state is not _ENTERED:
                raise RuntimeError(f'Cannot reschedule a timeout in {self._state!r} state')
            self._when = when
            old, self._timeout_handler = self._timeout_handler, None
            if when is not None:
                self._timeout_handler = (
                    loop.call_soon(self._on_timeout) if when <= loop.time() else loop.call_at(when, self._on_timeout)
                )
        if old is not None:
            old.cancel()

    async def __aenter__(self):
        if self._state is not _CREATED:
            raise RuntimeError('Timeout has already been entered')
        task = current_task()
        if task is None:
            raise RuntimeError('Timeout should be used inside a task')
        self._state = _ENTERED
        self._task = task
        if self._when is not None:
            self.reschedule(self._when)
        return self

    async def __aexit__(self, et, exc, tb):
        with self._lock:
            expiring = self._state is _EXPIRING
            if expiring:
                self._state = _EXPIRED
            elif self._state is _ENTERED:
                # leave the scope *under the lock*, so a deadline firing right
                # now finds a state it must not cancel through
                self._state = _EXITED
            handler, self._timeout_handler = self._timeout_handler, None
        if handler is not None:
            handler.cancel()
        # `expiring` means the deadline won the race and cancelled the task from
        # inside the lock, so `uncancel` has to run -- it withdraws the request
        # even when the body finished first and the cancel is still undelivered,
        # which is what keeps it from leaking past this scope.
        if expiring and self._task.uncancel() <= 0 and et is not None and issubclass(et, CancelledError):
            raise _TimeoutError from exc
        return None

    def _on_timeout(self):
        with self._lock:
            self._timeout_handler = None
            if self._state is not _ENTERED:
                return  # the body already left the scope; cancelling now would escape it
            self._state = _EXPIRING
            # under the lock deliberately: releasing it first lets `__aexit__`
            # run its `uncancel` *before* this `cancel`, leaving the request
            # outstanding on a task that has left the scope
            self._task.cancel()


def timeout(delay):
    loop = get_running_loop()
    return Timeout(loop.time() + delay if delay is not None else None)


def timeout_at(when):
    return Timeout(when)
