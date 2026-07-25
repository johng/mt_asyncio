"""asyncio.timeout / timeout_at, ported from CPython's ``Lib/asyncio/timeouts.py``.

Derived from CPython, Copyright (c) 2001-2026 Python Software Foundation;
All Rights Reserved. Used under the PSF License Agreement (see NOTICE).

Changes from the original: uses this package's loop/task objects
(``get_running_loop``/``current_task``) and drops the debug-mode assertions.
"""

from __future__ import annotations

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

    def when(self):
        return self._when

    def expired(self):
        return self._state in (_EXPIRING, _EXPIRED)

    def reschedule(self, when):
        if self._state is not _ENTERED:
            raise RuntimeError(f'Cannot reschedule a timeout in {self._state!r} state')
        self._when = when
        if self._timeout_handler is not None:
            self._timeout_handler.cancel()
        if when is None:
            self._timeout_handler = None
        else:
            loop = get_running_loop()
            if when <= loop.time():
                self._timeout_handler = loop.call_soon(self._on_timeout)
            else:
                self._timeout_handler = loop.call_at(when, self._on_timeout)

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
        if self._state is _EXPIRING:
            self._state = _EXPIRED
            if self._task.uncancel() <= 0 and et is not None and issubclass(et, CancelledError):
                raise _TimeoutError from exc
        elif self._state is _ENTERED:
            self._state = _EXITED
        if self._timeout_handler is not None:
            self._timeout_handler.cancel()
            self._timeout_handler = None
        return None

    def _on_timeout(self):
        if self._state is _ENTERED:
            self._task.cancel()
            self._state = _EXPIRING
        self._timeout_handler = None


def timeout(delay):
    loop = get_running_loop()
    return Timeout(loop.time() + delay if delay is not None else None)


def timeout_at(when):
    return Timeout(when)
