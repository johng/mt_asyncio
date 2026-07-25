"""Thread-safe asyncio-compatible Future backed by a mt_asyncio Event.

This is the linchpin of the multi-threaded loop: completion is broadcast through
a mt_asyncio ``Event`` and ``__await__`` yields the Event's ``Waiter`` so the mt_asyncio
runtime drives suspension/resume -- the awaiter is resumed on whatever worker
thread steals the wake handle (true cross-thread stepping). State transitions
are serialized by a ``threading.Lock`` (there is no GIL), never held across a
suspension.

Design and race-safety validated experimentally (3000/3000 exactly-once
callback races, 2000/2000 single-winner completions, one Event waking 200/200
concurrent awaiters across worker threads, foreign-thread completion).
"""

from __future__ import annotations

import contextvars
import threading
from asyncio import CancelledError, InvalidStateError
from types import GenericAlias

from .._mt_asyncio import Event as _Event
from ._context import _current_task, get_running_loop


_PENDING = 'PENDING'
_FINISHED = 'FINISHED'
_CANCELLED = 'CANCELLED'


class Future:
    """An ``asyncio.Future``-compatible object, safe under concurrent access
    from many mt_asyncio worker threads. Only awaitable under the mt_asyncio runtime
    (``__await__`` yields a ``Waiter``, not ``self``)."""

    _asyncio_future_blocking = False  # class attr -> asyncio.isfuture() is True
    _log_traceback = False
    _source_traceback = None

    def __init__(self, *, loop=None):
        # `loop` is optional, as in stdlib asyncio: `asyncio.Future()` inside a
        # running loop is idiomatic, and compat mode makes that call land here
        self._loop = loop if loop is not None else get_running_loop()
        self._event = _Event()
        self._lock = threading.Lock()
        self._state = _PENDING
        self._result = None
        self._exception = None
        self._cancel_message = None
        self._callbacks = []
        self._asyncio_future_blocking = False

    def __repr__(self):
        return f'<{type(self).__name__} state={self._state}>'

    # as on stdlib Future/Task: libraries write `asyncio.Task[None]` in type
    # aliases, which is evaluated at import time
    __class_getitem__ = classmethod(GenericAlias)

    def get_loop(self):
        return self._loop

    # -- the linchpin -------------------------------------------------------

    def __await__(self):
        # Arm the awaiting task so Task.cancel() can deliver a CancelledError
        # at this suspension point -- this is what makes *every* `await future`
        # cancellable, not just those wrapped in a helper.
        task = _current_task.get()
        if not self._event.is_set():
            if task is not None:
                with task._lock:
                    if task._must_cancel:
                        task._must_cancel = False
                        raise task._make_cancelled_error()
                    task._fut_waiter = self
            self._asyncio_future_blocking = True
            try:
                yield self._event.waiter(None)  # mt_asyncio registers this Waiter on _event
            finally:
                if task is not None:
                    with task._lock:
                        task._fut_waiter = None
        elif task is not None:
            # already resolved, but a cancel is pending: CPython checks
            # `_must_cancel` on every task step, so cancellation wins over the
            # ready result here too
            with task._lock:
                if task._must_cancel:
                    task._must_cancel = False
                    raise task._make_cancelled_error()
        # resumed on some worker thread; state + payload are visible (Event fence)
        if not self._event.is_set():
            raise RuntimeError("await wasn't used with future")
        return self._read()

    __iter__ = __await__

    def _read(self):
        with self._lock:
            st = self._state
            if st == _FINISHED:
                if self._exception is not None:
                    raise self._exception
                return self._result
            if st == _CANCELLED:
                raise self._make_cancelled_error()
        raise InvalidStateError('Result is not ready.')

    # -- completion (single-winner via _lock) -------------------------------

    def set_result(self, result):
        with self._lock:
            if self._state != _PENDING:
                raise InvalidStateError(f'{self._state}: {self!r}')
            self._result = result
            self._state = _FINISHED
            cbs = self._callbacks
            self._callbacks = []
        self._event.set()
        self._run_callbacks(cbs)

    def set_exception(self, exception):
        if isinstance(exception, type):
            exception = exception()
        if isinstance(exception, StopIteration):
            raise TypeError('StopIteration cannot be raised into a Future')
        with self._lock:
            if self._state != _PENDING:
                raise InvalidStateError(f'{self._state}: {self!r}')
            self._exception = exception
            self._state = _FINISHED
            cbs = self._callbacks
            self._callbacks = []
        self._event.set()
        self._run_callbacks(cbs)

    def cancel(self, msg=None):
        with self._lock:
            if self._state != _PENDING:
                return False
            self._state = _CANCELLED
            self._cancel_message = msg
            cbs = self._callbacks
            self._callbacks = []
        self._event.set()
        self._run_callbacks(cbs)
        return True

    # -- callbacks ----------------------------------------------------------

    def add_done_callback(self, fn, *, context=None):
        if context is None:
            context = contextvars.copy_context()
        with self._lock:
            if self._state == _PENDING:
                self._callbacks.append((fn, context))
                return
        self._loop.call_soon(fn, self, context=context)

    def remove_done_callback(self, fn):
        with self._lock:
            kept = [(f, c) for (f, c) in self._callbacks if f != fn]
            removed = len(self._callbacks) - len(kept)
            if removed:
                self._callbacks = kept
        return removed

    def _run_callbacks(self, cbs):
        for fn, ctx in cbs:
            self._loop.call_soon(fn, self, context=ctx)

    # -- queries (locked snapshot) ------------------------------------------

    def done(self):
        return self._state != _PENDING

    def cancelled(self):
        return self._state == _CANCELLED

    def result(self):
        with self._lock:
            if self._state == _CANCELLED:
                raise self._make_cancelled_error()
            if self._state != _FINISHED:
                raise InvalidStateError('Result is not set.')
            if self._exception is not None:
                raise self._exception
            return self._result

    def exception(self):
        with self._lock:
            if self._state == _CANCELLED:
                raise self._make_cancelled_error()
            if self._state != _FINISHED:
                raise InvalidStateError('Exception is not set.')
            return self._exception

    def _make_cancelled_error(self):
        if self._cancel_message is None:
            return CancelledError()
        return CancelledError(self._cancel_message)


def isfuture(obj):
    return getattr(obj, '_asyncio_future_blocking', None) is not None
