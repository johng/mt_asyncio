"""asyncio.TaskGroup, ported from CPython's ``Lib/asyncio/taskgroups.py``.

Derived from CPython, Copyright (c) 2001-2026 Python Software Foundation;
All Rights Reserved. Used under the PSF License Agreement (see NOTICE).

Changes from the original: adapted to the multi-threaded model. ``_on_task_done``
fires from child completions on arbitrary worker threads (concurrently), so the
group state (``_tasks``/``_errors``/``_on_completed_fut``/flags) is guarded by a
``threading.Lock``, the error is recorded in the same critical section that
empties ``_tasks``, and the parent is woken last. Cancellation uses our
cooperative ``Task.cancel``/``uncancel``/``cancelling``.
"""

from __future__ import annotations

import threading
from asyncio import CancelledError

from ._context import current_task, get_running_loop


class TaskGroup:
    def __init__(self):
        self._entered = False
        self._exiting = False
        self._aborting = False
        self._loop = None
        self._parent_task = None
        self._parent_cancel_requested = False
        self._tasks: set = set()
        self._errors: list = []
        self._base_error = None
        self._on_completed_fut = None
        self._lock = threading.Lock()

    def __repr__(self):
        return f'<TaskGroup tasks={len(self._tasks)} errors={len(self._errors)}>'

    async def __aenter__(self):
        if self._entered:
            raise RuntimeError('TaskGroup has already been entered')
        self._loop = get_running_loop()
        self._parent_task = current_task()
        if self._parent_task is None:
            raise RuntimeError('TaskGroup cannot determine the parent task')
        self._entered = True
        return self

    async def __aexit__(self, et, exc, tb):
        try:
            return await self._aexit(et, exc)
        finally:
            self._parent_task = None
            self._base_error = None

    async def _aexit(self, et, exc):
        self._exiting = True

        if exc is not None and self._is_base_error(exc) and self._base_error is None:
            self._base_error = exc

        propagate_cancellation_error = exc if (et is not None and issubclass(et, CancelledError)) else None

        if et is not None and not self._aborting:
            self._abort()

        # wait for all children; the waiter future may be cancelled repeatedly
        while True:
            with self._lock:
                if not self._tasks:
                    break
                if self._on_completed_fut is None:
                    self._on_completed_fut = self._loop.create_future()
                waiter = self._on_completed_fut
            try:
                await waiter
            except CancelledError as ex:
                if not self._aborting:
                    propagate_cancellation_error = ex
                    self._abort()
            with self._lock:
                self._on_completed_fut = None

        if self._base_error is not None:
            raise self._base_error

        if self._parent_cancel_requested and self._parent_task.uncancel() == 0:
            propagate_cancellation_error = None

        if propagate_cancellation_error is not None and not self._errors:
            raise propagate_cancellation_error

        if et is not None and not issubclass(et, CancelledError):
            self._errors.append(exc)

        if self._errors:
            if self._parent_task.cancelling():
                self._parent_task.uncancel()
                self._parent_task.cancel()
            errors = self._errors
            self._errors = []
            raise BaseExceptionGroup('unhandled errors in a TaskGroup', errors) from None

    def create_task(self, coro, *, name=None, context=None):
        if not self._entered:
            raise RuntimeError('TaskGroup has not been entered')
        if self._exiting and not self._tasks:
            raise RuntimeError('TaskGroup is finished')
        if self._aborting:
            raise RuntimeError('TaskGroup is shutting down')
        task = self._loop.create_task(coro, name=name, context=context)
        with self._lock:
            self._tasks.add(task)
        task.add_done_callback(self._on_task_done)
        return task

    def _is_base_error(self, exc):
        return isinstance(exc, (SystemExit, KeyboardInterrupt))

    def _abort(self):
        self._aborting = True
        with self._lock:
            tasks = list(self._tasks)
        for t in tasks:
            if not t.done():
                t.cancel()

    def _on_task_done(self, task):
        # Unlike CPython, this callback runs on a worker thread while the parent
        # may already be running elsewhere. Two consequences drive the ordering
        # below: the error must be recorded in the *same* critical section that
        # empties `_tasks` (so a parent that observes an empty group has already
        # seen the error), and the parent must be woken **last** (once awake it
        # can finish `_aexit` and drop `_parent_task` out from under us).
        exc = None if task.cancelled() else task.exception()

        do_cancel = False
        parent_done = False
        with self._lock:
            self._tasks.discard(task)
            parent_task = self._parent_task
            if exc is not None:
                self._errors.append(exc)
                if self._is_base_error(exc) and self._base_error is None:
                    self._base_error = exc
                parent_done = parent_task is None or parent_task.done()
                do_cancel = not self._aborting and not self._parent_cancel_requested and not parent_done
                if do_cancel:
                    self._parent_cancel_requested = True

        if exc is not None:
            if parent_done:
                self._loop.call_exception_handler(
                    {
                        'message': 'Task errored out but its parent TaskGroup is already completed',
                        'exception': exc,
                        'task': task,
                    }
                )
            elif do_cancel:
                self._abort()
                parent_task.cancel()

        with self._lock:
            waiter = self._on_completed_fut
            wake = waiter is not None and not self._tasks
        if wake and not waiter.done():
            waiter.set_result(True)
