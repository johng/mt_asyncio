"""Task, cooperative cancellation, and per-task context tracking.

A Task drives its coroutine on mt_asyncio's multi-threaded runtime (genuine parallel
stepping across workers). Because mt_asyncio's native ``Waiter.abort`` throw machinery
drops re-suspensions and poisons cleanup awaits, cancellation is delivered
*cooperatively*: ``Task.cancel()`` cancels the innermost Future the task is
parked on (``_fut_waiter``), whose ``__await__`` raises ``asyncio.CancelledError``
in Python at the suspension point. Nothing touches mt_asyncio's abort flag, so the
coroutine may ``await`` freely during cleanup and be cancelled again.

``current_task`` / ``get_running_loop`` are ContextVars (tasks migrate between
worker threads, so thread-locals cannot identify them); this requires the mt_asyncio
runtime to be created with ``context=True``.
"""

from __future__ import annotations

import itertools
from asyncio import CancelledError

from .._mt_asyncio import get_runtime
from ._context import (
    _current_task,
    _running_loop,
    current_task,
    get_running_loop,
)
from ._futures import Future, isfuture


__all__ = [
    'Task',
    'current_task',
    'ensure_future',
    'get_running_loop',
]

_task_name_counter = itertools.count(1).__next__


async def _park(fut):
    """Await ``fut``; arming for cancellation now happens in ``Future.__await__``,
    so this is a thin passthrough kept for call-site readability."""
    return await fut


class Task(Future):
    """A coroutine driven on the mt_asyncio runtime, presenting the asyncio.Task API."""

    def __init__(self, coro, *, loop=None, name=None, context=None, eager_start=False):
        # `eager_start` is accepted for signature compatibility -- aiohttp and
        # others construct `asyncio.Task(coro, loop=loop, eager_start=True)`
        # directly -- but is a no-op here. Upstream it means "run the first step
        # synchronously on the calling thread"; our tasks go straight onto the
        # runtime, where a worker may already be stepping them before this
        # constructor returns. It is a latency optimisation, not a semantic.
        if loop is None:
            loop = get_running_loop()
        super().__init__(loop=loop)
        self._coro = coro
        self._name = str(name) if name is not None else f'Task-{_task_name_counter()}'
        self._context = context
        self._fut_waiter = None
        self._must_cancel = False
        self._num_cancels_requested = 0
        loop._register_task(self)
        self._spawn()

    def __repr__(self):
        return f'<Task {self._name!r} state={self._state}>'

    # -- introspection ------------------------------------------------------

    def get_coro(self):
        return self._coro

    def get_name(self):
        return self._name

    def set_name(self, value):
        self._name = str(value)

    def get_context(self):
        return self._context

    def get_stack(self, *, limit=None):
        frames = []
        coro = self._coro
        while coro is not None and (limit is None or len(frames) < limit):
            frame = getattr(coro, 'cr_frame', None)
            if frame is None:
                break
            frames.append(frame)
            coro = getattr(coro, 'cr_await', None)
        return frames

    def print_stack(self, *, limit=None, file=None):
        import traceback

        for frame in self.get_stack(limit=limit):
            traceback.print_stack(frame, limit=1, file=file)

    # -- cancellation (asyncio semantics, cooperative delivery) -------------

    def cancel(self, msg=None):
        if self.done():
            return False
        self._num_cancels_requested += 1
        with self._lock:
            fut_waiter = self._fut_waiter
            self._cancel_message = msg
            if fut_waiter is None:
                self._must_cancel = True
                return True
        # parked on a future: cancel it so the await raises CancelledError
        if fut_waiter.cancel(msg):
            return True
        # the future had already completed; deliver at the next await instead
        with self._lock:
            self._must_cancel = True
        return True

    def cancelling(self):
        return self._num_cancels_requested

    def uncancel(self):
        if self._num_cancels_requested > 0:
            self._num_cancels_requested -= 1
            if self._num_cancels_requested == 0:
                # matches CPython: the last uncancel also withdraws a cancel
                # that has not been delivered yet
                with self._lock:
                    self._must_cancel = False
        return self._num_cancels_requested

    def set_result(self, result):
        raise RuntimeError('Task does not support set_result operation')

    def set_exception(self, exception):
        raise RuntimeError('Task does not support set_exception operation')

    # -- driving ------------------------------------------------------------

    def _spawn(self):
        coro = self._runner()
        rt = get_runtime()
        if self._context is not None:
            self._context.run(rt._spawn_coro, coro)
        else:
            rt._spawn_coro(coro)

    async def _runner(self):
        _current_task.set(self)
        _running_loop.set(self._loop)
        try:
            result = await self._coro
        except CancelledError as exc:
            Future.cancel(self, exc.args[0] if exc.args else None)
        except BaseException as exc:
            Future.set_exception(self, exc)
        else:
            # a cancel requested while the coroutine was running, with no further
            # await to deliver it at, still cancels the task (CPython does this
            # on the StopIteration branch of Task.__step)
            with self._lock:
                pending_cancel = self._must_cancel
                self._must_cancel = False
            if pending_cancel:
                Future.cancel(self, self._cancel_message)
            else:
                Future.set_result(self, result)
        finally:
            self._loop._unregister_task(self)


def ensure_future(coro_or_future, *, loop=None):
    if isfuture(coro_or_future):
        return coro_or_future
    if _iscoroutine(coro_or_future):
        if loop is None:
            loop = get_running_loop()
        return loop.create_task(coro_or_future)
    raise TypeError(f'An asyncio.Future, a coroutine or an awaitable is required, got {coro_or_future!r}')


def _iscoroutine(obj):
    import inspect

    return inspect.iscoroutine(obj)
