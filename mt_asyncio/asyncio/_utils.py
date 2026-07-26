"""Smaller asyncio-compatible helpers: to_thread, as_completed,
run_coroutine_threadsafe, wrap_future, and coroutine predicates."""

from __future__ import annotations

import concurrent.futures as _cf
import contextvars
import functools
import inspect
from asyncio import TimeoutError as _TimeoutError

from ._context import get_running_loop
from ._futures import isfuture
from ._loop import _wrap_concurrent_future
from ._sync import Queue
from ._tasks import ensure_future


def iscoroutine(obj):
    return inspect.iscoroutine(obj)


def iscoroutinefunction(func):
    return inspect.iscoroutinefunction(func)


async def to_thread(func, /, *args, **kwargs):
    loop = get_running_loop()
    ctx = contextvars.copy_context()
    call = functools.partial(ctx.run, func, *args, **kwargs)
    return await loop.run_in_executor(None, call)


def as_completed(fs, *, timeout=None):
    loop = get_running_loop()
    todo = {ensure_future(f, loop=loop) for f in fs}
    done: Queue = Queue()
    timeout_handle = None

    def _on_timeout():
        for f in list(todo):
            f.remove_done_callback(_on_completion)
            done.put_nowait(None)
        todo.clear()

    def _on_completion(f):
        if f not in todo:
            return
        todo.discard(f)
        done.put_nowait(f)
        if not todo and timeout_handle is not None:
            timeout_handle.cancel()

    async def _wait_for_one():
        f = await done.get()
        if f is None:
            raise _TimeoutError
        return f.result()

    for f in todo:
        f.add_done_callback(_on_completion)
    if todo and timeout is not None:
        timeout_handle = loop.call_later(timeout, _on_timeout)
    for _ in range(len(todo)):
        yield _wait_for_one()


def run_coroutine_threadsafe(coro, loop):
    if not iscoroutine(coro):
        raise TypeError('A coroutine object is required')
    future: _cf.Future = _cf.Future()

    def _callback():
        try:
            task = loop.create_task(coro)
        except BaseException as exc:
            if future.set_running_or_notify_cancel():
                future.set_exception(exc)
            raise
        task.add_done_callback(_chain)

    def _chain(task):
        if future.cancelled():
            return
        if not future.set_running_or_notify_cancel():
            return
        if task.cancelled():
            future.cancel()
        elif task.exception() is not None:
            future.set_exception(task.exception())
        else:
            future.set_result(task.result())

    loop.call_soon_threadsafe(_callback)
    return future


def wrap_future(future, *, loop=None):
    if isfuture(future):
        return future
    if not isinstance(future, _cf.Future):
        raise TypeError(f'A concurrent.futures.Future is required, got {future!r}')
    if loop is None:
        loop = get_running_loop()
    return _wrap_concurrent_future(future, loop)
