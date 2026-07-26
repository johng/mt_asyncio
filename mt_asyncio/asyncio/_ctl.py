"""Coroutine combinators and the run() entry point.

All cancellation here is cooperative: children are cancelled via ``Task.cancel()``
(which fires the child's current leaf and raises ``asyncio.CancelledError`` in
Python), never via mt_asyncio's native ``abort()`` (which poisons async cleanup).
Aggregation state touched from done-callbacks is guarded by a ``threading.Lock``
because callbacks run on worker threads, possibly concurrently.
"""

from __future__ import annotations

import functools
import inspect
import threading
from asyncio import CancelledError, TimeoutError as _TimeoutError

from ._loop import EventLoop
from ._tasks import _running_loop, current_task, ensure_future, get_running_loop
from ._timeouts import timeout as _timeout_ctx


__all__ = [
    'current_task',
    'gather',
    'run',
    'shield',
    'wait',
    'wait_for',
]


def run(main, *, debug=None, threads=None):
    if _running_loop.get() is not None:
        raise RuntimeError('mt_asyncio.asyncio.run() cannot be called from a running event loop')
    if not inspect.iscoroutine(main):
        # mirrors asyncio.Runner.run: awaitables are wrapped, anything else is a TypeError
        if inspect.isawaitable(main):
            main = _await_one(main)
        else:
            raise TypeError('An asyncio.Future, a coroutine or an awaitable is required')

    loop = EventLoop(threads=threads)
    try:
        if debug is not None:
            loop.set_debug(debug)
        return loop.run_until_complete(main)
    finally:
        try:
            _cancel_all_tasks(loop)
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            loop.close()


async def _await_one(awaitable):
    return await awaitable


def _cancel_all_tasks(loop):
    to_cancel = loop._all_tasks()
    if not to_cancel:
        return
    for task in to_cancel:
        task.cancel()
    loop.run_until_complete(gather(*to_cancel, return_exceptions=True))


def _release_waiter(waiter, *_args):
    if not waiter.done():
        waiter.set_result(None)


async def _cancel_and_wait(fut):
    """Cancel `fut` and wait for it to settle, on a waiter of our own.

    CPython's helper, same shape: awaiting `fut` directly would make the wait
    depend on the cancellation it is performing.
    """
    loop = get_running_loop()
    waiter = loop.create_future()
    cb = functools.partial(_release_waiter, waiter)
    fut.add_done_callback(cb)
    try:
        fut.cancel()
        await waiter
    finally:
        fut.remove_done_callback(cb)


async def wait_for(aw, timeout):
    """CPython 3.12+'s shape: a cancel scope around a direct await.

    What this replaced wrapped `aw` in a Task, armed a `call_later`, and parked
    on the Task. The intermediate Task is what costs here specifically: on a
    single-threaded loop its completion is a same-thread callback, but on this
    runtime it is a cross-worker wake, so the wrapper cost about as much again
    as the wait it wrapped -- 22.0us against 6.6us for the bare park/wake, on
    psycopg's `wait_for(Event.wait(), 0.1)`. Awaiting `aw` on the calling task
    leaves only the timer.

    `timeout <= 0` keeps the Task: there the awaitable has to be cancelled and
    awaited to completion before `TimeoutError` goes out, and only a future can
    be cancelled that way.
    """
    if timeout is not None and timeout <= 0:
        loop = get_running_loop()
        fut = ensure_future(aw, loop=loop)
        if fut.done():
            return fut.result()
        await _cancel_and_wait(fut)
        try:
            return fut.result()
        except CancelledError as exc:
            raise _TimeoutError from exc

    async with _timeout_ctx(timeout):
        return await aw


def shield(aw):
    loop = get_running_loop()
    inner = ensure_future(aw, loop=loop)
    if inner.done():
        return inner
    outer = loop.create_future()

    def _inner_done(_f):
        if outer.cancelled():
            if not _f.cancelled() and _f.exception() is not None:
                _f.exception()  # retrieve to silence "never retrieved"
            return
        if _f.cancelled():
            outer.cancel()
        else:
            exc = _f.exception()
            if exc is not None:
                outer.set_exception(exc)
            else:
                outer.set_result(_f.result())

    inner.add_done_callback(_inner_done)
    return outer


async def gather(*coros_or_futures, return_exceptions=False):
    loop = get_running_loop()
    if not coros_or_futures:
        return []

    children = [ensure_future(c, loop=loop) for c in coros_or_futures]
    results = [None] * len(children)
    outer = loop.create_future()
    lock = threading.Lock()
    remaining = len(children)
    settled = False

    def _child_done(index, child):
        nonlocal remaining, settled
        with lock:
            if settled:
                return
            if child.cancelled():
                exc = CancelledError()
            else:
                exc = child.exception()
            if exc is not None and not return_exceptions:
                settled = True
                if not outer.done():
                    outer.set_exception(exc)
                return
            results[index] = exc if exc is not None else child.result()
            remaining -= 1
            if remaining == 0:
                settled = True
                if not outer.done():
                    outer.set_result(results)

    for i, child in enumerate(children):
        child.add_done_callback(functools.partial(_child_done, i))

    try:
        return await outer
    except CancelledError:
        for child in children:
            child.cancel()
        raise


_FIRST_COMPLETED = 'FIRST_COMPLETED'
_FIRST_EXCEPTION = 'FIRST_EXCEPTION'
_ALL_COMPLETED = 'ALL_COMPLETED'


async def wait(aws, *, timeout=None, return_when=_ALL_COMPLETED):
    loop = get_running_loop()
    tasks = [ensure_future(a, loop=loop) for a in aws]
    if not tasks:
        raise ValueError('Set of Tasks/Futures is empty.')
    waiter = loop.create_future()
    lock = threading.Lock()
    ndone = 0
    settled = False
    n = len(tasks)

    def _release():
        """Wake the caller. Call with `lock` held."""
        nonlocal settled
        if not settled:
            settled = True
            if not waiter.done():
                waiter.set_result(None)

    def _on_done(t):
        nonlocal ndone
        with lock:
            ndone += 1
            if return_when == _FIRST_COMPLETED:
                _release()
            elif return_when == _FIRST_EXCEPTION and (t.cancelled() or t.exception() is not None):
                _release()
            elif ndone == n:
                _release()

    def _on_timeout():
        with lock:
            _release()

    for t in tasks:
        t.add_done_callback(_on_done)

    handle = None
    if timeout is not None:
        handle = loop.call_later(timeout, _on_timeout)
    try:
        await waiter
    finally:
        if handle is not None:
            handle.cancel()
        for t in tasks:
            t.remove_done_callback(_on_done)

    done, pending = set(), set()
    for t in tasks:
        (done if t.done() else pending).add(t)
    return done, pending
