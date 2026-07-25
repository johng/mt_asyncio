"""Coroutine combinators and the run() entry point.

All cancellation here is cooperative: children are cancelled via ``Task.cancel()``
(which fires the child's current leaf and raises ``asyncio.CancelledError`` in
Python), never via mt_asyncio's native ``abort()`` (which poisons async cleanup).
Aggregation state touched from done-callbacks is guarded by a ``threading.Lock``
because callbacks run on worker threads, possibly concurrently.
"""

from __future__ import annotations

import functools
import threading
from asyncio import CancelledError, TimeoutError as _TimeoutError

from .._mt_asyncio import Event as _Event
from ._loop import EventLoop
from ._tasks import _park, _running_loop, current_task, ensure_future, get_running_loop


__all__ = [
    'gather',
    'run',
    'shield',
    'sleep',
    'wait',
    'wait_for',
]


def run(main, *, debug=None, threads=None):
    if _running_loop.get() is not None:
        raise RuntimeError('mt_asyncio.asyncio.run() cannot be called from a running event loop')
    import inspect

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


async def _yield_now():
    ev = _Event()
    ev.set()
    await ev.waiter(None)


async def sleep(delay, result=None):
    loop = get_running_loop()
    if delay <= 0:
        await _yield_now()
        return result
    fut = loop.create_future()
    handle = loop.call_later(delay, _set_result_unless_done, fut, result)
    try:
        return await _park(fut)
    finally:
        handle.cancel()


def _set_result_unless_done(fut, value):
    if not fut.done():
        fut.set_result(value)


async def wait_for(aw, timeout):
    loop = get_running_loop()
    task = ensure_future(aw, loop=loop)
    if timeout is None:
        return await _park(task)
    if timeout <= 0:
        if task.done():
            return task.result()
        task.cancel()
        try:
            await _park(task)
        except CancelledError:
            pass
        raise _TimeoutError from None

    state = {'timed_out': False}

    def _on_timeout():
        if not task.done():
            state['timed_out'] = True
            task.cancel()

    handle = loop.call_later(timeout, _on_timeout)
    try:
        return await _park(task)
    except CancelledError:
        if state['timed_out'] and task.cancelled():
            raise _TimeoutError from None
        raise
    finally:
        handle.cancel()


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
        empty = loop.create_future()
        empty.set_result([])
        return await _park(empty)

    children = [ensure_future(c, loop=loop) for c in coros_or_futures]
    n = len(children)
    results = [None] * n
    outer = loop.create_future()
    lock = threading.Lock()
    state = {'remaining': n, 'settled': False}

    def _child_done(index, child):
        with lock:
            if state['settled']:
                return
            if child.cancelled():
                exc = CancelledError()
                if return_exceptions:
                    results[index] = exc
                else:
                    state['settled'] = True
                    if not outer.done():
                        outer.set_exception(exc)
                    return
            else:
                exc = child.exception()
                if exc is not None:
                    if return_exceptions:
                        results[index] = exc
                    else:
                        state['settled'] = True
                        if not outer.done():
                            outer.set_exception(exc)
                        return
                else:
                    results[index] = child.result()
            state['remaining'] -= 1
            if state['remaining'] == 0:
                state['settled'] = True
                if not outer.done():
                    outer.set_result(results)

    for i, child in enumerate(children):
        child.add_done_callback(functools.partial(_child_done, i))

    try:
        return await _park(outer)
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
    counter = {'done': 0, 'settled': False}
    n = len(tasks)

    def _release():
        if not counter['settled']:
            counter['settled'] = True
            if not waiter.done():
                waiter.set_result(None)

    def _on_done(t):
        with lock:
            counter['done'] += 1
            if return_when == _FIRST_COMPLETED:
                _release()
            elif return_when == _FIRST_EXCEPTION and (t.cancelled() or t.exception() is not None):
                _release()
            elif counter['done'] == n:
                _release()

    for t in tasks:
        t.add_done_callback(_on_done)

    handle = None
    if timeout is not None:
        handle = loop.call_later(timeout, lambda: (_with_lock(lock, _release)))
    try:
        await _park(waiter)
    finally:
        if handle is not None:
            handle.cancel()
        for t in tasks:
            t.remove_done_callback(_on_done)

    done, pending = set(), set()
    for t in tasks:
        (done if t.done() else pending).add(t)
    return done, pending


def _with_lock(lock, fn):
    with lock:
        fn()


# re-export helpers used elsewhere
__all__ += ['current_task']
