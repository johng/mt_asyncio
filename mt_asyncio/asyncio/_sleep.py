"""``sleep``, and the bare yield it degenerates to.

Apart from the other combinators in :mod:`._ctl` because it is the only one
``_net`` and ``_streams`` want, and ``_ctl`` -> ``_loop`` -> ``_net`` means
reaching back for it would close a cycle. Nothing here needs the loop *class*,
only a running loop, so it sits below all of that.
"""

from __future__ import annotations

from .._mt_asyncio import Event as _Event
from ._context import current_task, get_running_loop


__all__ = ['sleep']


async def _yield_now():
    # the one suspension point that does not go through `Future.__await__`, so
    # it has to hand back connection claims itself (see `_transports`)
    task = current_task()
    if task is not None and task._mt_claims is not None:
        task._mt_release_claims()
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
        return await fut
    finally:
        handle.cancel()


def _set_result_unless_done(fut, value):
    if not fut.done():
        fut.set_result(value)
