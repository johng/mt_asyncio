"""mt_asyncio's asyncio API: a single, multi-threaded event loop.

Drop-in usage::

    import mt_asyncio.asyncio as asyncio

    async def main():
        async with asyncio.TaskGroup() as tg:
            tg.create_task(worker())
        await asyncio.gather(a(), b())
        await asyncio.sleep(1)

    asyncio.run(main())

This loop steps asyncio **tasks in true parallel** across mt_asyncio's worker threads
while presenting the asyncio API. Because tasks and callbacks run on many
threads, asyncio's "callbacks are serialized on the loop thread" guarantee does
NOT hold: shared state touched from tasks/callbacks needs the locks provided
here (which are genuinely cross-thread). That is the price of parallelism.

Only awaitables produced by this package (its ``Future``/``Task``/``sleep``/
locks/queues/``sock_*``) are driven correctly; a stock ``asyncio.Future`` cannot
be awaited here (it yields ``self``, which the mt_asyncio driver rejects).

See ``COMPATIBILITY.md`` next to this module for the full API surface and the
list of behavioural differences from CPython's loop; ``tests/test_parity.py``
runs the same scenarios against both this package and stdlib ``asyncio`` and
asserts they agree.
"""

from asyncio import (
    CancelledError as CancelledError,
    InvalidStateError as InvalidStateError,
    TimeoutError as TimeoutError,
)

from ._ctl import (
    gather as gather,
    run as run,
    shield as shield,
    sleep as sleep,
    wait as wait,
    wait_for as wait_for,
)
from ._futures import Future as Future, isfuture as isfuture
from ._loop import EventLoop as EventLoop
from ._net import (
    Server as Server,
    open_connection as open_connection,
    start_server as start_server,
)
from ._streams import (
    StreamReader as StreamReader,
    StreamReaderProtocol as StreamReaderProtocol,
    StreamWriter as StreamWriter,
)
from ._sync import (
    BoundedSemaphore as BoundedSemaphore,
    Condition as Condition,
    Event as Event,
    LifoQueue as LifoQueue,
    Lock as Lock,
    PriorityQueue as PriorityQueue,
    Queue as Queue,
    QueueEmpty as QueueEmpty,
    QueueFull as QueueFull,
    Semaphore as Semaphore,
)
from ._tasks import (
    Task as Task,
    current_task as current_task,
    ensure_future as ensure_future,
    get_running_loop as get_running_loop,
)
from ._tg import TaskGroup as TaskGroup
from ._timeouts import (
    Timeout as Timeout,
    timeout as timeout,
    timeout_at as timeout_at,
)
from ._utils import (
    as_completed as as_completed,
    iscoroutine as iscoroutine,
    iscoroutinefunction as iscoroutinefunction,
    run_coroutine_threadsafe as run_coroutine_threadsafe,
    to_thread as to_thread,
    wrap_future as wrap_future,
)


def new_event_loop(threads: int | None = None) -> EventLoop:
    return EventLoop(threads=threads)


def get_event_loop() -> EventLoop:
    return get_running_loop()


def all_tasks(loop=None):
    if loop is None:
        loop = get_running_loop()
    return loop._all_tasks()


def create_task(coro, *, name=None, context=None):
    return get_running_loop().create_task(coro, name=name, context=context)


FIRST_COMPLETED = 'FIRST_COMPLETED'
FIRST_EXCEPTION = 'FIRST_EXCEPTION'
ALL_COMPLETED = 'ALL_COMPLETED'


# imported last: `_compat` reaches back into this namespace, but only from inside
# its functions, so the package is fully built by the time anything runs
from . import _compat as compat  # noqa: E402


__all__ = [
    'ALL_COMPLETED',
    'BoundedSemaphore',
    'CancelledError',
    'Condition',
    'Event',
    'EventLoop',
    'FIRST_COMPLETED',
    'FIRST_EXCEPTION',
    'Future',
    'InvalidStateError',
    'LifoQueue',
    'Lock',
    'PriorityQueue',
    'Queue',
    'QueueEmpty',
    'QueueFull',
    'Semaphore',
    'Server',
    'StreamReader',
    'StreamReaderProtocol',
    'StreamWriter',
    'Task',
    'TaskGroup',
    'Timeout',
    'TimeoutError',
    'all_tasks',
    'as_completed',
    'compat',
    'create_task',
    'current_task',
    'ensure_future',
    'gather',
    'get_event_loop',
    'get_running_loop',
    'iscoroutine',
    'iscoroutinefunction',
    'isfuture',
    'new_event_loop',
    'open_connection',
    'run',
    'start_server',
    'run_coroutine_threadsafe',
    'shield',
    'sleep',
    'timeout',
    'timeout_at',
    'to_thread',
    'wait',
    'wait_for',
    'wrap_future',
]
