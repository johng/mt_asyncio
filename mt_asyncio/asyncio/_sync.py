"""asyncio-compatible synchronization primitives, cancel-safe under parallelism.

Built on the multi-threaded ``Future`` (mt_asyncio-Event-backed) plus a short
``threading.Lock`` for the O(1) queue critical sections (never held across an
await). The native mt_asyncio ``Lock``/``Semaphore``/``Channel`` are deliberately not
used: their wait-queues strand a cancelled waiter (validated deadlock), whereas
our Future tracks cancellation so ``release`` simply skips a cancelled waiter.
"""

from __future__ import annotations

import collections
import heapq
import threading
from asyncio import CancelledError
from types import GenericAlias

from ._context import get_running_loop


__all__ = [
    'BoundedSemaphore',
    'Condition',
    'Event',
    'LifoQueue',
    'Lock',
    'PriorityQueue',
    'Queue',
    'QueueEmpty',
    'QueueFull',
    'Semaphore',
]


class Event:
    def __init__(self):
        self._value = False
        self._mu = threading.Lock()
        self._waiters: list = []

    def is_set(self):
        with self._mu:
            return self._value

    def set(self):
        with self._mu:
            if self._value:
                return
            self._value = True
            waiters = self._waiters
            self._waiters = []
        for fut in waiters:
            if not fut.done():
                fut.set_result(True)

    def clear(self):
        with self._mu:
            self._value = False

    async def wait(self):
        with self._mu:
            if self._value:
                return True
            fut = get_running_loop().create_future()
            self._waiters.append(fut)
        try:
            await fut
        except CancelledError:
            with self._mu:
                if fut in self._waiters:
                    self._waiters.remove(fut)
            raise
        return True


class Lock:
    def __init__(self):
        self._locked = False
        self._mu = threading.Lock()
        self._waiters: collections.deque = collections.deque()

    def locked(self):
        with self._mu:
            return self._locked

    async def acquire(self):
        with self._mu:
            if not self._locked and not self._waiters:
                self._locked = True
                return True
            fut = get_running_loop().create_future()
            self._waiters.append(fut)
        try:
            await fut
        except CancelledError:
            granted = False
            with self._mu:
                try:
                    self._waiters.remove(fut)
                except ValueError:
                    granted = fut.done() and not fut.cancelled()
            if granted:
                self.release()  # we owned it but were cancelled: hand it on
            raise
        return True

    def release(self):
        with self._mu:
            while self._waiters:
                fut = self._waiters.popleft()
                if not fut.done():
                    fut.set_result(True)  # ownership transfer; _locked stays True
                    return
            self._locked = False

    async def __aenter__(self):
        await self.acquire()
        return None

    async def __aexit__(self, *exc):
        self.release()


class Semaphore:
    def __init__(self, value=1):
        if value < 0:
            raise ValueError('Semaphore initial value must be >= 0')
        self._value = value
        self._mu = threading.Lock()
        self._waiters: collections.deque = collections.deque()

    def locked(self):
        # matches CPython: no permits left, or someone is still queued for one
        with self._mu:
            return self._value == 0 or any(not w.cancelled() for w in self._waiters)

    async def acquire(self):
        with self._mu:
            if self._value > 0 and not self._waiters:
                self._value -= 1
                return True
            fut = get_running_loop().create_future()
            self._waiters.append(fut)
        try:
            await fut
        except CancelledError:
            granted = False
            with self._mu:
                try:
                    self._waiters.remove(fut)
                except ValueError:
                    granted = fut.done() and not fut.cancelled()
            if granted:
                self.release()  # permit handed to us but cancelled: pass it on
            raise
        return True

    def release(self):
        with self._mu:
            while self._waiters:
                fut = self._waiters.popleft()
                if not fut.done():
                    fut.set_result(True)  # transfer the permit directly
                    return
            self._value += 1

    async def __aenter__(self):
        await self.acquire()
        return None

    async def __aexit__(self, *exc):
        self.release()


class BoundedSemaphore(Semaphore):
    def __init__(self, value=1):
        super().__init__(value)
        self._bound = value

    def release(self):
        with self._mu:
            if not self._waiters and self._value >= self._bound:
                raise ValueError('BoundedSemaphore released too many times')
            while self._waiters:
                fut = self._waiters.popleft()
                if not fut.done():
                    fut.set_result(True)
                    return
            self._value += 1


class Condition:
    def __init__(self, lock=None):
        self._lock = lock if lock is not None else Lock()
        self.locked = self._lock.locked
        self._mu = threading.Lock()
        self._waiters: collections.deque = collections.deque()

    async def __aenter__(self):
        await self._lock.acquire()
        return None

    async def __aexit__(self, *exc):
        self._lock.release()

    async def wait(self):
        fut = get_running_loop().create_future()
        with self._mu:
            self._waiters.append(fut)
        self._lock.release()
        try:
            await fut
        except CancelledError:
            with self._mu:
                if fut in self._waiters:
                    self._waiters.remove(fut)
            raise
        finally:
            await self._lock.acquire()
        return True

    def notify(self, n=1):
        with self._mu:
            count = 0
            while self._waiters and count < n:
                fut = self._waiters.popleft()
                if not fut.done():
                    fut.set_result(True)
                    count += 1

    def notify_all(self):
        with self._mu:
            waiters = list(self._waiters)
            self._waiters.clear()
        for fut in waiters:
            if not fut.done():
                fut.set_result(True)


class QueueEmpty(Exception):
    pass


class QueueFull(Exception):
    pass


class Queue:
    # as on stdlib Queue: `asyncio.Queue[Item]` appears in type aliases, which
    # are evaluated at import time
    __class_getitem__ = classmethod(GenericAlias)

    def __init__(self, maxsize=0):
        self._maxsize = maxsize
        self._mu = threading.Lock()
        self._getters: collections.deque = collections.deque()
        self._putters: collections.deque = collections.deque()
        self._unfinished = 0
        self._finished = Event()
        self._finished.set()
        self._init(maxsize)

    # -- overridable storage hooks (FIFO by default) ------------------------

    def _init(self, maxsize):
        self._queue: collections.deque = collections.deque()

    def _qlen(self):
        return len(self._queue)

    def _put(self, item):
        self._queue.append(item)

    def _get(self):
        return self._queue.popleft()

    # -- public API ---------------------------------------------------------

    def qsize(self):
        with self._mu:
            return self._qlen()

    @property
    def maxsize(self):
        return self._maxsize

    def empty(self):
        with self._mu:
            return self._qlen() == 0

    def full(self):
        with self._mu:
            return self._maxsize > 0 and self._qlen() >= self._maxsize

    def _put_locked(self, item):
        self._put(item)
        self._unfinished += 1
        self._finished.clear()
        while self._getters:
            fut = self._getters.popleft()
            if not fut.done():
                fut.set_result(None)
                break

    async def put(self, item):
        while True:
            with self._mu:
                if self._maxsize <= 0 or self._qlen() < self._maxsize:
                    self._put_locked(item)
                    return
                fut = get_running_loop().create_future()
                self._putters.append(fut)
            try:
                await fut
            except CancelledError:
                with self._mu:
                    if fut in self._putters:
                        self._putters.remove(fut)
                raise

    def put_nowait(self, item):
        with self._mu:
            if 0 < self._maxsize <= self._qlen():
                raise QueueFull
            self._put_locked(item)

    def _get_locked(self):
        item = self._get()
        while self._putters:
            fut = self._putters.popleft()
            if not fut.done():
                fut.set_result(None)
                break
        return item

    async def get(self):
        while True:
            with self._mu:
                if self._qlen() > 0:
                    return self._get_locked()
                fut = get_running_loop().create_future()
                self._getters.append(fut)
            try:
                await fut
            except CancelledError:
                with self._mu:
                    if fut in self._getters:
                        self._getters.remove(fut)
                raise

    def get_nowait(self):
        with self._mu:
            if self._qlen() == 0:
                raise QueueEmpty
            return self._get_locked()

    def task_done(self):
        with self._mu:
            if self._unfinished <= 0:
                raise ValueError('task_done() called too many times')
            self._unfinished -= 1
            finished = self._unfinished == 0
        if finished:
            self._finished.set()

    async def join(self):
        if self._unfinished > 0:
            await self._finished.wait()


class LifoQueue(Queue):
    def _init(self, maxsize):
        self._queue: list = []

    def _get(self):
        return self._queue.pop()


class PriorityQueue(Queue):
    def _init(self, maxsize):
        self._queue: list = []

    def _put(self, item):
        heapq.heappush(self._queue, item)

    def _get(self):
        return heapq.heappop(self._queue)
