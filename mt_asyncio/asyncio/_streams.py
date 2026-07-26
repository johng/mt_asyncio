"""Streams: CPython's, with the check-then-park races closed.

``StreamReader``'s parsing layer -- ``read``, ``readline``, ``readuntil``,
``readexactly``, ``__aiter__`` -- is reused untouched. What is not safe is the
handoff between the transport feeding data and the task waiting for it, because
it is split across two objects::

    # reading task                      # feed_data, on another worker
    while len(self._buffer) < n:
                                        self._buffer.extend(data)
                                        self._wakeup_waiter()   # _waiter None
        self._waiter = create_future()
        await self._waiter              # never woken

An external lock around ``_wait_for_data`` cannot fix that: the *check* belongs
to the caller, outside anything we can wrap. So the wake condition becomes a
generation counter. Every feed bumps ``_feed_gen``; ``_wait_for_data`` parks only
if nothing has been fed since it last looked. Either it observes a new generation
and returns, or it registers under the lock and ``_wakeup_waiter`` finds it there.

Two conditions govern that early return, and both are load-bearing. It triggers
on a *generation change* rather than on "buffer is non-empty", which is what
stops ``readexactly(n)`` spinning: with data present but fewer than ``n`` bytes,
the next call sees the generation unchanged and parks properly. And it returns
only when there is in fact something to see -- a non-empty buffer, EOF or an
exception -- because ``read(n)`` tests its condition once rather than looping, so
returning with an empty buffer would have it report a stream that ended. See
``_wait_for_data``.

Lock order is **transport -> flow control -> reader**. Stated precisely, the rule
is that nothing may take the transport lock *for the first time* while holding
the reader lock, or it deadlocks against ``data_received`` coming the other way.
``_maybe_resume_transport`` and ``_wait_for_data`` are written around it: both
decide under the reader lock and call ``resume_reading()`` outside it, because
both run in the reading task, which holds no transport lock of its own.

``feed_data`` looks like it breaks the rule and does not: it holds the reader lock
across ``super().feed_data()``, which calls ``transport.pause_reading()`` once the
buffer passes ``2 * limit``. But every caller of ``feed_data``/``feed_eof``/
``set_exception`` arrives from a protocol callback, entered with the transport's
RLock already held *by this thread*, so the acquisition is re-entrant and closes
no cycle (measured: 31,070 of them under a flow-control-saturating load, every
one re-entrant). Feeding a transport-backed reader from outside a protocol
callback would make it a real inversion.
"""

from __future__ import annotations

import threading
from asyncio.streams import (
    StreamReader as _StreamReader,
    StreamReaderProtocol as _StreamReaderProtocol,
    StreamWriter as _StreamWriter,
)

from ._sleep import sleep


class StreamReader(_StreamReader):
    """``asyncio.StreamReader`` with a race-free feed/wait handoff."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._lock = threading.Lock()
        # bumped by every feed; compared by _wait_for_data to decide whether
        # anything has arrived since it last looked
        self._feed_gen = 0
        self._seen_gen = 0

    # -- fed from the transport ---------------------------------------------
    #
    # These run on whichever worker ran the transport's _read_ready, with the
    # transport lock held. Taking the reader lock here is the correct order.

    def feed_data(self, data):
        with self._lock:
            self._feed_gen += 1
            super().feed_data(data)

    def feed_eof(self):
        with self._lock:
            self._feed_gen += 1
            super().feed_eof()

    def set_exception(self, exc):
        with self._lock:
            self._feed_gen += 1
            super().set_exception(exc)

    def _maybe_resume_transport(self):
        # NOT under the lock while calling the transport: this runs in the
        # reading task, and reader -> transport is the reverse of the order
        # data_received takes. Decide under the lock, act outside it.
        with self._lock:
            if not (self._paused and len(self._buffer) <= self._limit):
                return
            self._paused = False
        self._transport.resume_reading()

    # -- awaited by the reading task ----------------------------------------

    async def _wait_for_data(self, func_name):
        if self._waiter is not None:
            raise RuntimeError(f'{func_name}() called while another coroutine is already waiting for incoming data')

        # waiting while paused deadlocks readexactly(n) for n > limit. Done
        # before the lock is taken, for the reason in _maybe_resume_transport.
        if self._paused:
            self._paused = False
            self._transport.resume_reading()

        with self._lock:
            if self._feed_gen != self._seen_gen:
                # Something arrived since we last looked. Catch up, but hand
                # control back only if there is something to see: `read(n)` tests
                #
                #     if not self._buffer and not self._eof:
                #         await self._wait_for_data('read')
                #     data = bytes(self._buffer[:n])       # empty -> b'' -> EOF
                #
                # once, so returning with an empty buffer has it report a closed
                # connection that is still open -- and a server that believes
                # that closes a live socket under its peer. The generation runs
                # ahead of an empty buffer whenever a read consumed the bytes
                # without parking (nothing but this function advances
                # `_seen_gen`), which is the normal case once feed_data lands on
                # another worker while this task is busy.
                self._seen_gen = self._feed_gen
                if self._buffer or self._eof or self._exception is not None:
                    return
            if self._eof or self._exception is not None:
                return
            waiter = self._waiter = self._loop.create_future()

        try:
            await waiter
        finally:
            self._waiter = None
            with self._lock:
                self._seen_gen = self._feed_gen


class StreamReaderProtocol(_StreamReaderProtocol):
    """``StreamReaderProtocol`` with locked write-flow control.

    ``_drain_helper`` has the same check-then-park shape as the reader: it tests
    ``_paused``, then registers a waiter. ``resume_writing`` arrives from the
    transport on another worker and can land in between, draining a waiter list
    the writer has not joined yet.
    """

    def __init__(self, stream_reader, client_connected_cb=None, loop=None):
        # RLock: connection_lost runs the whole stdlib chain under it, and that
        # chain re-enters flow-control bookkeeping
        self._flow_lock = threading.RLock()
        super().__init__(stream_reader, client_connected_cb, loop=loop)

    def pause_writing(self):
        with self._flow_lock:
            super().pause_writing()

    def resume_writing(self):
        with self._flow_lock:
            super().resume_writing()

    def connection_lost(self, exc):
        with self._flow_lock:
            super().connection_lost(exc)

    async def _drain_helper(self):
        with self._flow_lock:
            if self._connection_lost:
                raise ConnectionResetError('Connection lost')
            if not self._paused:
                return
            waiter = self._loop.create_future()
            self._drain_waiters.append(waiter)
        try:
            await waiter
        finally:
            with self._flow_lock:
                try:
                    self._drain_waiters.remove(waiter)
                except ValueError:
                    pass


class StreamWriter(_StreamWriter):
    """``asyncio.StreamWriter`` whose ``drain`` does not await stdlib's sleep."""

    async def drain(self):
        if self._reader is not None:
            exc = self._reader.exception()
            if exc is not None:
                raise exc
        if self._transport.is_closing():
            # let connection_lost() run, so a write/drain loop on a closed
            # socket reports the error instead of spinning
            await sleep(0)
        await self._protocol._drain_helper()
