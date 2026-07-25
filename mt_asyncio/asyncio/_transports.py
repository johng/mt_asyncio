"""Socket transports: CPython's, serialised per connection.

CPython's ``_SelectorSocketTransport`` is a good transport. What it is not is
thread-safe, because it never had to be: a selector loop runs every callback on
one thread, so ``write()`` can never overlap ``_write_ready()``. We removed that
guarantee, so we have to put it back.

The failure is not theoretical and it is not the one people expect. Free-threaded
Python does make the container operations atomic -- a ``deque`` shared by four
threads loses nothing. What it does not do is make a *sequence* atomic, and
``_write_send`` is a sequence::

    chunk = buffer.popleft()
    n = sock.send(chunk)          # partial
    buffer.appendleft(chunk[n:])  # <- a concurrent write() appended in here

Measured on 3.14t: ~1000 out-of-order splices per 128KB, with the socket itself
under a lock. The bytes on the wire are simply in the wrong order. Same shape as
``gather``'s ``nfinished += 1``, one layer down.

So: **one RLock per connection, held at every entry point.** Callbacks entered
from the reactor take it (``_read_ready``, ``_write_ready``), app-facing methods
take it (``write``, ``close``, ...), and teardown takes it. Reentrancy is real --
``writelines`` calls ``_write_ready()`` directly and ``_maybe_resume_protocol``
calls ``protocol.resume_writing()``, which may call ``write()`` -- hence RLock.

The consequence, stated plainly: **this parallelises connections, not the inside
of one connection.** A single connection's protocol callbacks are serialised,
which is exactly asyncio's contract; the parallelism comes from having many.
"""

from __future__ import annotations

import inspect
import threading
from asyncio.selector_events import _SelectorSocketTransport, _SelectorTransport


try:  # pragma: no cover - platform dependent
    from asyncio.selector_events import _HAS_SENDMSG
except ImportError:  # pragma: no cover
    _HAS_SENDMSG = False

from asyncio.base_events import _set_nodelay


# CPython 3.15 gives transports a `context`: the contextvars.Context the
# connection was opened in, remembered so that every reactor callback the
# transport schedules runs inside it. 3.14 has no such parameter and no such
# plumbing, and we support both, so the pass-through is conditional. Detected
# rather than version-gated -- the constructor is what we actually call.
_HAS_TRANSPORT_CONTEXT = 'context' in inspect.signature(_SelectorTransport.__init__).parameters


class SocketTransport(_SelectorSocketTransport):
    """``_SelectorSocketTransport`` with a per-connection lock.

    Everything below is either a lock acquisition around inherited behaviour or
    a place where CPython's implementation assumes a single-threaded loop and we
    have to differ. Those are marked.
    """

    def __init__(self, loop, sock, protocol, waiter=None, extra=None, server=None, lock=None, context=None):
        # a TLS connection passes the lock in, so the raw transport, SSLProtocol
        # and the app-facing transport are all serialised by the same one
        self._lock = lock if lock is not None else threading.RLock()
        # `_SelectorSocketTransport.__init__` ends with three separate
        # `call_soon`s -- connection_made, then _add_reader, then the waiter --
        # and relies on them running in that order. Our call_soon has no FIFO
        # guarantee (handles go to a work-stealing injector and two can run at
        # once), so a naive super().__init__ could start reading before
        # connection_made returned. We run the grandparent and sequence the
        # startup ourselves in `_startup`.
        self._read_ready_cb = None
        if _HAS_TRANSPORT_CONTEXT:
            # sets self._context, which the inherited _add_reader/_add_writer/
            # _call_soon helpers then thread through to the loop
            _SelectorTransport.__init__(self, loop, sock, protocol, extra, server, context)
        else:
            _SelectorTransport.__init__(self, loop, sock, protocol, extra, server)
        self._eof = False
        self._empty_waiter = None
        self._write_impl = self._write_sendmsg if _HAS_SENDMSG else self._write_send
        self._write_ready = self._locked_write_ready
        _set_nodelay(self._sock)
        # the 3.15 equivalent of the three call_soons is self._call_soon, which
        # is exactly this with the context attached; passing it explicitly keeps
        # one code path across both versions (3.14 ignores it and copies the
        # current context, as it did before)
        loop.call_soon(self._startup, waiter, context=context)

    # -- startup ------------------------------------------------------------

    def _startup(self, waiter):
        with self._lock:
            self._protocol.connection_made(self)
            # only start reading once connection_made has returned
            if not self._closing:
                self._add_reader(self._sock_fd, self._read_ready)
        if waiter is not None and not waiter.cancelled():
            waiter.set_result(None)

    # -- reactor-entered callbacks ------------------------------------------

    def _locked_write_ready(self):
        with self._lock:
            impl = self._write_impl
            if impl is not None:
                impl()

    def _read_ready(self):
        with self._lock:
            cb = self._read_ready_cb
            if cb is not None:
                cb()

    # -- app-facing surface -------------------------------------------------

    def write(self, data):
        with self._lock:
            super().write(data)

    def writelines(self, list_of_data):
        with self._lock:
            super().writelines(list_of_data)

    def write_eof(self):
        with self._lock:
            super().write_eof()

    def close(self):
        with self._lock:
            super().close()

    def abort(self):
        with self._lock:
            super().abort()

    def pause_reading(self):
        with self._lock:
            super().pause_reading()

    def resume_reading(self):
        with self._lock:
            super().resume_reading()

    def set_protocol(self, protocol):
        with self._lock:
            super().set_protocol(protocol)

    def get_write_buffer_size(self):
        with self._lock:
            return super().get_write_buffer_size()

    def set_write_buffer_limits(self, high=None, low=None):
        with self._lock:
            super().set_write_buffer_limits(high=high, low=low)

    # -- teardown -----------------------------------------------------------

    def _fatal_error(self, exc, message='Fatal error on transport'):
        with self._lock:
            super()._fatal_error(exc, message)

    def _force_close(self, exc):
        with self._lock:
            super()._force_close(exc)

    def _call_connection_lost(self, exc):
        with self._lock:
            loop, fd = self._loop, self._sock_fd
            # `_write_ready = None` upstream; ours is the wrapper, so drop the
            # implementation it dispatches to as well
            self._write_impl = None
            # release the reactor registration before the socket is closed: the
            # kernel is free to reuse this fd immediately afterwards, and a
            # registration left behind would be handed to the next socket
            if loop is not None:
                loop._release_fd(fd)
            super()._call_connection_lost(exc)

    def _make_empty_waiter(self):
        with self._lock:
            return super()._make_empty_waiter()

    def _reset_empty_waiter(self):
        with self._lock:
            super()._reset_empty_waiter()
