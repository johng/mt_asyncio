"""TLS: CPython's ``sslproto``, under the connection's lock.

This is where reuse pays for itself. ``asyncio.sslproto`` is 929 lines of
MemoryBIO state machine, and it is the one layer that needs *nothing*
loop-model-specific -- its entire dependency on the loop is ``call_soon``,
``call_later``, ``call_exception_handler``, ``get_debug`` and ``time``, all of
which we have. Reimplementing it would also mean choosing a TLS library, and any
choice other than ``ssl`` breaks the ``ssl.SSLContext`` objects that callers
(asyncpg, aiohttp) hand us. So we keep it.

What it does need is serialisation. ``SSLProtocol`` carries ~40 mutable fields
and a hand-rolled state machine, entered from three directions:

1. the raw transport's callbacks (``data_received``, ``connection_lost``, ...)
2. the app, through ``_SSLProtocolTransport`` (``write``, ``close``, ...)
3. its own handshake/shutdown timers

**One lock covers all three**, because it is the same lock the raw
:class:`~._transports.SocketTransport` holds. Direction 1 is then already inside
it; this module adds it to directions 2 and 3.
"""

from __future__ import annotations

import threading
from asyncio.sslproto import SSLProtocol as _SSLProtocol, _SSLProtocolTransport

from ._transports import SocketTransport


class SSLProtocolTransport(_SSLProtocolTransport):
    """The app-facing side of a TLS connection, under the connection lock."""

    def __init__(self, loop, ssl_protocol, lock):
        self._lock = lock
        super().__init__(loop, ssl_protocol)

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

    def set_read_buffer_limits(self, high=None, low=None):
        with self._lock:
            super().set_read_buffer_limits(high=high, low=low)

    def _force_close(self, exc):
        with self._lock:
            super()._force_close(exc)


class SSLProtocol(_SSLProtocol):
    """``asyncio.sslproto.SSLProtocol`` sharing the raw transport's lock."""

    def __init__(self, loop, app_protocol, sslcontext, waiter, *args, lock=None, **kwargs):
        self._lock = lock if lock is not None else threading.RLock()
        super().__init__(loop, app_protocol, sslcontext, waiter, *args, **kwargs)

    def _get_app_transport(self):
        if self._app_transport is None:
            if self._app_transport_created:
                raise RuntimeError('Creating _SSLProtocolTransport twice')
            self._app_transport = SSLProtocolTransport(self._loop, self, self._lock)
            self._app_transport_created = True
        return self._app_transport

    # -- timer callbacks: the one path not already inside the lock ----------

    def _check_handshake_timeout(self):
        with self._lock:
            super()._check_handshake_timeout()

    def _check_shutdown_timeout(self):
        with self._lock:
            super()._check_shutdown_timeout()

    def _on_handshake_complete(self, handshake_exc):
        with self._lock:
            super()._on_handshake_complete(handshake_exc)

    def _on_shutdown_complete(self, shutdown_exc):
        with self._lock:
            super()._on_shutdown_complete(shutdown_exc)


def make_ssl_transport(
    loop,
    rawsock,
    protocol,
    sslcontext,
    waiter=None,
    *,
    server_side=False,
    server_hostname=None,
    extra=None,
    server=None,
    ssl_handshake_timeout=None,
    ssl_shutdown_timeout=None,
):
    lock = threading.RLock()
    ssl_protocol = SSLProtocol(
        loop,
        protocol,
        sslcontext,
        waiter,
        server_side,
        server_hostname,
        ssl_handshake_timeout=ssl_handshake_timeout,
        ssl_shutdown_timeout=ssl_shutdown_timeout,
        lock=lock,
    )
    # the raw transport shares the lock, so every callback it makes into
    # SSLProtocol is already serialised against the app-facing side
    SocketTransport(loop, rawsock, ssl_protocol, None, extra, server, lock=lock)
    return ssl_protocol._app_transport


async def start_tls(
    loop,
    transport,
    protocol,
    sslcontext,
    *,
    server_side=False,
    server_hostname=None,
    ssl_handshake_timeout=None,
    ssl_shutdown_timeout=None,
):
    if not getattr(transport, '_start_tls_compatible', False):
        raise TypeError(f'transport {transport!r} is not supported by start_tls()')

    waiter = loop.create_future()
    lock = getattr(transport, '_lock', None) or threading.RLock()
    ssl_protocol = SSLProtocol(
        loop,
        protocol,
        sslcontext,
        waiter,
        server_side,
        server_hostname,
        ssl_handshake_timeout=ssl_handshake_timeout,
        ssl_shutdown_timeout=ssl_shutdown_timeout,
        call_connection_made=False,
        lock=lock,
    )

    transport.set_protocol(ssl_protocol)

    def _begin():
        # upstream schedules connection_made and resume_reading as two separate
        # call_soons and relies on their order. Ours has no FIFO guarantee, so
        # they go in one handle.
        ssl_protocol.connection_made(transport)
        transport.resume_reading()

    handle = loop.call_soon(_begin)
    try:
        await waiter
    except BaseException:
        transport.close()
        handle.cancel()
        raise

    return ssl_protocol._get_app_transport()
