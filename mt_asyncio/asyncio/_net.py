"""``create_connection`` / ``create_server``: mostly CPython's, borrowed.

These methods look loop-specific and are not. Every one of them reaches the loop
only through methods we already implement::

    create_connection            _connect_sock, _create_connection_transport,
                                 _ensure_resolved, _debug
    _connect_sock                sock_connect
    _create_connection_transport _make_socket_transport, create_future
    _start_serving               _add_reader
    _accept_connection           create_task, call_later, call_exception_handler,
                                 _remove_reader
    _accept_connection2          _make_socket_transport, create_future

So they are bound into this mixin as-is rather than retyped. Two are not, and
both for the same reason -- they reach into ``asyncio.base_events``'s module
globals for ``tasks.gather``/``tasks.sleep``, which resolve to stdlib coroutines
our driver cannot step:

* ``create_server`` -- reimplemented below, so that it works with or without
  :mod:`mt_asyncio.asyncio.compat` installed. (Under compat those names are
  shadowed and the borrowed one would work too; our own API must not depend on
  the user having installed a global patch.)
* ``_stop_serving`` -- three lines upstream, and we must also release the
  reactor registration.

Happy eyeballs is the one deliberate feature gap: ``staggered_race`` calls
``futures.future_add_to_awaited_by``, a ``_asyncio`` C function that only accepts
genuine CPython futures. ``happy_eyeballs_delay`` is therefore ignored and
connection attempts are made sequentially -- a performance difference, not a
behavioural one.
"""

from __future__ import annotations

import socket
import threading
from asyncio.base_events import BaseEventLoop as _Base, Server as _Server, _set_reuseport
from asyncio.selector_events import BaseSelectorEventLoop as _Sel

from ._transports import SocketTransport


class Server(_Server):
    """``asyncio.base_events.Server``, driveable and serialised.

    Two changes. ``start_serving`` must not await stdlib's ``sleep(0)`` (it
    yields a bare ``None``, which our driver rejects), and the client bookkeeping
    needs a lock: ``_detach`` runs on whichever worker tore the connection down,
    and it races ``wait_closed``'s check-then-append::

        if self._waiters is None:      # not None yet
            return
                                       # _detach -> _wakeup -> _waiters = None
        self._waiters.append(waiter)   # AttributeError
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # RLock: close() and _detach() both reach _wakeup() while holding it
        self._mt_lock = threading.RLock()

    async def start_serving(self):
        from ._ctl import sleep

        self._start_serving()
        await sleep(0)

    def _attach(self, transport):
        with self._mt_lock:
            super()._attach(transport)

    def _detach(self, transport):
        with self._mt_lock:
            super()._detach(transport)

    def _wakeup(self):
        with self._mt_lock:
            super()._wakeup()

    def close(self):
        with self._mt_lock:
            super().close()

    async def wait_closed(self):
        with self._mt_lock:
            if self._waiters is None:
                return
            waiter = self._loop.create_future()
            self._waiters.append(waiter)
        await waiter


class NetworkMixin:
    """Transport-level networking, mixed into :class:`~._loop.EventLoop`."""

    # -- borrowed from CPython, unmodified ----------------------------------

    _ensure_resolved = _Base._ensure_resolved
    _create_server_getaddrinfo = _Base._create_server_getaddrinfo
    _connect_sock = _Base._connect_sock
    _create_connection_transport = _Base._create_connection_transport
    _start_serving = _Sel._start_serving
    _accept_connection = _Sel._accept_connection
    _accept_connection2 = _Sel._accept_connection2

    _std_create_connection = _Base.create_connection

    # -- transport factories ------------------------------------------------

    def _make_socket_transport(self, sock, protocol, waiter=None, *, extra=None, server=None):
        return SocketTransport(self, sock, protocol, waiter, extra, server)

    def _make_ssl_transport(
        self,
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
        from ._tls import make_ssl_transport

        return make_ssl_transport(
            self,
            rawsock,
            protocol,
            sslcontext,
            waiter,
            server_side=server_side,
            server_hostname=server_hostname,
            extra=extra,
            server=server,
            ssl_handshake_timeout=ssl_handshake_timeout,
            ssl_shutdown_timeout=ssl_shutdown_timeout,
        )

    # -- outbound -----------------------------------------------------------

    async def create_connection(self, protocol_factory, host=None, port=None, **kwargs):
        # see the module docstring: staggered_race needs CPython futures
        kwargs.pop('happy_eyeballs_delay', None)
        return await self._std_create_connection(protocol_factory, host, port, **kwargs)

    # -- inbound ------------------------------------------------------------

    async def create_server(
        self,
        protocol_factory,
        host=None,
        port=None,
        *,
        family=socket.AF_UNSPEC,
        flags=socket.AI_PASSIVE,
        sock=None,
        backlog=100,
        ssl=None,
        reuse_address=None,
        reuse_port=None,
        keep_alive=None,
        ssl_handshake_timeout=None,
        ssl_shutdown_timeout=None,
        start_serving=True,
    ):
        from ._ctl import sleep

        if isinstance(ssl, bool):
            raise TypeError('ssl argument must be an SSLContext or None')
        if ssl_handshake_timeout is not None and ssl is None:
            raise ValueError('ssl_handshake_timeout is only meaningful with ssl')
        if ssl_shutdown_timeout is not None and ssl is None:
            raise ValueError('ssl_shutdown_timeout is only meaningful with ssl')

        if host is not None or port is not None:
            if sock is not None:
                raise ValueError('host/port and sock can not be specified at the same time')
            if reuse_address is None:
                reuse_address = hasattr(socket, 'SO_REUSEADDR')
            hosts = [None] if host in ('', None) else [host] if isinstance(host, str) else list(host)

            # upstream resolves the hosts concurrently with tasks.gather; doing
            # it in sequence keeps this working without compat installed, and a
            # server binds a handful of names at most
            infos = set()
            for hst in hosts:
                infos.update(await self._create_server_getaddrinfo(hst, port, family=family, flags=flags))

            sockets = []
            completed = False
            try:
                for af, stype, sproto, _cname, sa in infos:
                    try:
                        sk = socket.socket(af, stype, sproto)
                    except OSError:
                        # an address family the kernel will not give us; the
                        # remaining ones may still work
                        continue
                    sockets.append(sk)
                    if reuse_address:
                        sk.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
                    if reuse_port:
                        _set_reuseport(sk)
                    if keep_alive:
                        sk.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
                    if af == getattr(socket, 'AF_INET6', None) and hasattr(socket, 'IPPROTO_IPV6'):
                        sk.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, True)
                    try:
                        sk.bind(sa)
                    except OSError as err:
                        raise OSError(
                            err.errno, f'error while attempting to bind on address {sa!r}: {err.strerror.lower()}'
                        ) from None
                completed = True
            finally:
                if not completed:
                    for sk in sockets:
                        sk.close()
        else:
            if sock is None:
                raise ValueError('Neither host/port nor sock were specified')
            if sock.type != socket.SOCK_STREAM:
                raise ValueError(f'A Stream Socket was expected, got {sock!r}')
            sockets = [sock]

        for sk in sockets:
            sk.setblocking(False)

        server = Server(self, sockets, protocol_factory, ssl, backlog, ssl_handshake_timeout, ssl_shutdown_timeout)
        if start_serving:
            server._start_serving()
            await sleep(0)
        return server

    def _stop_serving(self, sock):
        self._remove_reader(sock.fileno())
        self._release_fd(sock.fileno())
        sock.close()

    # -- TLS upgrade --------------------------------------------------------

    async def start_tls(
        self,
        transport,
        protocol,
        sslcontext,
        *,
        server_side=False,
        server_hostname=None,
        ssl_handshake_timeout=None,
        ssl_shutdown_timeout=None,
    ):
        from ._tls import start_tls

        return await start_tls(
            self,
            transport,
            protocol,
            sslcontext,
            server_side=server_side,
            server_hostname=server_hostname,
            ssl_handshake_timeout=ssl_handshake_timeout,
            ssl_shutdown_timeout=ssl_shutdown_timeout,
        )


async def open_connection(host=None, port=None, *, limit=2**16, **kwds):
    from ._streams import StreamReader, StreamReaderProtocol, StreamWriter
    from ._tasks import get_running_loop

    loop = get_running_loop()
    reader = StreamReader(limit=limit, loop=loop)
    protocol = StreamReaderProtocol(reader, loop=loop)
    transport, _ = await loop.create_connection(lambda: protocol, host, port, **kwds)
    writer = StreamWriter(transport, protocol, reader, loop)
    return reader, writer


async def start_server(client_connected_cb, host=None, port=None, *, limit=2**16, **kwds):
    from ._streams import StreamReader, StreamReaderProtocol
    from ._tasks import get_running_loop

    loop = get_running_loop()

    def factory():
        reader = StreamReader(limit=limit, loop=loop)
        return StreamReaderProtocol(reader, client_connected_cb, loop=loop)

    return await loop.create_server(factory, host, port, **kwds)
