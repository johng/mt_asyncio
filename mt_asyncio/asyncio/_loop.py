"""The single multi-threaded event loop.

There is no pump thread: the mt_asyncio runtime *is* the scheduler. Tasks step in
true parallel across worker threads; timers fire from the reactor's poll thread;
``call_soon`` schedules a callback onto a worker (thread-safe, so
``call_soon`` == ``call_soon_threadsafe`` -- no self-pipe, no ``_check_thread``).
``run_until_complete`` merely parks the calling thread until the target Future
completes, leaving the runtime hot (so pending tasks survive across calls).

Consequence of true parallelism: callbacks/done-callbacks run on worker threads,
possibly concurrently -- asyncio's "callbacks are serialized on the loop thread"
contract does not hold. Shared state touched from callbacks needs the provided
(genuinely cross-thread) locks.
"""

from __future__ import annotations

import asyncio as _aio
import concurrent.futures as _cf
import contextvars
import errno
import functools
import select
import selectors
import socket
import threading
import time as _time
import traceback
import weakref
from asyncio.selector_events import BaseSelectorEventLoop as _Sel
from selectors import _fileobj_to_fd

from .. import io as _tio
from .._mt_asyncio import CancelledError as _MtAsyncioCancelled, Event as _Event, get_runtime
from . import _net
from ._futures import Future
from ._reactor import reactor as _reactor
from ._tasks import Task, _running_loop, ensure_future
from ._wakes import pending_wakes


class Handle:
    """asyncio.Handle-compatible: a scheduled callback that can be cancelled."""

    __slots__ = ('_cb', '_args', '_context', '_cancelled')

    def __init__(self, cb, args, context):
        self._cb = cb
        self._args = args
        self._context = context
        self._cancelled = False

    def cancel(self):
        self._cancelled = True
        self._cb = None
        self._args = None

    def cancelled(self):
        return self._cancelled

    def _run(self):
        # snapshot before use: cancel() runs on other threads and nulls both
        # fields, so testing _cancelled and then reading them is a race that
        # ends in `TypeError: Value after * must be an iterable, not NoneType`
        cb, args = self._cb, self._args
        if self._cancelled or cb is None or args is None:
            return
        try:
            self._context.run(cb, *args)
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException:
            traceback.print_exc()


class TimerHandle(Handle):
    __slots__ = ('_when',)

    def __init__(self, when, cb, args, context):
        super().__init__(cb, args, context)
        self._when = when

    def when(self):
        return self._when


_POLL_IN = select.POLLIN | select.POLLPRI
_POLL_OUT = select.POLLOUT


#: file object or raw fd -> int. Borrowed: it is what ``selectors`` does to
#  everything, and so what the ``_Sel`` methods below assume has happened.
_fileno = _fileobj_to_fd


def _fd_ready(fd, mask):
    """Ask the kernel whether `fd` is ready *right now*.

    The reactor is edge-triggered and caches readiness in userspace, so a set
    bit only means "ready as of the last edge we saw". An ``add_reader``
    callback does its own syscalls, and a drain we never observe leaves the bit
    stale forever. ``poll(2)`` with a zero timeout is the only authority on the
    current level.

    Any revents at all counts as ready: POLLERR/POLLHUP/POLLNVAL are reported
    whether or not they were requested, and a selector loop would hand those to
    the callback too (recv returns EOF, send raises EPIPE -- the callback's job).
    """
    poller = select.poll()
    poller.register(fd, mask)
    return bool(poller.poll(0))


class _SelectorShim:
    """Just enough of a ``selectors.BaseSelector`` for stdlib's transport repr.

    ``_SelectorTransport.__repr__`` asks the loop's selector whether an fd is
    currently polled for read/write. We have no selector -- readiness lives in
    the reactor -- so the fd registry answers instead.
    """

    __slots__ = ('_loop',)

    def __init__(self, loop):
        self._loop = loop

    def get_key(self, fileobj):
        fd = _fileno(fileobj)
        reg = self._loop._sios.get(fd)
        events = 0
        if reg is not None:
            if reg.reader is not None:
                events |= selectors.EVENT_READ
            if reg.writer is not None:
                events |= selectors.EVENT_WRITE
        if not events:
            raise KeyError(fileobj)
        return selectors.SelectorKey(fileobj, fd, events, None)


class _FdIO:
    """Reactor registration for one fd.

    Shared by the ``sock_*`` helpers and ``add_reader``/``add_writer``: mio
    registers an fd once, for both directions, for its whole lifetime, so a
    second registration of the same fd would either fail (epoll: EEXIST) or
    silently steal the first one's events (kqueue overwrites udata). One
    registration per fd, therefore, no matter who asked for it.
    """

    __slots__ = ('sio', 'reader', 'writer', 'pinned', 'owner')

    def __init__(self, sio):
        self.sio = sio
        self.reader = None
        self.writer = None
        # set by the sock_* helpers, which have no removal point: those keep the
        # registration alive until the loop closes. add_reader/add_writer do have
        # one, and drop it, so an fd that is closed and recycled gets a fresh
        # registration rather than a dead one.
        self.pinned = False
        # weakref to the socket a pinned registration belongs to, so a recycled
        # fd can be told apart from the same socket coming back (see `_sio`)
        self.owner = None


class _FdHandle(Handle):
    """A persistent ``add_reader``/``add_writer`` registration.

    asyncio's contract is level-triggered and persistent: the callback fires on
    every loop iteration the fd is ready, until it is removed. Ours is neither
    -- ``arm_r_cb`` is one-shot and fires on an *edge*. Persistence is emulated
    by re-arming after each dispatch, and the level is recovered by asking the
    kernel (see :func:`_fd_ready`) whenever the cached bit claims readiness.

    That is not a psycopg detail, it is what makes any conventional consumer
    work: a reader that does one ``recv()`` per callback needs to be called
    again while data remains, and no further edge is coming.

    Dispatches for one handle are strictly serial: the re-arm happens *after*
    the callback returns, never before, so a consumer never sees two of its own
    callbacks at once and reads stay ordered.
    """

    __slots__ = ('_fd', '_sio', '_writer', '_active')

    def __init__(self, fd, sio, writer, cb, args, context):
        super().__init__(cb, args, context)
        self._fd = fd
        self._sio = sio
        self._writer = writer
        self._active = True

    def cancel(self):
        self._active = False
        super().cancel()

    #: retries before giving up on reconciling the cached bit with the kernel.
    #  Unbounded is not safe: `arm_*_cb` also reports ready for the SHUTDOWN bit,
    #  which `clear_*` cannot clear (it only clears the readiness bits), so an
    #  fd whose registration was torn down under us would spin here forever --
    #  burning a worker thread each, and deadlocking the runtime once as many
    #  fds are waiting as there are workers.
    _ARM_RETRIES = 8

    def _arm(self):
        sio, writer = self._sio, self._writer
        arm = sio.arm_w_cb if writer else sio.arm_r_cb
        clear = sio.clear_w if writer else sio.clear_r
        mask = _POLL_OUT if writer else _POLL_IN
        attempts = 0
        while self._active:
            if not arm(self._wake):
                return  # parked: the poll thread calls _wake on the next edge
            # the cached bit says ready, but it may predate a drain we never saw
            if _fd_ready(self._fd, mask):
                self._dispatch_soon()
                return
            attempts += 1
            if attempts >= self._ARM_RETRIES:
                # the cache insists it is ready and the kernel disagrees, so the
                # registration is gone (or the fd is). Hand the callback one
                # dispatch so the consumer's own syscall reports the error, the
                # way a selector loop would, rather than spinning.
                self._dispatch_soon()
                return
            # stale. Clearing is tick-guarded on the Rust side: it is a no-op if
            # a fresh edge landed since arm() recorded the tick, and we go round
            # again to observe it.
            clear()

    def _wake(self):
        # on the reactor poll thread, GIL held. Hand straight off to a worker:
        # an arbitrary user callback must never run here or it stalls all I/O.
        if self._active:
            self._dispatch_soon()

    def _dispatch_soon(self):
        """Queue one dispatch, on the *global* run queue rather than this worker's.

        The distinction is load-bearing, and the reason is worth spelling out.

        Emulating level-triggered readiness means re-arming after every dispatch
        (see the class docstring). When the callback consumes what it was woken
        for, that terminates: the fd stops being ready and the re-arm parks. But
        an ``add_reader`` callback is not obliged to consume anything. psycopg's
        does not -- its ``wakeup`` only sets an ``Event``, and libpq reads the
        socket later, from the task that was waiting on it. Between the callback
        returning and the task getting a turn, the fd is still readable, so the
        re-arm finds it ready and dispatches again. That is correct behaviour,
        and CPython's selector loop does the same thing; there, the repeat is
        naturally rate-limited because ready-callbacks and task steps are
        interleaved on one thread, so the consumer always gets its turn.

        We have no such interleaving, and ``Runtime::add_handle`` pushes onto the
        calling worker's local deque while ``find_work`` pops that deque *before*
        looking at the injector. So a self-rescheduling handle is popped straight
        back off by the same worker, forever: a closed loop that never reaches
        the injector, which is exactly where the wake handle for the consuming
        task is sitting. One such fd pins one worker. As many such fds as there
        are workers, and nothing else in the process runs again -- the symptom
        being a hang with every worker at 100% and the consumer never scheduled.

        ``_call_soon_deferred`` pushes to the injector instead, so each
        re-dispatch queues behind whatever work is already pending and the
        consumer is reached. The cost is losing local-queue locality on a path
        that is already one syscall deep, which is not a trade worth defending.
        """
        get_runtime()._call_soon_deferred(self._dispatch)

    def _dispatch(self):
        if not self._active:
            return
        self._run()
        self._arm()


class _MtAsyncioExecutor(_cf.Executor):
    """concurrent.futures.Executor backed by mt_asyncio's blocking thread-pool."""

    __slots__ = ()

    def submit(self, fn, /, *args, **kwargs):
        cfut: _cf.Future = _cf.Future()
        if not cfut.set_running_or_notify_cancel():
            return cfut
        try:
            _ctl, event, res = get_runtime()._spawn_blocking(fn, *args, **kwargs)
        except BaseException as exc:  # surface submit failures on the future
            cfut.set_exception(exc)
            return cfut

        async def _drain():
            await event.waiter(None)
            err, val = res.fetch()
            if err is True:
                cfut.set_exception(val)
            elif err is False:
                cfut.set_result(val)
            else:
                cfut.set_exception(_MtAsyncioCancelled())

        get_runtime()._spawn_coro(_drain())
        return cfut


class EventLoop(_net.NetworkMixin, _aio.AbstractEventLoop):
    """A single asyncio-compatible event loop over mt_asyncio's parallel runtime.

    Subclasses ``asyncio.AbstractEventLoop`` so third-party ``isinstance`` checks
    pass under :mod:`mt_asyncio.asyncio.compat`. It is a plain class, not an ABC,
    so this costs nothing; the methods we still do not implement (datagram,
    subprocesses, sendfile) inherit its ``NotImplementedError`` stubs, which is
    the failure we want.

    ``NetworkMixin`` supplies ``create_connection``/``create_server``/``start_tls``
    and must precede ``AbstractEventLoop`` in the MRO so it wins over the stubs.
    """

    def __init__(self, threads: int | None = None):
        self._threads = threads
        self._reactor_acquired = False
        self._closed = False
        self._running = False
        self._stop_event: threading.Event | None = None
        self._debug = False
        self._exception_handler = None
        self._task_factory = None
        self._executor = _MtAsyncioExecutor()
        self._tasks: set = set()
        self._tasks_lock = threading.Lock()
        # fd -> _FdIO. Guarded, because two tasks racing on the same socket must
        # not each create a registration for it.
        self._sios: dict = {}
        self._sios_lock = threading.Lock()
        self._transports: weakref.WeakValueDictionary = weakref.WeakValueDictionary()
        # stdlib transports introspect loop._selector in their __repr__
        self._selector = _SelectorShim(self)

    # -- factories ----------------------------------------------------------

    def create_future(self):
        return Future(loop=self)

    def create_task(self, coro, *, name=None, context=None, **kwargs):
        self._check_closed()
        # tasks go straight onto the runtime, so it has to exist even when they
        # are scheduled before run_forever/run_until_complete
        self._ensure_reactor()
        if self._task_factory is not None:
            return self._task_factory(self, coro, context=context, name=name, **kwargs)
        return Task(coro, loop=self, name=name, context=context, **kwargs)

    def get_task_factory(self):
        return self._task_factory

    def set_task_factory(self, factory):
        if factory is not None and not callable(factory):
            raise TypeError('task factory must be a callable or None')
        self._task_factory = factory

    # -- task registry ------------------------------------------------------

    def _register_task(self, task):
        with self._tasks_lock:
            self._tasks.add(task)

    def _unregister_task(self, task):
        with self._tasks_lock:
            self._tasks.discard(task)

    def _all_tasks(self):
        with self._tasks_lock:
            return {t for t in self._tasks if not t.done()}

    # -- clock --------------------------------------------------------------

    def time(self):
        return _time.monotonic()

    # -- callbacks / timers -------------------------------------------------

    def call_soon(self, cb, *args, context=None):
        self._check_closed()
        self._ensure_reactor()
        h = Handle(cb, args, _handle_context(context))
        pending = pending_wakes()
        if pending is None:
            get_runtime()._call_soon(h._run)
        else:
            # inside a protocol callback: queueing the handle now would let
            # another worker run it before this callback returns, which is the
            # one thing asyncio's loop never does (see `._wakes`)
            pending.append(functools.partial(get_runtime()._call_soon, h._run))
        return h

    call_soon_threadsafe = call_soon

    def call_later(self, delay, cb, *args, context=None):
        return self.call_at(self.time() + delay, cb, *args, context=context)

    def call_at(self, when, cb, *args, context=None):
        self._check_closed()
        self._ensure_reactor()
        h = TimerHandle(when, cb, args, _handle_context(context))
        delay_us = max(0, round((when - self.time()) * 1_000_000))

        async def _timer():
            await _Event().waiter(delay_us)  # timer-only waiter; resumes when due
            h._run()

        get_runtime()._spawn_coro(_timer())
        return h

    # -- executor -----------------------------------------------------------

    def run_in_executor(self, executor, func, *args):
        self._check_closed()
        self._ensure_reactor()
        if executor is None:
            executor = self._executor
        cfut = executor.submit(func, *args)
        return _wrap_concurrent_future(cfut, self)

    def set_default_executor(self, executor):
        self._executor = executor

    def get_debug(self):
        return self._debug

    def set_debug(self, enabled):
        self._debug = bool(enabled)

    # -- exceptions ---------------------------------------------------------

    def set_exception_handler(self, handler):
        self._exception_handler = handler

    def get_exception_handler(self):
        return self._exception_handler

    def default_exception_handler(self, context):
        message = context.get('message') or 'Unhandled exception in event loop'
        exc = context.get('exception')
        print(f'{message}')
        if exc is not None:
            traceback.print_exception(type(exc), exc, exc.__traceback__)

    def call_exception_handler(self, context):
        if self._exception_handler is None:
            self.default_exception_handler(context)
            return
        try:
            self._exception_handler(self, context)
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException:
            traceback.print_exc()

    # -- lifecycle ----------------------------------------------------------

    def _ensure_reactor(self):
        if not self._reactor_acquired:
            _reactor.acquire(self._threads, context=True)
            self._reactor_acquired = True

    def is_running(self):
        return self._running

    def is_closed(self):
        return self._closed

    def _check_closed(self):
        if self._closed:
            raise RuntimeError('Event loop is closed')

    def run_until_complete(self, coro):
        self._check_closed()
        self._ensure_reactor()
        fut = ensure_future(coro, loop=self)
        done = threading.Event()
        fut.add_done_callback(lambda _f: done.set())
        self._running = True
        token = _running_loop.set(self)
        try:
            done.wait()
        finally:
            self._running = False
            _running_loop.reset(token)
        return fut.result()

    def run_forever(self):
        self._check_closed()
        self._ensure_reactor()
        self._stop_event = threading.Event()
        self._running = True
        token = _running_loop.set(self)
        try:
            self._stop_event.wait()
        finally:
            self._running = False
            _running_loop.reset(token)

    def stop(self):
        if self._stop_event is not None:
            self._stop_event.set()

    def close(self):
        if self._running:
            raise RuntimeError('Cannot close a running event loop')
        if self._closed:
            return
        self._closed = True
        if self._reactor_acquired:
            self._reactor_acquired = False
            _reactor.release()
        with self._sios_lock:
            for fd in list(self._sios):
                self._drop_registration(fd)

    async def shutdown_asyncgens(self):
        return None

    async def shutdown_default_executor(self, timeout=None):
        return None

    # -- low-level sockets (cancellable, on the mt_asyncio callback reactor) -----

    def _registration(self, fd):
        """Get or create the registration for `fd`. Call with `_sios_lock` held."""
        reg = self._sios.get(fd)
        if reg is None:
            reg = self._sios[fd] = _FdIO(_tio.register(fd))
        return reg

    def _drop_registration(self, fd):
        """Deregister and forget `fd`. Call with `_sios_lock` held.

        Both steps stay under the lock because deregistration is by fd *number*,
        not by registration object: releasing outside the lock lets another
        thread register the same fd in between and have its brand new
        registration torn down by ours.
        """
        reg = self._sios.pop(fd, None)
        if reg is None:
            return
        for handle in (reg.reader, reg.writer):
            if handle is not None:
                handle.cancel()
        try:
            reg.sio.close()
        except Exception:  # the fd may already be gone; nothing to release
            pass

    def _sio(self, sock):
        """Registration for a socket used by the ``sock_*`` helpers.

        File descriptors are recycled aggressively: close a socket and the very
        next one the process opens is likely to be handed the same number. The
        kernel drops a closed fd from the reactor with it, so the registration
        we cached is dead -- a reader parked on it waits for an edge that can
        never arrive. Comparing the owning socket object catches that.
        """
        with self._sios_lock:
            fd = sock.fileno()
            reg = self._sios.get(fd)
            if reg is not None and reg.owner is not None and reg.owner() is not sock:
                self._drop_registration(fd)
            reg = self._registration(fd)
            reg.owner = weakref.ref(sock)
            reg.pinned = True
            return reg.sio

    # -- add_reader/add_writer ----------------------------------------------

    # borrowed from CPython, unmodified: all four are "reject an fd a transport
    # owns, then delegate", and what they delegate to is ours
    _ensure_fd_no_transport = _Sel._ensure_fd_no_transport
    add_reader = _Sel.add_reader
    add_writer = _Sel.add_writer
    remove_reader = _Sel.remove_reader
    remove_writer = _Sel.remove_writer

    # `context` is keyword-only and 3.15-only at the call sites that pass it
    # (a transport forwards the context its connection was opened in), but
    # accepting it everywhere costs nothing and keeps one signature.
    def _add_reader(self, fd, callback, *args, context=None):
        self._add_fd_callback(fd, False, callback, args, context)

    def _add_writer(self, fd, callback, *args, context=None):
        self._add_fd_callback(fd, True, callback, args, context)

    def _remove_reader(self, fd):
        return self._remove_fd_callback(fd, False)

    def _remove_writer(self, fd):
        return self._remove_fd_callback(fd, True)

    def _add_fd_callback(self, fileobj, writer, callback, args, context=None):
        self._check_closed()
        self._ensure_reactor()
        fd = _fileno(fileobj)
        context = _handle_context(context)
        with self._sios_lock:
            reg = self._registration(fd)
            handle = _FdHandle(fd, reg.sio, writer, callback, args, context)
            if writer:
                reg.writer, old = handle, reg.writer
            else:
                reg.reader, old = handle, reg.reader
        # replacing a registration cancels the old one, as asyncio does; its arm
        # loop exits on the next check and the Rust slot is overwritten by ours
        if old is not None:
            old.cancel()
        handle._arm()

    def _remove_fd_callback(self, fileobj, writer):
        if self._closed:
            return False
        fd = _fileno(fileobj)
        with self._sios_lock:
            reg = self._sios.get(fd)
            if reg is None:
                return False
            handle = reg.writer if writer else reg.reader
            if handle is None:
                return False
            if writer:
                reg.writer = None
            else:
                reg.reader = None
            # cancel *before* dropping: _drop_registration shuts the
            # ScheduledIO down, and an _arm() still in flight on it would find
            # a readiness bit it can never clear
            handle.cancel()
            if reg.reader is None and reg.writer is None and not reg.pinned:
                self._drop_registration(fd)
        return True

    def _release_fd(self, fd):
        """Drop the reactor registration for `fd` outright.

        Transports own their fd and close it, so they release it here rather
        than leaving a registration pointing at a descriptor the kernel is free
        to hand to the next socket.
        """
        with self._sios_lock:
            self._drop_registration(_fileno(fd))

    async def _wait_readable(self, sio):
        fut = self.create_future()
        if sio.arm_r_cb(functools.partial(_set_result_unless_done, fut)):
            return True
        await fut
        return False

    async def _wait_writable(self, sio):
        fut = self.create_future()
        if sio.arm_w_cb(functools.partial(_set_result_unless_done, fut)):
            return True
        await fut
        return False

    async def sock_recv(self, sock, n):
        sock.setblocking(False)
        sio = self._sio(sock)
        while True:
            if not await self._wait_readable(sio):
                continue
            try:
                return sock.recv(n)
            except (BlockingIOError, InterruptedError):
                sio.clear_r()

    async def sock_recv_into(self, sock, buf):
        sock.setblocking(False)
        sio = self._sio(sock)
        while True:
            if not await self._wait_readable(sio):
                continue
            try:
                return sock.recv_into(buf)
            except (BlockingIOError, InterruptedError):
                sio.clear_r()

    async def sock_sendall(self, sock, data):
        sock.setblocking(False)
        sio = self._sio(sock)
        view = memoryview(data)
        sent = 0
        total = view.nbytes
        while sent < total:
            if not await self._wait_writable(sio):
                continue
            try:
                sent += sock.send(view[sent:])
            except (BlockingIOError, InterruptedError):
                sio.clear_w()

    async def sock_connect(self, sock, address):
        sock.setblocking(False)
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            infos = await self.getaddrinfo(*address[:2], family=sock.family, type=sock.type, proto=sock.proto)
            if not infos:
                raise OSError(f'getaddrinfo() returned empty list for {address!r}')
            address = infos[0][4]
        sio = self._sio(sock)
        try:
            sock.connect(address)
        except (BlockingIOError, InterruptedError):
            pass
        while True:
            if not await self._wait_writable(sio):
                continue
            err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err == 0:
                return
            # errno, not socket: the socket module re-exports EAGAIN/EWOULDBLOCK
            # but not EINPROGRESS, so reaching for it here turned every failed
            # connect into an AttributeError instead of the real OSError
            if err in (errno.EAGAIN, errno.EWOULDBLOCK, errno.EINPROGRESS):
                sio.clear_w()
                continue
            raise OSError(err, f'Connect call failed {address!r}')

    async def sock_accept(self, sock):
        sock.setblocking(False)
        sio = self._sio(sock)
        while True:
            if not await self._wait_readable(sio):
                continue
            try:
                conn, addr = sock.accept()
                conn.setblocking(False)
                return conn, addr
            except (BlockingIOError, InterruptedError):
                sio.clear_r()

    async def getaddrinfo(self, host, port, *, family=0, type=0, proto=0, flags=0):
        return await self.run_in_executor(
            None, functools.partial(socket.getaddrinfo, host, port, family, type, proto, flags)
        )

    async def getnameinfo(self, sockaddr, flags=0):
        return await self.run_in_executor(None, socket.getnameinfo, sockaddr, flags)


def _handle_context(context):
    """The context a single scheduled callback will run in.

    asyncio lets a caller pin a callback to a particular ``contextvars.Context``,
    and CPython 3.15 leans on that internally: a transport now remembers the
    context its connection was created in and hands it to every ``call_soon`` and
    ``_add_reader`` it makes, so protocol callbacks see the contextvars the
    opener set.

    A ``Context`` may only be entered by one thread at a time -- ``Context.run``
    on one another thread is already inside raises ``RuntimeError: cannot enter
    context`` -- and our callbacks really do run at the same moment on different
    workers (a transport's reader and writer handles, say). So a supplied context
    is a *template*: each handle takes its own copy. Reads see everything the
    caller set; writes made inside one callback do not leak into the next, which
    under parallel dispatch is the only well-defined answer available.
    """
    return context.copy() if context is not None else contextvars.copy_context()


def _set_result_unless_done(fut, *args):
    if not fut.done():
        fut.set_result(None)


def _wrap_concurrent_future(cfut, loop):
    """Bridge a concurrent.futures.Future to a mt_asyncio.asyncio Future."""
    afut = loop.create_future()

    def _on_done(cf):
        if cf.cancelled():
            loop.call_soon(afut.cancel)
            return
        exc = cf.exception()
        if exc is not None:
            loop.call_soon(afut.set_exception, exc)
        else:
            loop.call_soon(afut.set_result, cf.result())

    cfut.add_done_callback(_on_done)
    return afut
