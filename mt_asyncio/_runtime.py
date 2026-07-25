import multiprocessing
import socket

from ._mt_asyncio import Runtime as _Runtime, set_runtime as _set_runtime
from ._signals import _set_sig_wfd, _sig_add, _sig_rem


class Runtime(_Runtime):
    """The process runtime: poll thread, worker threads and blocking pool.

    ``run_forever`` drives the reactor and blocks the calling thread; coroutines
    are scheduled onto the workers from anywhere. The asyncio loops in
    :mod:`mt_asyncio.asyncio` own this lifecycle for you (see
    ``mt_asyncio.asyncio._reactor``), so constructing one directly is only needed to
    choose the runtime's size or signal set up front.
    """

    def run_forever(self):
        try:
            self._run_forever_pre()
            self._run()
        finally:
            self._run_forever_post()

    def _run_forever_pre(self):
        self._ssock_start()
        self._sig_reg()

    def _run_forever_post(self):
        self._sig_dereg()
        self._ssock_stop()
        self._stopping = False

    def _ssock_start(self):
        if self._ssock_w is not None:
            raise RuntimeError('self-socket has been already setup')

        self._ssock_r, self._ssock_w = socket.socketpair()
        try:
            self._ssock_r.setblocking(False)
            self._ssock_w.setblocking(False)
        except Exception:
            self._ssock_w = None
            self._ssock_r = None
            raise

    def _ssock_stop(self):
        if not self._ssock_w:
            raise RuntimeError('self-socket has not been setup')

        self._ssock_w = None
        self._ssock_r = None

    def _sig_reg(self):
        try:
            fd = self._ssock_w.fileno()
            self._sig_wfd = _set_sig_wfd(fd)
            for sig in self._sigset:
                _sig_add(sig)
        except Exception:
            if self._sigset:
                raise
            return

        self._sig_listening = True

    def _sig_dereg(self):
        if not self._sig_listening:
            return

        self._sig_listening = False

        for sig in self._sigset:
            try:
                _sig_rem(sig)
            except Exception:
                pass
        _set_sig_wfd(self._sig_wfd)

    def stop(self):
        self._stopping = True


def new(
    context: bool = False,
    signals: list[int] | None = None,
    threads: int | None = None,
    blocking_threadpool_size: int = 128,
    blocking_threadpool_idle_ttl: int = 30,
) -> Runtime:
    # cores + headroom, not cores. Workers are the scheduler as well as the
    # execution capacity, so a blocking call in a coroutine consumes one; with
    # exactly `cpu_count()` of them, `cpu_count()` concurrent blocking calls
    # leave nothing to run task steps, timer callbacks or I/O dispatch, and the
    # runtime stalls for the duration with no throughput signal to warn you.
    # The spare workers are off-CPU whenever they are idle or blocked, so the
    # margin costs little and removes the sharpest edge in the model.
    threads = threads or multiprocessing.cpu_count() + 4
    runtime = Runtime(
        threads=threads,
        threads_blocking=blocking_threadpool_size,
        threads_blocking_timeout=blocking_threadpool_idle_ttl,
        context=context,
        signals=signals or [],
    )
    _set_runtime(runtime)
    return runtime
