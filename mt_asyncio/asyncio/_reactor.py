"""Process-wide lifecycle for the shared mt_asyncio runtime behind asyncio loops.

Multiple :class:`~._loop.EventLoop` instances (e.g. one per OS thread in a
worker-per-thread server) share a single mt_asyncio runtime and its reactor. The
runtime must therefore be started once and stopped only when the *last* loop is
done with it -- a per-loop start/stop races ("self-socket already setup") and a
loop closing early would stop the runtime out from under its siblings.

This module owns that shared, reference-counted lifecycle under a lock.
"""

from __future__ import annotations

import threading
import time

from .._mt_asyncio import get_runtime
from .._runtime import new as _rt_new


try:  # pragma: no cover - depends on the compiled extension surface
    from .._mt_asyncio import RuntimeNotInitializedError as _RuntimeNotInitialized
except ImportError:  # pragma: no cover
    _RuntimeNotInitialized = RuntimeError


class _Reactor:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._count = 0
        self._runtime = None
        self._thread: threading.Thread | None = None
        # whether *we* started run_forever (vs piggy-backing on an externally
        # driven runtime, whose lifecycle we must not touch)
        self._owns_run = False

    def acquire(self, threads: int | None = None, *, context: bool = False):
        with self._lock:
            if self._count == 0:
                self._start(threads, context)
            self._count += 1
            return self._runtime

    def release(self) -> None:
        with self._lock:
            if self._count == 0:
                return
            self._count -= 1
            if self._count == 0 and self._owns_run:
                self._stop()

    def _start(self, threads: int | None, context: bool = False) -> None:
        try:
            runtime = get_runtime()
        except (_RuntimeNotInitialized, RuntimeError):
            runtime = None
        if runtime is None:
            runtime = _rt_new(threads=threads, context=context)
        elif context and not runtime._context:
            # a runtime already exists but does not propagate contextvars, so
            # current_task()/get_running_loop() would silently resolve wrong.
            # Fail loudly instead: the runtime is process-wide and its options
            # are fixed by whoever created it first.
            raise RuntimeError(
                'the running mt_asyncio runtime was created without `context=True`, which mt_asyncio.asyncio '
                'requires; create it with `mt_asyncio.runtime(context=True)` (or let the loop create it)'
            )
        self._runtime = runtime

        if getattr(runtime, '_ssock_w', None) is not None:
            # already driven elsewhere: reuse it, leave its lifecycle alone
            self._owns_run = False
            return

        thread = threading.Thread(
            target=runtime.run_forever,
            name='mt_asyncio-reactor',
            daemon=True,
        )
        thread.start()
        self._thread = thread
        self._owns_run = True

        deadline = time.monotonic() + 5.0
        while getattr(runtime, '_ssock_w', None) is None:
            if time.monotonic() > deadline:
                raise RuntimeError('timed out starting the mt_asyncio reactor thread')
            time.sleep(0.0005)

    def _stop(self) -> None:
        runtime, thread = self._runtime, self._thread
        self._thread = None
        self._owns_run = False
        if runtime is not None:
            runtime.stop()
        if thread is not None:
            thread.join(timeout=5.0)


# process-wide singleton
reactor = _Reactor()
