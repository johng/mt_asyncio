"""Tests for mt_asyncio's multi-threaded asyncio loop (``mt_asyncio.asyncio``).

Plain synchronous tests driving real asyncio-style coroutines via
``mt_asyncio.asyncio.run``. Require a free-threaded build.
"""

import os
import socket
import threading
import time

import pytest

import mt_asyncio.asyncio as aio


def test_run_returns_value():
    async def main():
        return 42

    assert aio.run(main()) == 42


def test_run_propagates_exception():
    class Boom(Exception):
        pass

    async def main():
        raise Boom('x')

    with pytest.raises(Boom, match='x'):
        aio.run(main())


def test_sleep():
    async def main():
        start = time.monotonic()
        await aio.sleep(0.05)
        return time.monotonic() - start

    assert aio.run(main()) >= 0.05


def test_gather_ordered_and_concurrent():
    async def item(v, d):
        await aio.sleep(d)
        return v

    async def main():
        start = time.monotonic()
        out = await aio.gather(item(1, 0.05), item(2, 0.1), item(3, 0.05))
        return out, time.monotonic() - start

    out, elapsed = aio.run(main())
    assert out == [1, 2, 3]
    assert elapsed < 0.2  # concurrent, not summed


def test_gather_return_exceptions():
    async def ok(v):
        await aio.sleep(0.01)
        return v

    async def bad():
        await aio.sleep(0.01)
        raise ValueError('nope')

    async def main():
        return await aio.gather(ok(1), bad(), ok(3), return_exceptions=True)

    out = aio.run(main())
    assert out[0] == 1 and out[2] == 3
    assert isinstance(out[1], ValueError)


def test_gather_first_error_propagates():
    async def bad():
        await aio.sleep(0.01)
        raise KeyError('k')

    async def main():
        with pytest.raises(KeyError):
            await aio.gather(aio.sleep(1), bad())
        return 'ok'

    assert aio.run(main()) == 'ok'


def test_create_task_and_current_task():
    async def worker():
        return aio.current_task().get_name()

    async def main():
        t = aio.create_task(worker(), name='w1')
        return await t

    assert aio.run(main()) == 'w1'


def test_true_multicore_parallelism():
    """CPU-bound tasks must genuinely overlap, not take turns.

    Sized to the machine on purpose. With more tasks than cores the best
    achievable ratio is not ``1 / tasks`` but ``ceil(tasks / cores) / tasks``,
    so a fixed four tasks on a three-core runner tops out at 0.5 -- which sat
    directly on the old 0.6 threshold and failed there about as often as it
    passed. Matching the fan-out to the core count puts the ideal back at
    ``1 / cores`` wherever this runs.

    The bar is halfway between perfect scaling and none, so it stays clear of
    both: a runtime that had lost parallel stepping scores ~1.0 and fails on any
    machine. This is a correctness check, not a benchmark -- how *close* to
    ideal we get is what ``bench/`` is for.

    Note there is no ``threads=`` here, and the ``threads=4`` this used to pass
    was doing nothing: ``conftest`` builds the process-wide runtime, and the
    first caller fixes its worker count for good. The fan-out below is over
    those workers.
    """
    cores = min(4, os.process_cpu_count() or 1)
    if cores < 2:
        pytest.skip('parallel stepping cannot be observed on a single core')

    def burn(n):
        x = 0
        for i in range(n):
            x += i * i
        return x

    async def cpu():
        return burn(4_000_000)

    async def main():
        s = time.monotonic()
        for _ in range(cores):
            await cpu()
        serial = time.monotonic() - s
        s = time.monotonic()
        await aio.gather(*[cpu() for _ in range(cores)])
        parallel = time.monotonic() - s
        return serial, parallel

    # interference on a shared runner only ever makes the parallel leg look
    # worse, so the best of a few attempts estimates the floor we care about;
    # a single sample measures whatever else the machine was doing
    ratios = []
    for _ in range(3):
        serial, parallel = aio.run(main())
        ratios.append(parallel / serial)

    best = min(ratios)
    bar = (1.0 / cores + 1.0) / 2
    assert best < bar, f'{cores} tasks on {cores} cores: parallel/serial {best:.2f}, need < {bar:.2f}'


def test_wait_for_timeout():
    async def main():
        with pytest.raises(aio.TimeoutError):
            await aio.wait_for(aio.sleep(1.0), timeout=0.05)
        return 'ok'

    assert aio.run(main()) == 'ok'


def test_wait_for_success():
    async def main():
        return await aio.wait_for(_value(7), timeout=1.0)

    async def _value(v):
        await aio.sleep(0.01)
        return v

    assert aio.run(main()) == 7


def test_cancel_with_async_cleanup():
    # the killer case: an await during cancellation cleanup must NOT wedge
    async def main():
        log = []

        async def worker():
            try:
                await aio.sleep(10)
            except aio.CancelledError:
                log.append('caught')
                await aio.sleep(0.01)  # async cleanup after cancel
                log.append('cleaned')
                raise

        t = aio.create_task(worker())
        await aio.sleep(0.05)
        t.cancel()
        with pytest.raises(aio.CancelledError):
            await t
        return log

    assert aio.run(main()) == ['caught', 'cleaned']


def test_cancel_swallowed():
    # a task may catch CancelledError and return normally
    async def main():
        async def worker():
            try:
                await aio.sleep(10)
            except aio.CancelledError:
                return 'swallowed'

        t = aio.create_task(worker())
        await aio.sleep(0.03)
        t.cancel()
        return await t

    assert aio.run(main()) == 'swallowed'


def test_run_in_executor():
    def blocking():
        time.sleep(0.02)
        return threading.get_ident()

    async def main():
        loop = aio.get_running_loop()
        worker_tid = await loop.run_in_executor(None, blocking)
        return worker_tid

    assert isinstance(aio.run(main()), int)


def test_lock_mutual_exclusion():
    async def main():
        lock = aio.Lock()
        mu = threading.Lock()
        st = {'cur': 0, 'max': 0, 'total': 0}

        async def worker():
            async with lock:
                with mu:
                    st['cur'] += 1
                    st['max'] = max(st['max'], st['cur'])
                await aio.sleep(0.003)
                with mu:
                    st['cur'] -= 1
                    st['total'] += 1

        await aio.gather(*[worker() for _ in range(16)])
        return st['max'], st['total']

    max_concurrent, total = aio.run(main(), threads=4)
    assert max_concurrent == 1
    assert total == 16


def test_lock_cancel_safety():
    async def main():
        lock = aio.Lock()
        await lock.acquire()

        async def waiter():
            async with lock:
                return 'got'

        w = aio.create_task(waiter())
        await aio.sleep(0.02)
        w.cancel()
        with pytest.raises(aio.CancelledError):
            await w
        lock.release()
        return await aio.wait_for(lock.acquire(), 0.5)

    assert aio.run(main()) is True


def test_semaphore_limits_concurrency():
    async def main():
        sem = aio.Semaphore(3)
        mu = threading.Lock()
        st = {'cur': 0, 'max': 0}

        async def worker():
            async with sem:
                with mu:
                    st['cur'] += 1
                    st['max'] = max(st['max'], st['cur'])
                await aio.sleep(0.004)
                with mu:
                    st['cur'] -= 1

        await aio.gather(*[worker() for _ in range(15)])
        return st['max']

    assert aio.run(main(), threads=8) == 3


def test_event_broadcast():
    async def main():
        ev = aio.Event()
        got = []

        async def w(i):
            await ev.wait()
            got.append(i)

        ts = [aio.create_task(w(i)) for i in range(6)]
        await aio.sleep(0.02)
        ev.set()
        await aio.gather(*ts)
        return sorted(got)

    assert aio.run(main(), threads=4) == [0, 1, 2, 3, 4, 5]


def test_queue_backpressure():
    async def main():
        q = aio.Queue(maxsize=2)
        out = []

        async def producer():
            for i in range(6):
                await q.put(i)
            await q.put(None)

        async def consumer():
            while True:
                x = await q.get()
                if x is None:
                    break
                out.append(x)

        await aio.gather(producer(), consumer())
        return out

    assert aio.run(main(), threads=4) == [0, 1, 2, 3, 4, 5]


def test_sock_tcp_echo():
    payload = b'hello mt asyncio' * 500

    async def main():
        loop = aio.get_running_loop()
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(('127.0.0.1', 0))
        listener.listen(8)
        listener.setblocking(False)
        addr = listener.getsockname()

        async def server():
            conn, _ = await loop.sock_accept(listener)
            with conn:
                buf = bytearray()
                while len(buf) < len(payload):
                    chunk = await loop.sock_recv(conn, 65536)
                    if not chunk:
                        break
                    buf += chunk
                await loop.sock_sendall(conn, bytes(buf))

        srv = aio.create_task(server())
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.setblocking(False)
        with listener, client:
            await loop.sock_connect(client, addr)
            await loop.sock_sendall(client, payload)
            recvd = bytearray()
            while len(recvd) < len(payload):
                chunk = await loop.sock_recv(client, 65536)
                if not chunk:
                    break
                recvd += chunk
            await srv
            return bytes(recvd)

    assert aio.run(main(), threads=4) == payload


# --- TaskGroup ---------------------------------------------------------------


def test_taskgroup_all_succeed():
    async def main():
        out = []

        async def app(i):
            await aio.sleep(0.01 * (i % 3))
            out.append(i)

        async with aio.TaskGroup() as tg:
            for i in range(5):
                tg.create_task(app(i))
        return sorted(out)

    assert aio.run(main(), threads=4) == [0, 1, 2, 3, 4]


def test_taskgroup_child_error_cancels_siblings():
    async def main():
        cancelled = []

        async def good(i):
            try:
                await aio.sleep(1)
            except aio.CancelledError:
                cancelled.append(i)
                raise

        async def bad():
            await aio.sleep(0.02)
            raise ValueError('boom')

        errs = None
        try:
            async with aio.TaskGroup() as tg:
                for i in range(3):
                    tg.create_task(good(i))
                tg.create_task(bad())
        except BaseExceptionGroup as eg:
            errs = [type(e).__name__ for e in eg.exceptions]
        return errs, len(cancelled)

    errs, ncancelled = aio.run(main(), threads=4)
    assert errs == ['ValueError']
    assert ncancelled == 3


def test_taskgroup_body_error_cancels_children():
    async def main():
        cancelled = []

        async def child():
            try:
                await aio.sleep(1)
            except aio.CancelledError:
                cancelled.append(1)
                raise

        raised = None
        try:
            async with aio.TaskGroup() as tg:
                tg.create_task(child())
                tg.create_task(child())
                await aio.sleep(0.02)
                raise KeyError('body')
        except BaseExceptionGroup as eg:
            raised = [type(e).__name__ for e in eg.exceptions]
        return raised, len(cancelled)

    raised, ncancelled = aio.run(main(), threads=4)
    assert 'KeyError' in raised
    assert ncancelled == 2


def test_taskgroup_external_cancel():
    async def main():
        started = aio.Event()

        async def group():
            async with aio.TaskGroup() as tg:
                tg.create_task(aio.sleep(1))
                started.set()
                await aio.sleep(1)

        t = aio.create_task(group())
        await started.wait()
        t.cancel()
        with pytest.raises(aio.CancelledError):
            await t
        return 'ok'

    assert aio.run(main(), threads=4) == 'ok'


# --- timeout() ---------------------------------------------------------------


def test_timeout_expires():
    async def main():
        with pytest.raises(aio.TimeoutError):
            async with aio.timeout(0.05):
                await aio.sleep(1)
        return 'ok'

    assert aio.run(main()) == 'ok'


def test_timeout_success():
    async def main():
        async with aio.timeout(1.0):
            await aio.sleep(0.01)
        return 'completed'

    assert aio.run(main()) == 'completed'


def test_timeout_none_disables():
    async def main():
        async with aio.timeout(None):
            await aio.sleep(0.01)
        return 'ok'

    assert aio.run(main()) == 'ok'


# --- as_completed ------------------------------------------------------------


def test_as_completed_order():
    async def item(v, d):
        await aio.sleep(d)
        return v

    async def main():
        order = []
        for coro in aio.as_completed([item('slow', 0.06), item('fast', 0.01), item('mid', 0.03)]):
            order.append(await coro)
        return order

    assert aio.run(main(), threads=4) == ['fast', 'mid', 'slow']


# --- queues ------------------------------------------------------------------


def test_lifo_queue():
    async def main():
        q = aio.LifoQueue()
        for i in (1, 2, 3):
            q.put_nowait(i)
        return [q.get_nowait() for _ in range(3)]

    assert aio.run(main()) == [3, 2, 1]


def test_priority_queue():
    async def main():
        q = aio.PriorityQueue()
        for i in (3, 1, 2):
            q.put_nowait(i)
        return [q.get_nowait() for _ in range(3)]

    assert aio.run(main()) == [1, 2, 3]


def test_queue_empty_full_nowait():
    async def main():
        q = aio.Queue(maxsize=1)
        q.put_nowait('a')
        full = False
        try:
            q.put_nowait('b')
        except aio.QueueFull:
            full = True
        q.get_nowait()
        empty = False
        try:
            q.get_nowait()
        except aio.QueueEmpty:
            empty = True
        return full, empty

    assert aio.run(main()) == (True, True)


# --- Condition ---------------------------------------------------------------


def test_condition_notify():
    async def main():
        cond = aio.Condition()
        received = []

        async def waiter(i):
            async with cond:
                await cond.wait()
                received.append(i)

        ws = [aio.create_task(waiter(i)) for i in range(3)]
        await aio.sleep(0.03)
        async with cond:
            cond.notify_all()
        await aio.gather(*ws)
        return sorted(received)

    assert aio.run(main(), threads=4) == [0, 1, 2]


# --- Future / Task direct API ------------------------------------------------


def test_future_states():
    async def main():
        loop = aio.get_running_loop()
        f = loop.create_future()
        pending = f.done()
        f.set_result(5)
        double = False
        try:
            f.set_result(6)
        except aio.InvalidStateError:
            double = True
        return pending, f.done(), f.result(), double

    assert aio.run(main()) == (False, True, 5, True)


def test_task_cancelling_uncancel():
    async def main():
        async def w():
            await aio.sleep(10)

        t = aio.create_task(w())
        await aio.sleep(0.02)
        before = t.cancelling()
        t.cancel()
        after = t.cancelling()
        un = t.uncancel()
        with pytest.raises(aio.CancelledError):
            t.cancel()  # still deliverable
            await t
        return before, after, un

    before, after, un = aio.run(main())
    assert before == 0 and after == 1 and un == 0


def test_bare_await_future_is_cancellable():
    async def main():
        loop = aio.get_running_loop()
        fut = loop.create_future()

        async def w():
            return await fut  # bare await, never completes

        t = aio.create_task(w())
        await aio.sleep(0.02)
        t.cancel()
        with pytest.raises(aio.CancelledError):
            await t
        return 'ok'

    assert aio.run(main()) == 'ok'


def test_shield_inner_survives_outer_cancel():
    async def main():
        log = []

        async def inner():
            await aio.sleep(0.05)
            log.append('inner-done')

        sh = aio.shield(inner())

        async def await_it():
            await sh

        t = aio.create_task(await_it())
        await aio.sleep(0.01)
        t.cancel()
        with pytest.raises(aio.CancelledError):
            await t
        log.append('outer-cancelled')
        await aio.sleep(0.1)
        return log

    result = aio.run(main(), threads=4)
    assert 'outer-cancelled' in result
    assert 'inner-done' in result


# --- wait variants -----------------------------------------------------------


def test_wait_first_completed():
    async def main():
        async def fast():
            await aio.sleep(0.01)
            return 'f'

        async def slow():
            await aio.sleep(1.0)
            return 's'

        done, pending = await aio.wait(
            [aio.create_task(fast()), aio.create_task(slow())],
            return_when=aio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
        return len(done), len(pending)

    assert aio.run(main(), threads=4) == (1, 1)


# --- bad yields --------------------------------------------------------------


def test_bad_yield_raises_typeerror():
    """A coroutine yielding something the runtime cannot drive gets a TypeError
    thrown in at the suspension point, rather than aborting the process."""

    class Bad:
        def __await__(self):
            yield object()

    async def main():
        await Bad()

    with pytest.raises(TypeError, match='cannot await'):
        aio.run(main())


def test_bad_yield_stdlib_future_raises_typeerror():
    """A stock `asyncio.Future` yields *itself*, which only CPython's
    `Task.__step` can park on."""
    import asyncio as std

    async def main():
        await std.Future(loop=aio.get_running_loop())

    with pytest.raises(TypeError, match='asyncio.Future'):
        aio.run(main())


def test_bad_yield_is_catchable_and_resumable():
    """The injected error is an ordinary exception: catching it leaves the
    coroutine fully usable, including further awaits."""

    class Bad:
        def __await__(self):
            yield object()

    async def main():
        caught = None
        try:
            await Bad()
        except TypeError as exc:
            caught = type(exc).__name__
        await aio.sleep(0.01)  # re-suspend after catching
        return caught, await aio.create_task(_ok())

    async def _ok():
        await aio.sleep(0)
        return 'ok'

    assert aio.run(main()) == ('TypeError', 'ok')


def test_bad_yield_in_task_surfaces_on_the_task():
    class Bad:
        def __await__(self):
            yield object()

    async def main():
        async def child():
            await Bad()

        task = aio.create_task(child())
        try:
            await task
        except TypeError:
            pass
        return task.done(), isinstance(task.exception(), TypeError)

    assert aio.run(main()) == (True, True)


# --- refcounting -------------------------------------------------------------


def test_spawned_coroutine_return_value_is_released():
    """`PyIter_Send` hands back a new reference to a coroutine's return value;
    the runtime must release it or every completed spawn leaks its result."""
    import gc

    from mt_asyncio._mt_asyncio import get_runtime

    class Obj:
        pass

    def live():
        gc.collect()
        return sum(1 for o in gc.get_objects() if type(o) is Obj)

    async def returns_obj():
        return Obj()

    async def main():
        before = live()
        for _ in range(200):
            get_runtime()._spawn_coro(returns_obj())
        await aio.sleep(0.25)
        return before, live()

    before, after = aio.run(main())
    assert after == before
