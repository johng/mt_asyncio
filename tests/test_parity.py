"""Differential tests: ``mt_asyncio.asyncio`` must behave like upstream ``asyncio``.

Every test here is written once against a backend module and run twice -- once
with stdlib ``asyncio``, once with ``mt_asyncio.asyncio`` -- asserting the same
observable outcome from both. A failure means we diverge from CPython, which is
a bug in us unless it is one of the deliberate differences documented in
``mt_asyncio/asyncio/COMPATIBILITY.md`` (those are pinned at the bottom of this file,
where the *expected* divergence is asserted explicitly).

Scenarios only use names both backends provide, and avoid depending on
callback ordering/serialization -- which is exactly what mt_asyncio trades away for
parallelism.
"""

import asyncio
import threading
import time

import pytest

import mt_asyncio.asyncio as taio


BACKENDS = [pytest.param(asyncio, id='stdlib'), pytest.param(taio, id='mt_asyncio')]


@pytest.fixture(params=BACKENDS)
def aio(request):
    return request.param


class Boom(Exception):
    pass


# --------------------------------------------------------------------------
# run / entrypoint
# --------------------------------------------------------------------------


def test_run_returns_value(aio):
    async def main():
        return 42

    assert aio.run(main()) == 42


def test_run_propagates_exception(aio):
    async def main():
        raise Boom('kaboom')

    with pytest.raises(Boom, match='kaboom'):
        aio.run(main())


def test_run_rejects_non_coroutine(aio):
    with pytest.raises(TypeError):
        aio.run(42)


def test_run_rejects_nesting(aio):
    async def main():
        with pytest.raises(RuntimeError):
            aio.run(inner())
        return 'ok'

    async def inner():
        return None

    assert aio.run(main()) == 'ok'


# --------------------------------------------------------------------------
# sleep
# --------------------------------------------------------------------------


def test_sleep_elapses(aio):
    async def main():
        start = time.monotonic()
        await aio.sleep(0.05)
        return time.monotonic() - start

    assert aio.run(main()) >= 0.04


def test_sleep_returns_result(aio):
    async def main():
        return await aio.sleep(0, 'done')

    assert aio.run(main()) == 'done'


def test_sleep_zero_is_a_suspension_point(aio):
    async def main():
        order = []

        async def other():
            order.append('other')

        task = aio.create_task(other())
        await aio.sleep(0)
        await task
        order.append('main')
        return order

    assert aio.run(main()) == ['other', 'main']


# --------------------------------------------------------------------------
# tasks
# --------------------------------------------------------------------------


def test_create_task_result(aio):
    async def child():
        await aio.sleep(0)
        return 7

    async def main():
        task = aio.create_task(child())
        value = await task
        return value, task.done(), task.result(), task.cancelled()

    assert aio.run(main()) == (7, True, 7, False)


def test_task_exception_is_stored(aio):
    async def child():
        raise Boom('x')

    async def main():
        task = aio.create_task(child())
        with pytest.raises(Boom):
            await task
        return type(task.exception()), task.done(), task.cancelled()

    assert aio.run(main()) == (Boom, True, False)


def test_task_name(aio):
    async def child():
        return None

    async def main():
        task = aio.create_task(child(), name='worker')
        got = task.get_name()
        task.set_name('renamed')
        await task
        return got, task.get_name()

    assert aio.run(main()) == ('worker', 'renamed')


def test_task_cancel(aio):
    async def child():
        await aio.sleep(10)

    async def main():
        task = aio.create_task(child())
        await aio.sleep(0.01)
        cancelled_now = task.cancel()
        with pytest.raises(aio.CancelledError):
            await task
        return cancelled_now, task.cancelled(), task.done()

    assert aio.run(main()) == (True, True, True)


def test_cancel_on_done_task_returns_false(aio):
    async def child():
        return 1

    async def main():
        task = aio.create_task(child())
        await task
        return task.cancel()

    assert aio.run(main()) is False


def test_cancelled_error_is_asyncio_cancelled_error(aio):
    async def child():
        try:
            await aio.sleep(10)
        except BaseException as exc:
            return type(exc) is asyncio.CancelledError

    async def main():
        task = aio.create_task(child())
        await aio.sleep(0.01)
        task.cancel()
        return await task

    assert aio.run(main()) is True


def test_cancellation_runs_cleanup_that_awaits(aio):
    marks = []

    async def child():
        try:
            await aio.sleep(10)
        except aio.CancelledError:
            await aio.sleep(0)  # cleanup may await
            marks.append('cleaned')
            raise

    async def main():
        task = aio.create_task(child())
        await aio.sleep(0.01)
        task.cancel()
        with pytest.raises(aio.CancelledError):
            await task

    aio.run(main())
    assert marks == ['cleaned']


def test_cancelling_and_uncancel(aio):
    async def child():
        try:
            await aio.sleep(10)
        except aio.CancelledError:
            return 'caught'

    async def main():
        task = aio.create_task(child())
        await aio.sleep(0.01)
        task.cancel()
        counted = task.cancelling()
        result = await task
        return counted, result, task.uncancel()

    assert aio.run(main()) == (1, 'caught', 0)


def test_uncancel_withdraws_pending_cancel(aio):
    async def main():
        task = aio.current_task()
        task.cancel()
        requested = task.cancelling()
        task.uncancel()
        await aio.sleep(0)  # would raise if the pending cancel had stuck
        return requested, 'survived'

    assert aio.run(main()) == (1, 'survived')


def test_cancel_with_no_further_await_still_cancels(aio):
    holder = {}

    async def child():
        holder['task'].cancel()  # cancel while running; no await follows
        return 'returned'

    async def main():
        holder['task'] = aio.create_task(child())
        with pytest.raises(aio.CancelledError):
            await holder['task']
        return holder['task'].cancelled()

    assert aio.run(main()) is True


def test_current_task_identity(aio):
    async def main():
        task = aio.current_task()
        return task is not None, task.get_name() is not None

    assert aio.run(main()) == (True, True)


def test_all_tasks_includes_children(aio):
    async def child(ev):
        await ev.wait()

    async def main():
        ev = aio.Event()
        tasks = [aio.create_task(child(ev)) for _ in range(3)]
        await aio.sleep(0.01)
        count = len(aio.all_tasks())
        ev.set()
        await aio.gather(*tasks)
        return count

    # 3 children + the main task itself
    assert aio.run(main()) == 4


# --------------------------------------------------------------------------
# futures
# --------------------------------------------------------------------------


def test_future_set_result(aio):
    async def main():
        loop = aio.get_running_loop()
        fut = loop.create_future()
        loop.call_soon(fut.set_result, 'v')
        return await fut

    assert aio.run(main()) == 'v'


def test_future_set_exception(aio):
    async def main():
        loop = aio.get_running_loop()
        fut = loop.create_future()
        loop.call_soon(fut.set_exception, Boom('f'))
        with pytest.raises(Boom, match='f'):
            await fut
        return fut.done()

    assert aio.run(main()) is True


def test_future_double_set_raises_invalid_state(aio):
    async def main():
        loop = aio.get_running_loop()
        fut = loop.create_future()
        fut.set_result(1)
        with pytest.raises(aio.InvalidStateError):
            fut.set_result(2)
        return fut.result()

    assert aio.run(main()) == 1


def test_future_result_before_done_raises(aio):
    async def main():
        fut = aio.get_running_loop().create_future()
        with pytest.raises(aio.InvalidStateError):
            fut.result()
        fut.cancel()
        return fut.cancelled()

    assert aio.run(main()) is True


def test_future_cancel_then_await(aio):
    async def main():
        fut = aio.get_running_loop().create_future()
        fut.cancel()
        with pytest.raises(aio.CancelledError):
            await fut

    aio.run(main())


def test_future_done_callback_fires(aio):
    async def main():
        loop = aio.get_running_loop()
        fut = loop.create_future()
        seen = []
        done = loop.create_future()

        def _cb(f):
            seen.append(f.result())
            if not done.done():
                done.set_result(True)

        fut.add_done_callback(_cb)
        fut.set_result('x')
        await done
        return seen

    assert aio.run(main()) == ['x']


def test_future_done_callback_on_completed_future(aio):
    async def main():
        loop = aio.get_running_loop()
        fut = loop.create_future()
        fut.set_result(1)
        done = loop.create_future()
        fut.add_done_callback(lambda f: done.set_result(f.result()))
        return await done

    assert aio.run(main()) == 1


def test_future_remove_done_callback(aio):
    async def main():
        loop = aio.get_running_loop()
        fut = loop.create_future()

        def _cb(f):
            pass

        fut.add_done_callback(_cb)
        removed = fut.remove_done_callback(_cb)
        fut.set_result(None)
        await aio.sleep(0.01)
        return removed

    assert aio.run(main()) == 1


# --------------------------------------------------------------------------
# gather
# --------------------------------------------------------------------------


def test_gather_preserves_order(aio):
    async def item(v, delay):
        await aio.sleep(delay)
        return v

    async def main():
        return await aio.gather(item(1, 0.03), item(2, 0.01), item(3, 0.02))

    assert aio.run(main()) == [1, 2, 3]


def test_gather_empty(aio):
    async def main():
        return await aio.gather()

    assert aio.run(main()) == []


def test_gather_propagates_first_exception(aio):
    async def ok():
        await aio.sleep(0.05)
        return 1

    async def bad():
        await aio.sleep(0.01)
        raise Boom('g')

    async def main():
        with pytest.raises(Boom, match='g'):
            await aio.gather(ok(), bad())

    aio.run(main())


def test_gather_return_exceptions(aio):
    async def ok():
        return 1

    async def bad():
        raise Boom('g')

    async def main():
        out = await aio.gather(ok(), bad(), return_exceptions=True)
        return out[0], type(out[1])

    assert aio.run(main()) == (1, Boom)


def test_gather_cancel_cancels_children(aio):
    async def child(ev):
        try:
            await aio.sleep(10)
        finally:
            ev.set()

    async def main():
        ev = aio.Event()
        inner = aio.create_task(_gather_forever(aio, child, ev))
        await aio.sleep(0.02)
        inner.cancel()
        with pytest.raises(aio.CancelledError):
            await inner
        await ev.wait()
        return True

    assert aio.run(main()) is True


async def _gather_forever(aio, child, ev):
    await aio.gather(child(ev))


# --------------------------------------------------------------------------
# wait_for / shield / wait / as_completed
# --------------------------------------------------------------------------


def test_wait_for_returns_value(aio):
    async def child():
        await aio.sleep(0.01)
        return 'v'

    async def main():
        return await aio.wait_for(child(), 1)

    assert aio.run(main()) == 'v'


def test_wait_for_timeout_raises_and_cancels(aio):
    cancelled = []

    async def child():
        try:
            await aio.sleep(10)
        except aio.CancelledError:
            cancelled.append(True)
            raise

    async def main():
        with pytest.raises(TimeoutError):
            await aio.wait_for(child(), 0.02)

    aio.run(main())
    assert cancelled == [True]


def test_wait_for_none_timeout(aio):
    async def child():
        await aio.sleep(0.01)
        return 5

    async def main():
        return await aio.wait_for(child(), None)

    assert aio.run(main()) == 5


def test_shield_survives_outer_cancel(aio):
    marks = []

    async def child():
        await aio.sleep(0.05)
        marks.append('finished')
        return 'v'

    async def main():
        inner = aio.create_task(child())
        outer = aio.create_task(_await_shield(aio, inner))
        await aio.sleep(0.01)
        outer.cancel()
        with pytest.raises(aio.CancelledError):
            await outer
        return await inner

    assert aio.run(main()) == 'v'
    assert marks == ['finished']


async def _await_shield(aio, inner):
    return await aio.shield(inner)


def test_wait_all_completed(aio):
    async def item(v):
        await aio.sleep(0.01)
        return v

    async def main():
        tasks = [aio.create_task(item(i)) for i in range(3)]
        done, pending = await aio.wait(tasks)
        return len(done), len(pending), sorted(t.result() for t in done)

    assert aio.run(main()) == (3, 0, [0, 1, 2])


def test_wait_first_completed(aio):
    async def fast():
        await aio.sleep(0.01)
        return 'fast'

    async def slow():
        await aio.sleep(1)
        return 'slow'

    async def main():
        tasks = [aio.create_task(fast()), aio.create_task(slow())]
        done, pending = await aio.wait(tasks, return_when=aio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
        return len(done), len(pending), next(iter(done)).result()

    assert aio.run(main()) == (1, 1, 'fast')


def test_wait_timeout_leaves_pending(aio):
    async def slow():
        await aio.sleep(1)

    async def main():
        tasks = [aio.create_task(slow())]
        done, pending = await aio.wait(tasks, timeout=0.02)
        for t in pending:
            t.cancel()
        return len(done), len(pending)

    assert aio.run(main()) == (0, 1)


def test_as_completed_yields_every_result(aio):
    async def item(v, delay):
        await aio.sleep(delay)
        return v

    async def main():
        out = []
        for fut in aio.as_completed([item(1, 0.03), item(2, 0.01), item(3, 0.02)]):
            out.append(await fut)
        return out

    assert sorted(aio.run(main())) == [1, 2, 3]


# --------------------------------------------------------------------------
# timeout context managers
# --------------------------------------------------------------------------


def test_timeout_expires(aio):
    async def main():
        with pytest.raises(TimeoutError):
            async with aio.timeout(0.02):
                await aio.sleep(10)

    aio.run(main())


def test_timeout_not_reached(aio):
    async def main():
        async with aio.timeout(1) as cm:
            await aio.sleep(0.01)
            value = 'ok'
        return value, cm.expired()

    assert aio.run(main()) == ('ok', False)


def test_timeout_expired_flag(aio):
    async def main():
        cm = aio.timeout(0.02)
        try:
            async with cm:
                await aio.sleep(10)
        except TimeoutError:
            pass
        return cm.expired()

    assert aio.run(main()) is True


def test_timeout_at(aio):
    async def main():
        loop = aio.get_running_loop()
        with pytest.raises(TimeoutError):
            async with aio.timeout_at(loop.time() + 0.02):
                await aio.sleep(10)

    aio.run(main())


def test_timeout_none_never_expires(aio):
    async def main():
        async with aio.timeout(None):
            await aio.sleep(0.01)
        return 'ok'

    assert aio.run(main()) == 'ok'


# --------------------------------------------------------------------------
# TaskGroup
# --------------------------------------------------------------------------


def test_taskgroup_runs_children(aio):
    async def main():
        out = []

        async def child(v):
            await aio.sleep(0.01)
            out.append(v)

        async with aio.TaskGroup() as tg:
            for i in range(3):
                tg.create_task(child(i))
        return sorted(out)

    assert aio.run(main()) == [0, 1, 2]


def test_taskgroup_child_error_becomes_exception_group(aio):
    async def main():
        async def bad():
            raise Boom('tg')

        caught = []
        try:
            async with aio.TaskGroup() as tg:
                tg.create_task(bad())
        except* Boom as eg:
            caught.append(len(eg.exceptions))
        return caught

    assert aio.run(main()) == [1]


def test_taskgroup_error_cancels_siblings(aio):
    cancelled = []

    async def main():
        async def bad():
            await aio.sleep(0.01)
            raise Boom('tg')

        async def sibling():
            try:
                await aio.sleep(10)
            except aio.CancelledError:
                cancelled.append(True)
                raise

        with pytest.raises(BaseExceptionGroup):
            async with aio.TaskGroup() as tg:
                tg.create_task(sibling())
                tg.create_task(bad())

    aio.run(main())
    assert cancelled == [True]


def test_taskgroup_body_error_cancels_children(aio):
    cancelled = []

    async def main():
        async def child():
            try:
                await aio.sleep(10)
            except aio.CancelledError:
                cancelled.append(True)
                raise

        with pytest.raises(BaseExceptionGroup):
            async with aio.TaskGroup() as tg:
                tg.create_task(child())
                await aio.sleep(0.01)
                raise Boom('body')

    aio.run(main())
    assert cancelled == [True]


def test_taskgroup_many_failing_children(aio):
    """Exercises the child-done/parent-exit race: children finish on worker
    threads while the parent is already unwinding the group."""

    async def main():
        async def bad():
            # sleep first so every child is created before any of them aborts
            # the group (mt_asyncio starts children in parallel; see COMPATIBILITY.md)
            await aio.sleep(0.01)
            raise Boom('tg')

        caught = []
        try:
            async with aio.TaskGroup() as tg:
                for _ in range(20):
                    tg.create_task(bad())
        except* Boom as eg:
            caught.append(len(eg.exceptions))
        return caught

    counts = aio.run(main())
    assert len(counts) == 1
    assert counts[0] >= 1


def test_taskgroup_create_task_after_exit_raises(aio):
    async def main():
        async def child():
            return None

        async with aio.TaskGroup() as tg:
            tg.create_task(child())
        with pytest.raises(RuntimeError):
            tg.create_task(child())

    aio.run(main())


# --------------------------------------------------------------------------
# synchronization primitives
# --------------------------------------------------------------------------


def test_event_wait_and_set(aio):
    async def main():
        ev = aio.Event()
        before = ev.is_set()

        async def setter():
            await aio.sleep(0.01)
            ev.set()

        task = aio.create_task(setter())
        got = await ev.wait()
        await task
        return before, got, ev.is_set()

    assert aio.run(main()) == (False, True, True)


def test_event_clear(aio):
    async def main():
        ev = aio.Event()
        ev.set()
        ev.clear()
        return ev.is_set()

    assert aio.run(main()) is False


def test_event_wait_returns_immediately_when_set(aio):
    async def main():
        ev = aio.Event()
        ev.set()
        return await ev.wait()

    assert aio.run(main()) is True


def test_lock_is_mutually_exclusive(aio):
    async def main():
        lock = aio.Lock()
        peak = 0
        active = 0
        marker = threading.Lock()

        async def worker():
            nonlocal peak, active
            async with lock:
                with marker:
                    active += 1
                    peak = max(peak, active)
                await aio.sleep(0.005)
                with marker:
                    active -= 1

        await aio.gather(*[worker() for _ in range(8)])
        return peak

    assert aio.run(main()) == 1


def test_lock_locked_state(aio):
    async def main():
        lock = aio.Lock()
        before = lock.locked()
        await lock.acquire()
        during = lock.locked()
        lock.release()
        return before, during, lock.locked()

    assert aio.run(main()) == (False, True, False)


def test_semaphore_bounds_concurrency(aio):
    async def main():
        sem = aio.Semaphore(2)
        peak = 0
        active = 0
        marker = threading.Lock()

        async def worker():
            nonlocal peak, active
            async with sem:
                with marker:
                    active += 1
                    peak = max(peak, active)
                await aio.sleep(0.005)
                with marker:
                    active -= 1

        await aio.gather(*[worker() for _ in range(10)])
        return peak

    assert aio.run(main()) <= 2


def test_semaphore_locked_when_exhausted(aio):
    async def main():
        sem = aio.Semaphore(1)
        before = sem.locked()
        await sem.acquire()
        during = sem.locked()
        sem.release()
        return before, during, sem.locked()

    assert aio.run(main()) == (False, True, False)


def test_semaphore_rejects_negative(aio):
    with pytest.raises(ValueError):
        aio.Semaphore(-1)


def test_bounded_semaphore_over_release(aio):
    async def main():
        sem = aio.BoundedSemaphore(1)
        await sem.acquire()
        sem.release()
        with pytest.raises(ValueError):
            sem.release()

    aio.run(main())


def test_condition_notify(aio):
    async def main():
        cond = aio.Condition()
        out = []

        async def waiter(tag):
            async with cond:
                await cond.wait()
                out.append(tag)

        tasks = [aio.create_task(waiter(i)) for i in range(2)]
        await aio.sleep(0.02)
        async with cond:
            cond.notify_all()
        await aio.gather(*tasks)
        return sorted(out)

    assert aio.run(main()) == [0, 1]


# --------------------------------------------------------------------------
# queues
# --------------------------------------------------------------------------


def test_queue_fifo_order(aio):
    async def main():
        q = aio.Queue()
        for i in range(3):
            await q.put(i)
        return [await q.get() for _ in range(3)]

    assert aio.run(main()) == [0, 1, 2]


def test_lifo_queue_order(aio):
    async def main():
        q = aio.LifoQueue()
        for i in range(3):
            await q.put(i)
        return [await q.get() for _ in range(3)]

    assert aio.run(main()) == [2, 1, 0]


def test_priority_queue_order(aio):
    async def main():
        q = aio.PriorityQueue()
        for i in (3, 1, 2):
            await q.put(i)
        return [await q.get() for _ in range(3)]

    assert aio.run(main()) == [1, 2, 3]


def test_queue_get_nowait_empty(aio):
    async def main():
        q = aio.Queue()
        with pytest.raises(aio.QueueEmpty):
            q.get_nowait()

    aio.run(main())


def test_queue_put_nowait_full(aio):
    async def main():
        q = aio.Queue(maxsize=1)
        q.put_nowait(1)
        with pytest.raises(aio.QueueFull):
            q.put_nowait(2)
        return q.full(), q.qsize()

    assert aio.run(main()) == (True, 1)


def test_queue_put_blocks_when_full(aio):
    async def main():
        q = aio.Queue(maxsize=1)
        await q.put(1)
        putter = aio.create_task(q.put(2))
        await aio.sleep(0.02)
        blocked = not putter.done()
        first = await q.get()
        await putter
        return blocked, first, await q.get()

    assert aio.run(main()) == (True, 1, 2)


def test_queue_get_blocks_when_empty(aio):
    async def main():
        q = aio.Queue()
        getter = aio.create_task(q.get())
        await aio.sleep(0.02)
        blocked = not getter.done()
        await q.put('v')
        return blocked, await getter

    assert aio.run(main()) == (True, 'v')


def test_queue_empty_and_qsize(aio):
    async def main():
        q = aio.Queue()
        empty = q.empty()
        await q.put(1)
        return empty, q.empty(), q.qsize()

    assert aio.run(main()) == (True, False, 1)


def test_queue_join_and_task_done(aio):
    async def main():
        q = aio.Queue()
        for i in range(3):
            await q.put(i)

        async def consumer():
            for _ in range(3):
                await q.get()
                q.task_done()

        task = aio.create_task(consumer())
        await q.join()
        await task
        return q.qsize()

    assert aio.run(main()) == 0


def test_queue_task_done_too_many(aio):
    async def main():
        q = aio.Queue()
        await q.put(1)
        await q.get()
        q.task_done()
        with pytest.raises(ValueError):
            q.task_done()

    aio.run(main())


# --------------------------------------------------------------------------
# threads / executors
# --------------------------------------------------------------------------


def test_to_thread_returns_value(aio):
    async def main():
        return await aio.to_thread(lambda: 'threaded')

    assert aio.run(main()) == 'threaded'


def test_to_thread_propagates_exception(aio):
    def boom():
        raise Boom('t')

    async def main():
        with pytest.raises(Boom, match='t'):
            await aio.to_thread(boom)

    aio.run(main())


def test_to_thread_runs_off_the_calling_thread(aio):
    async def main():
        here = threading.get_ident()
        there = await aio.to_thread(threading.get_ident)
        return here != there

    assert aio.run(main()) is True


def test_run_in_executor(aio):
    async def main():
        loop = aio.get_running_loop()
        return await loop.run_in_executor(None, lambda: 21 * 2)

    assert aio.run(main()) == 42


def test_run_coroutine_threadsafe(aio):
    loop = aio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()

    async def work():
        await aio.sleep(0.01)
        return 'threadsafe'

    try:
        fut = aio.run_coroutine_threadsafe(work(), loop)
        assert fut.result(5) == 'threadsafe'
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        loop.close()


# --------------------------------------------------------------------------
# loop services
# --------------------------------------------------------------------------


def test_call_soon_runs_callback(aio):
    async def main():
        loop = aio.get_running_loop()
        fut = loop.create_future()
        loop.call_soon(lambda: fut.set_result('soon'))
        return await fut

    assert aio.run(main()) == 'soon'


def test_call_later_delays(aio):
    async def main():
        loop = aio.get_running_loop()
        fut = loop.create_future()
        start = loop.time()
        loop.call_later(0.03, lambda: fut.set_result(loop.time() - start))
        return await fut

    assert aio.run(main()) >= 0.02


def test_call_later_cancelled_handle_does_not_fire(aio):
    async def main():
        loop = aio.get_running_loop()
        fired = []
        handle = loop.call_later(0.01, lambda: fired.append(True))
        handle.cancel()
        await aio.sleep(0.05)
        return fired, handle.cancelled()

    assert aio.run(main()) == ([], True)


def test_loop_time_is_monotonic(aio):
    async def main():
        loop = aio.get_running_loop()
        first = loop.time()
        await aio.sleep(0.01)
        return loop.time() >= first

    assert aio.run(main()) is True


def test_get_running_loop_outside_raises(aio):
    with pytest.raises(RuntimeError):
        aio.get_running_loop()


def test_loop_close_is_idempotent(aio):
    loop = aio.new_event_loop()
    loop.close()
    loop.close()
    assert loop.is_closed()


def test_closed_loop_rejects_work(aio):
    loop = aio.new_event_loop()
    loop.close()

    async def noop():
        return None

    coro = noop()
    try:
        with pytest.raises(RuntimeError):
            loop.create_task(coro)
    finally:
        coro.close()


# --------------------------------------------------------------------------
# introspection helpers
# --------------------------------------------------------------------------


def test_iscoroutine_predicates(aio):
    async def coro():
        return None

    c = coro()
    try:
        assert aio.iscoroutine(c) is True
        assert aio.iscoroutinefunction(coro) is True
        assert aio.iscoroutine(42) is False
    finally:
        c.close()


def test_isfuture(aio):
    async def main():
        loop = aio.get_running_loop()
        fut = loop.create_future()
        result = aio.isfuture(fut), aio.isfuture(42)
        fut.cancel()
        return result

    assert aio.run(main()) == (True, False)


def test_ensure_future_wraps_coroutine(aio):
    async def child():
        return 3

    async def main():
        task = aio.ensure_future(child())
        same = aio.ensure_future(task)
        return await task, same is task

    assert aio.run(main()) == (3, True)


# --------------------------------------------------------------------------
# Documented, deliberate divergences (see COMPATIBILITY.md)
#
# These assert what mt_asyncio does *differently*. They are here so the difference is
# pinned by a test rather than left to drift.
# --------------------------------------------------------------------------


def test_divergence_callbacks_are_not_serialized():
    """stdlib runs every callback on one thread; mt_asyncio runs them on workers."""

    async def main():
        loop = taio.get_running_loop()
        fut = loop.create_future()
        loop.call_soon(lambda: fut.set_result(threading.get_ident()))
        callback_thread = await fut
        return callback_thread, threading.get_ident()

    callback_thread, _ = taio.run(main())
    assert isinstance(callback_thread, int)
    # the callback ran on a runtime worker, not on the thread that called run()
    assert callback_thread != threading.main_thread().ident


def test_divergence_stock_asyncio_future_is_not_awaitable():
    """A CPython Future yields itself; the mt_asyncio driver cannot resume it."""

    async def main():
        fut = asyncio.Future()
        fut.set_result(1)
        return await fut

    with pytest.raises(BaseException):  # noqa: B017 - any failure is the point
        taio.run(main())


def test_divergence_call_soon_is_threadsafe():
    """`call_soon` and `call_soon_threadsafe` are the same function."""
    assert taio.EventLoop.call_soon is taio.EventLoop.call_soon_threadsafe
