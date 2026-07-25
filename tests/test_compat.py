"""``mt_asyncio.asyncio.compat``: code that does ``import asyncio`` on our loop.

Two things are under test. First, that the shadowed names really are ours --
patching is process-global, so a leak would silently turn ``test_parity.py`` into
a suite comparing mt_asyncio against itself, and every test here therefore
installs through a fixture with hard teardown. Second, that the patterns which
*fail* on stdlib primitives under parallel stepping now hold: `gather`'s
unsynchronised ``nfinished += 1`` hangs, `Lock` corrupts its waiter deque, and
`TaskGroup` raises "Set changed size during iteration" -- all measured before
this module existed.

The parallel-stress cases are run repeatedly on purpose: the stdlib failures are
races that need several attempts to show up, so a single pass proves little.
"""

import asyncio
import threading

import pytest

import mt_asyncio.asyncio as taio


# the genuine stdlib objects, captured before any test can shadow them
STDLIB_FUTURE = asyncio.Future
STDLIB_GATHER = asyncio.gather
STDLIB_LOCK = asyncio.Lock

STRESS_RUNS = 5


@pytest.fixture
def compat():
    """Install compat mode for one test, and guarantee it comes back off."""
    assert not taio.compat.is_installed(), 'compat leaked from an earlier test'
    with taio.compat.installed():
        yield taio.compat
    assert not taio.compat.is_installed()
    assert asyncio.gather is STDLIB_GATHER
    assert asyncio.Lock is STDLIB_LOCK
    assert asyncio.Future is STDLIB_FUTURE


# --------------------------------------------------------------------------
# install / uninstall mechanics
# --------------------------------------------------------------------------


def test_not_installed_by_default():
    assert not taio.compat.is_installed()
    assert asyncio.gather is STDLIB_GATHER


def test_install_shadows_the_unsafe_names(compat):
    assert asyncio.gather is taio.gather
    assert asyncio.Lock is taio.Lock
    assert asyncio.TaskGroup is taio.TaskGroup
    assert asyncio.Queue is taio.Queue
    assert asyncio.Event is taio.Event
    assert asyncio.sleep is taio.sleep


def test_install_patches_submodule_bindings(compat):
    # `from asyncio.locks import Lock` must see the replacement too
    import asyncio.locks
    import asyncio.tasks

    assert asyncio.locks.Lock is taio.Lock
    assert asyncio.tasks.gather is taio.gather


def test_install_is_idempotent(compat):
    taio.compat.install()
    taio.compat.install()
    assert taio.compat.is_installed()
    # a second install must not have saved our own patches as the "originals"
    taio.compat.uninstall()
    assert asyncio.gather is STDLIB_GATHER
    taio.compat.install()  # restore state the fixture expects to tear down


def test_uninstall_without_install_is_harmless():
    taio.compat.uninstall()
    assert asyncio.gather is STDLIB_GATHER


def test_exception_types_are_not_shadowed(compat):
    # we re-export the stdlib objects, so these must be untouched identities
    assert asyncio.CancelledError is taio.CancelledError
    assert asyncio.TimeoutError is taio.TimeoutError
    assert asyncio.InvalidStateError is taio.InvalidStateError


def test_transport_methods_still_fail_loudly(compat):
    # compat must not imply networking it cannot deliver: these stay the
    # inherited AbstractEventLoop stubs, which raise NotImplementedError
    loop = taio.new_event_loop()
    for name in ('create_datagram_endpoint', 'sendfile'):
        assert getattr(type(loop), name) is getattr(asyncio.AbstractEventLoop, name), name


def test_fd_callbacks_are_implemented(compat):
    # add_reader/add_writer are ours, not the inherited stubs -- this is what
    # libpq-style drivers (psycopg's async path) wait on
    loop = taio.new_event_loop()
    for name in ('add_reader', 'add_writer', 'remove_reader', 'remove_writer'):
        assert getattr(type(loop), name) is not getattr(asyncio.AbstractEventLoop, name), name
    # removing something that was never added is a no-op, as in asyncio
    assert loop.remove_reader(0) is False
    assert loop.remove_writer(0) is False


def test_loop_is_an_abstract_event_loop():
    # third-party isinstance checks must pass, with or without compat installed
    assert isinstance(taio.new_event_loop(), asyncio.AbstractEventLoop)


# --------------------------------------------------------------------------
# the patterns that break on stdlib primitives under parallel stepping
# --------------------------------------------------------------------------


async def _unit(i):
    await asyncio.sleep(0)
    return i


@pytest.mark.parametrize('run', range(STRESS_RUNS))
def test_gather_resolves(compat, run):
    """stdlib `gather` does an unsynchronised `nfinished += 1` -> lost wakeup."""

    async def main():
        return await asyncio.gather(*[_unit(i) for i in range(300)])

    assert taio.run(main()) == list(range(300))


@pytest.mark.parametrize('run', range(STRESS_RUNS))
def test_lock_is_mutually_exclusive(compat, run):
    """stdlib `Lock` check-then-sets `_locked` over a bare deque."""

    async def main():
        lock = asyncio.Lock()
        state = {'inside': 0, 'peak': 0, 'done': 0}

        async def critical():
            async with lock:
                state['inside'] += 1
                state['peak'] = max(state['peak'], state['inside'])
                await asyncio.sleep(0)
                state['done'] += 1
                state['inside'] -= 1

        async with asyncio.TaskGroup() as tg:
            for _ in range(300):
                tg.create_task(critical())
        return state

    state = taio.run(main())
    assert state['peak'] == 1, f'lock admitted {state["peak"]} holders at once'
    assert state['done'] == 300


@pytest.mark.parametrize('run', range(STRESS_RUNS))
def test_taskgroup_runs_every_child(compat, run):
    """stdlib `TaskGroup._tasks` is a plain set -> "changed size during iteration"."""

    async def main():
        seen = []
        seen_lock = threading.Lock()

        async def child(i):
            await asyncio.sleep(0)
            with seen_lock:
                seen.append(i)

        async with asyncio.TaskGroup() as tg:
            for i in range(300):
                tg.create_task(child(i))
        return seen

    assert sorted(taio.run(main())) == list(range(300))


@pytest.mark.parametrize('run', range(STRESS_RUNS))
def test_queue_loses_nothing(compat, run):
    async def main():
        queue = asyncio.Queue()
        out = []

        async def producer():
            for i in range(200):
                await queue.put(i)

        async def consumer():
            for _ in range(200):
                out.append(await queue.get())

        async with asyncio.TaskGroup() as tg:
            tg.create_task(producer())
            tg.create_task(consumer())
        return out

    assert sorted(taio.run(main())) == list(range(200))


def test_wait_for_and_shield(compat):
    async def main():
        assert await asyncio.wait_for(_unit(7), timeout=5) == 7
        assert await asyncio.shield(_unit(9)) == 9

    taio.run(main())


def test_create_task_and_current_task(compat):
    async def main():
        async def child():
            await asyncio.sleep(0)
            return asyncio.current_task() is not None

        task = asyncio.create_task(child())
        assert isinstance(task, taio.Task)
        return await task

    assert taio.run(main()) is True


def test_get_running_loop_inside_compat(compat):
    async def main():
        loop = asyncio.get_running_loop()
        assert isinstance(loop, taio.EventLoop)
        assert asyncio.get_event_loop() is loop
        return True

    assert taio.run(main()) is True


# --------------------------------------------------------------------------
# the boundary: foreign awaitables are not driven, by design
# --------------------------------------------------------------------------


def test_foreign_future_is_rejected_without_compat():
    """A genuine stdlib future is not drivable, and says so."""

    async def main():
        loop = taio.get_running_loop()
        fut = STDLIB_FUTURE(loop=loop)
        loop.call_soon(fut.set_result, 1)  # resolve later, so we really suspend
        return await fut

    with pytest.raises(TypeError, match='may only yield mt_asyncio'):
        taio.run(main())


def test_foreign_future_is_still_rejected_with_compat(compat):
    """Compat shadows the *names*; it does not teach the runtime foreign futures.

    Nothing needs it to: install-before-import closes every route to a real
    stdlib future, since `Future`, `asyncio.futures.Future` and the
    `asyncio.tasks` internals are all rebound and no stdlib `Task` is ever
    constructed. Reaching one means a stale pre-install binding, and a `TypeError`
    naming the problem beats parking on something we cannot wake.
    """

    async def main():
        loop = asyncio.get_running_loop()
        fut = STDLIB_FUTURE(loop=loop)
        loop.call_soon(fut.set_result, 1)
        return await fut

    with pytest.raises(TypeError, match='may only yield mt_asyncio'):
        taio.run(main())


def test_shadowed_future_is_ours_and_awaits_fine(compat):
    """The path that actually matters: `asyncio.Future()` resolves to ours."""

    async def main():
        loop = asyncio.get_running_loop()
        assert asyncio.Future is taio.Future
        out = []
        for i in range(50):
            fut = asyncio.Future()

            async def resolve(fut=fut, i=i):
                await asyncio.sleep(0)
                fut.set_result(i * 3)

            asyncio.create_task(resolve())
            out.append(await fut)
        assert loop is asyncio.get_running_loop()
        return out

    assert taio.run(main()) == [i * 3 for i in range(50)]


def test_loop_create_future_is_ours(compat):
    async def main():
        return type(asyncio.get_running_loop().create_future())

    assert taio.run(main()) is taio.Future
