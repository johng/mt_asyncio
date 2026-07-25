# How mt_asyncio drives coroutines

## Threads

Three kinds, and only two of them do work:

- **the calling thread** — blocks in `done.wait()` (`_loop.py:249`) for the whole
  of `run_until_complete`. It drives nothing.
- **N workers** — each loops on `find_work() -> handle.run()` (`work.rs:133`),
  parking on a condvar when every queue is empty.
- **1 poll thread** — `mt_asyncio-reactor` (`_reactor.py:75`). Owns epoll/kqueue
  for file descriptors and the min-heap of timers.

## Work queues

Queues hold **handles**, not waiters: `{coro, ctx, value, checkpoint}`. A handle
is already-decided work — "step this coroutine with this value".

`value` is what will be **sent into** the coroutine, i.e. what the `await`
expression evaluates to. Almost always `None`; the exception is a multi-event
`Sentinel`, where it is the composed result list.

There is not one queue: a deque per worker, one global injector, and stealing
between them (`work.rs:100-125`).

## Stepping a coroutine

A worker pops a handle and calls `PyIter_Send(coro, value)`. This is the
`am_send` type **slot**, not the `send()` method — same "push a value in"
semantics, but it reports completion through a tri-state return plus an
out-param instead of raising `StopIteration`:

| result | out-param holds |
|---|---|
| `PYGEN_NEXT` | the coroutine suspended; the object it yielded |
| `PYGEN_RETURN` | it finished; its return value |
| `PYGEN_ERROR` | nothing (NULL); an exception is set |

Coroutine frames are heap-allocated, which is why a resume may land on any
worker rather than the one that parked it.

Only two yielded values are legal (`handles.rs:129-144`):

- `None` — bare suspension, re-queue the same handle
- a `Waiter` — park
- anything else — a `TypeError` is thrown in at the suspension point

Not every `await` reaches this code. `await coro` is pure interpreter
delegation; `await fut` on an already-done future never yields. An `await` chain
five frames deep produces **one** Waiter, at the innermost point.

## Parking

`Waiter::register_coro` (`events.rs:151`). One Waiter per suspension, with **no
links between successive waiters**:

- build one `Arc<CoroSuspension>` — resume target, `consumed` flag, optional sentinel
- push a `Waker` holding that Arc onto each event's watcher queue
- if the event is **already set**, wake inline instead of queueing
  (`add_waker` re-reads the flag under the watchers lock, so a set that races
  registration is never lost)
- if a timeout was given, also push a `Timer` sharing the same Arc
- the Waiter itself is then dropped — it is a one-shot message, not state

The only thing that persists across suspensions is `checkpoint`, threaded through
the handle. It is non-`None` only for `Waiter.checkpoint()`, the abort handle,
which `mt_asyncio.asyncio` never uses (see `_tasks.py:1-13`).

## Waking — push, never poll

Nothing scans for ready waiters. `Event.set()` (`events.rs:57`) CASes
`false -> true`, drains its waker queue, and each waker converts its
`CoroSuspension` into a fresh handle pushed onto a work queue. The
parked-to-runnable transition happens on whatever thread called `set()`.

A `consumed` CAS guarantees exactly one resume, which is how a timeout racing
its event resolves to a single wake.

The other wake source is the timer heap, drained by deadline on the poll thread
(`runtime.rs:145-159`). The poll timeout is derived from the earliest deadline,
so the thread sleeps in the kernel rather than spinning.
