# `mt_asyncio.asyncio` — asyncio compatibility & gaps

`mt_asyncio.asyncio` is a single asyncio-compatible event loop that runs tasks in
**true parallel** across mt_asyncio's worker threads on free-threaded CPython. It
presents the asyncio API so that

```python
import mt_asyncio.asyncio as asyncio
```

is a drop-in for code written against `asyncio`. This document records exactly
what is supported, what behaves differently, and what is not implemented.

Coverage of the public `asyncio` namespace: **43 / 119 names**. The bulk of what
is missing is one coherent subsystem (streams/transports/subprocess) plus
introspection and loop-policy plumbing — see [Not implemented](#not-implemented).

---

## 1. The one contract that changes: callbacks are not serialized

Stdlib asyncio runs **every** callback, task step, and done-callback on a single
loop thread, which gives implicit mutual exclusion — code can mutate shared state
from a callback without locking. `mt_asyncio.asyncio` runs task steps and callbacks
on **many worker threads, in parallel**. That is the entire point (it is how you
get multiple cores), but it means:

- **Shared state touched from tasks or callbacks must be locked.** Use the
  provided `Lock`/`Semaphore`/`Event`/`Queue` (they are genuinely cross-thread),
  or a `threading.Lock`. A `+=` on a shared dict/counter from parallel tasks is a
  data race, exactly as it would be with `threading`.
- **`call_soon` == `call_soon_threadsafe`.** There is no distinguished loop
  thread to hop to; both schedule onto a worker. Callbacks may run concurrently
  and their ordering relative to the scheduling code is only "eventually."
- **`loop.call_soon` callbacks and `Future.add_done_callback` callbacks may
  overlap.** The library's own aggregators (`gather`, `wait`, `TaskGroup`) are
  internally locked; your callbacks are your responsibility.

It is unavoidable for real parallelism. It is the single largest behavioral difference from stdlib asyncio.

### The exception: one connection is still serialized

Transports are the one place the guarantee is put back, because protocol objects
are written against it and cannot be locked from outside. For a single
connection, `mt_asyncio.asyncio` reproduces the stdlib ordering exactly:

1. **Callbacks never overlap.** One `RLock` per connection covers
   `data_received`, `_write_ready`, `connection_made`, `connection_lost` and every
   app-facing method. See `_transports.py`.
2. **A callback finishes before any task it woke takes a step.** Completing a
   future inside a callback only *schedules* the awaiter in stdlib; it did so
   immediately here, on another worker. Wakes raised inside a protocol callback
   are now held until it returns. See `_wakes.py`.
3. **No callback runs between a task's `write()` and its next suspension.** A
   stdlib loop cannot regain control in that window, and protocols finish
   mutating themselves there — asyncpg assigns `self.statement` *after* the bytes
   are on the wire. `write()` claims the connection for the writing task and the
   claim is handed back when it suspends. See `_transports.py`.

(2) and (3) are not documented asyncio guarantees; they are emergent properties
of a single-threaded loop that library authors were entitled to rely on. Both
were found the hard way — (2) as `InternalClientError: cannot switch to state 12`
and (3) as a segfault in `_decode_row`, in asyncpg, above four workers.

Parallelism therefore comes from having **many** connections, not from splitting
one. What is still *not* restored is any ordering between a connection's
callbacks and tasks that touch it without writing to it; a library that shares
mutable state across tasks is on its own (see §6).

---

## 2. Requirements

- **Free-threaded CPython (3.14t+).** Parallel stepping requires the GIL to be
  disabled; mt_asyncio only runs on free-threaded builds anyway.
- **The runtime is created with `context=True`** (handled internally) so
  `contextvars` — and therefore `current_task()` / `get_running_loop()` — are
  copied per task and isolated across the tree, matching asyncio semantics.
- **One runtime per process.** `run()` reuses a process-wide, reference-counted
  runtime, so multiple `run()` calls and multiple loops share it.

---

## 3. Supported API

### Running
`run`, `EventLoop`, `new_event_loop`, `get_event_loop`, `get_running_loop`,
`loop.run_until_complete`, `run_forever`, `stop`, `close`.

### Tasks & futures
`Future`, `Task`, `create_task`, `ensure_future`, `current_task`, `all_tasks`,
`isfuture`, `wrap_future`, `run_coroutine_threadsafe`, `iscoroutine`,
`iscoroutinefunction`. Task supports `cancel`/`cancelling`/`uncancel`,
`get_name`/`set_name`, `get_coro`, `get_context`, `get_stack`.

### Combinators
`gather` (ordered, `return_exceptions`, first-error propagation, outer-cancel
cancels children), `wait` (`FIRST_COMPLETED`/`FIRST_EXCEPTION`/`ALL_COMPLETED`),
`wait_for`, `shield` (inner survives outer cancel), `as_completed`, `sleep`,
`to_thread`, `TaskGroup`, `timeout`, `timeout_at`, `Timeout`.

### Synchronization
`Lock`, `Event`, `Condition`, `Semaphore`, `BoundedSemaphore`, `Queue`,
`LifoQueue`, `PriorityQueue`, `QueueEmpty`, `QueueFull`. **All are cancel-safe**
(a cancelled waiter cleanly removes itself; no deadlock/permit-leak) and built on
the multi-threaded Future.

### Loop services
`create_future`, `create_task`, `call_soon`, `call_soon_threadsafe`,
`call_later`, `call_at`, `time`, `run_in_executor` (→ mt_asyncio blocking pool),
`set_default_executor`, `getaddrinfo`, `getnameinfo`,
`sock_recv`/`sock_recv_into`/`sock_sendall`/`sock_connect`/`sock_accept`
(cancellable, on mt_asyncio's edge-triggered reactor), exception handler hooks,
`shutdown_asyncgens`, `shutdown_default_executor`, `set_debug`/`get_debug`.

Cancellation is delivered cooperatively at every `await` (bugfixed: bare
`await future` is cancellable, not only helper-wrapped awaits), and cleanup that
itself `await`s works — unlike the runtime's native `abort()`, which poisons cleanup.

---

## 4. Behavioral differences (subtle but supported)

| Area | Difference |
|---|---|
| Callback threading | Callbacks/task steps run on multiple threads, not serialized (§1). |
| `call_soon_threadsafe` | Identical to `call_soon`; no self-pipe / no `_check_thread`. |
| `run_until_complete` | Leaves the runtime hot; other tasks keep progressing across calls rather than pausing. |
| `CancelledError` | Ours is `asyncio.CancelledError`. A stray `mt_asyncio._mt_asyncio.CancelledError` from a raw mt_asyncio primitive is a distinct type; stay within `mt_asyncio.asyncio` primitives so cancellation is always `asyncio.CancelledError`. |
| Awaitables | Only awaitables from this package are drivable: a coroutine may yield a mt_asyncio `Waiter` or `None`, nothing else. A stock `asyncio.Future` (which yields `self`) cannot be awaited here — the runtime throws `TypeError` in at the suspension point, so it propagates like any other exception and surfaces on the task. |
| Introspection | `future_add_to_awaited_by` / call-graph tools are absent, so `asyncio.graph`-style task-parent introspection is unavailable. |
| `get_stack` | Best-effort; a task mid-step on a worker has a racy frame (accurate only while suspended). |
| `TaskGroup.create_task` | Children start running the moment they are created, in parallel with the code creating them. If an early child fails while the parent is still in its `for ... tg.create_task(...)` loop, the group is already aborting and the next `create_task` raises `RuntimeError('TaskGroup is shutting down')`. CPython cannot hit this: its children only start once the parent suspends. Create children that do not fail before the loop finishes, or spawn them from a single already-suspended point. |

`tests/test_parity.py` runs every scenario in this document's supported surface
against both stdlib `asyncio` and `mt_asyncio.asyncio` and asserts they agree, so a
divergence that is not listed here is a bug.

---

## 5. Not implemented

Categorized, with rationale and the practical workaround.

### High-level networking (streams / transports / protocols)
Missing: `open_connection`, `open_unix_connection`, `start_server`,
`start_unix_server`, `StreamReader`, `StreamWriter`, `StreamReaderProtocol`,
`Transport`/`ReadTransport`/`WriteTransport`/`BaseTransport`,
`Protocol`/`BaseProtocol`/`BufferedProtocol`, `DatagramProtocol`/`DatagramTransport`,
`Server`, `AbstractServer`, `loop.create_connection`, `loop.create_server`,
`loop.add_reader`/`add_writer`.

- **Why:** the entire transport/protocol stack sits on `add_reader`/`add_writer`,
  which are not implemented. This is the largest single gap.
- **Workaround:** use the low-level `sock_*` methods (fully supported and
  cancellable) for socket I/O.

### Subprocesses
Missing: `create_subprocess_exec`, `create_subprocess_shell`,
`SubprocessProtocol`, `SubprocessTransport`, `loop.subprocess_exec/shell`.
- **Workaround:** `await to_thread(subprocess.run, ...)`.

### Barrier
Missing: `Barrier`, `BrokenBarrierError`.
- **Why:** not yet ported (generation-based rebuild over `Event`). Straightforward
  follow-up.
- **Workaround:** a `Semaphore`/`Event` handshake, or `TaskGroup` join.

### Loop base classes & policy
Missing: `AbstractEventLoop`, `BaseEventLoop`, `SelectorEventLoop`, `Runner`,
`get_event_loop_policy`, `set_event_loop_policy`, `set_event_loop`, `Handle`,
`TimerHandle` (the last two exist internally in `_loop.py`, just not exported).
- **Why:** the mt loop is its own `EventLoop`, not a `BaseEventLoop` subclass;
  policies are deprecated in 3.14. `run()` covers `Runner`'s role.
- **Workaround:** use `mt_asyncio.asyncio.run` / `new_event_loop` directly.

### Introspection / debugging
Missing: `capture_call_graph`, `format_call_graph`, `print_call_graph`,
`future_add_to_awaited_by`, `future_discard_from_awaited_by`,
`FrameCallGraphEntry`, `FutureCallGraph`.
- **Why:** the C `_asyncio` awaited-by graph is not maintained for our tasks.

### Eager tasks
Missing: `create_eager_task_factory`, `eager_task_factory`.
- **Why:** they close over the C `Task`; our tasks always start on the runtime.

### Stream/misc exceptions
Missing: `IncompleteReadError`, `LimitOverrunError`, `SendfileNotAvailableError`,
`QueueShutDown` (+ `Queue.shutdown()`).

---

## 6. The drop-in boundary, and compat mode

`import mt_asyncio.asyncio as asyncio` parallelizes code that uses **these**
names. A third-party library does `import asyncio` internally and gets CPython's.

### Why that is a problem — and what the problem is *not*

Not the C `Future`/`Task`. On free-threaded 3.14 the current-task slot lives in
**thread state**, so `_enter_task(loop, task)` from a second thread on the same
loop is allowed, and `_asyncio.Future` is internally locked (16k
`add_done_callback` calls racing `set_result` across 5 threads lose and duplicate
nothing).

The problem is the pure-Python layer above them, which has no synchronisation:

| module | `threading.Lock` references |
| --- | --- |
| `asyncio.tasks` | 0 |
| `asyncio.locks` | 0 |
| `asyncio.taskgroups` | 0 |

`gather._done_callback` does an unsynchronised `nfinished += 1`; lose one
increment and the outer future never resolves. `Lock.acquire` check-then-sets
`_locked` over a bare `deque`. Measured on a loop dispatching `call_soon` across
8 threads, 4 runs each: `gather` + `wait_for` deadlocked 3/4, `asyncio.Lock`
failed 4/4 with `RuntimeError: Set changed size during iteration`,
`asyncio.Queue` deadlocked 1/4. These are **hangs and corruption**, not wrong
return values.

This package reimplements exactly those primitives with locks, which is why they
work here.

### `compat.install()`

```python
import mt_asyncio.asyncio as asyncio

asyncio.compat.install()   # FIRST, before importing anything that should see it

import some_library
asyncio.run(some_library.main())
```

One mechanism: **shadowing**. `gather`, `wait`, `wait_for`, `shield`, `sleep`,
`to_thread`, `Task`, `Future`, `TaskGroup`, `timeout`/`timeout_at`, `Lock`,
`Event`, `Condition`, `Semaphore`, `BoundedSemaphore`, the `Queue` family,
`create_task`, `current_task`, `all_tasks`, `ensure_future`, `as_completed`,
`get_running_loop`, `get_event_loop`, `run`, `run_coroutine_threadsafe` and
`wrap_future` are rebound in `asyncio` and in the submodules that re-export them
(`asyncio.locks`, `asyncio.tasks`, …). `CancelledError`, `InvalidStateError` and
`TimeoutError` are deliberately untouched — we re-export the stdlib objects, so
patching would be a no-op.

Nothing more is needed, and that is a consequence of the install-first rule.
With it, every route to a genuine stdlib future is closed: `Future`,
`asyncio.futures.Future` and the `asyncio.tasks` internals are all rebound,
subclassing after install subclasses ours, and no stdlib `Task` is ever
constructed because `Task` is shadowed too. So the runtime does **not** learn to
park on foreign awaitables — awaiting one stays a `TypeError` naming the problem,
which is a better outcome than parking on something we cannot wake. The only
construct that would slip past shadowing is a hand-rolled
`_asyncio_future_blocking` awaitable; that pattern appears nowhere in the stdlib
outside `asyncio` itself.

Caveats: patching is **process-global** (`compat.uninstall()` reverses it, and
`compat.installed()` is a context manager for tests), and ordering matters —
install before importing anything that should see it.

### What compat does not fix: shared state across tasks

Transports *are* implemented — `add_reader`/`add_writer`, `create_connection`,
`create_server`, `start_tls` and the streams layer all work, and
`tests/test_network.py` runs 48 scenarios against both backends. What compat
cannot fix is one layer up.

The locking in `_transports.py`, `_tls.py` and `_streams.py` makes a *connection*
safe: reads, writes, teardown and the TLS state machine are serialised per
connection. It does nothing for state a library keeps *across* connections, and
that turns out to be the real boundary. Two examples, both measured:

```python
# aiohttp/web_server.py:66          # websockets/asyncio/server.py:469
for conn in self._connections:      for connection in self.handlers
```

Both iterate a registry during shutdown while it is being mutated elsewhere —
aiohttp from a task done-callback (`web_server.py:50`), websockets from the
connection handler's `finally` (`server.py:395`). Under stdlib asyncio neither
can happen, because tasks and callbacks share one thread. Here they run at once.

Measured over 12 runs each: aiohttp 3 failures (`RuntimeError: dictionary changed
size during iteration`), websockets 1 hang — the same exception, but raised
inside an un-awaited `close_task`, so `closed_waiter` is never resolved and
`wait_closed()` blocks forever.

**No lock we can add fixes this.** In both cases one side of the race is ordinary
library code running in a task. A lock held during protocol callbacks is held by
neither participant, and serialising tasks would mean giving up the parallelism
this project exists for. Libraries that share mutable state between tasks need
their own locks — the same conclusion as §6, applied one level up.

Requests themselves are reliable: aiohttp served every request correctly in all
12 runs, over both HTTP and HTTPS, and only its *shutdown* races.

`anyio` (and therefore `httpx`) is a separate case: its cancel-scope machinery
re-schedules `_deliver_cancellation` via `call_soon` until it observes the target
cancelled, and our cooperative cancellation does not satisfy that loop, so it
livelocks. Not supported.

---

## 7. Performance characteristics

Measured with `bench/asyncio_bench.py` (or `make bench`), comparing **stdlib
asyncio** (single-core by design) against **`mt_asyncio.asyncio`** running the
byte-for-byte same coroutines. Each configuration runs in a fresh process;
figures are the median of 3 measured runs after a warmup.

Environment: free-threaded CPython 3.14.4, mt_asyncio 0.8.3 (**release** build), Apple
M5 Max — 18 CPUs, but only **6 of them are performance cores** (12 are efficiency
cores). Past 8 threads the runtime is scheduling onto efficiency cores, which is
why several workloads flatten between 8 and 16 threads rather than continuing to
climb. Absolute numbers are machine-specific; re-run before quoting. Run against
a release build — a debug build is several times slower, and the bench scripts
refuse to run on one.

Full speedup matrix vs stdlib asyncio:

| workload | 1t | 2t | 4t | 8t | 16t | verdict |
|---|---|---|---|---|---|---|
| `mixed` (CPU between awaits) | 1.21× | 2.34× | 3.43× | 6.42× | **7.34×** | scales — the headline case |
| `wait_for_overhead` (each unit under `wait_for`) | 0.97× | 1.86× | 3.42× | 3.52× | **6.59×** | scales — the `wait_for` scaffolding is not the bottleneck |
| `taskgroup_fanout` (fan-out via `TaskGroup`) | 1.08× | 1.85× | 3.60× | 4.22× | **5.72×** | scales |
| `semaphore_bounded` (bounded concurrency + CPU) | 0.97× | 2.11× | 3.84× | **5.52×** | 4.75× | scales |
| `gather_fanout` (fan-out + CPU) | 1.00× | 1.74× | 2.27× | 3.38× | **5.32×** | scales |
| `sock_work` (I/O + per-request CPU — real server handler) | 1.00× | 1.79× | 3.30× | 4.44× | **5.28×** | scales |
| `queue_pipeline` (producer→`Queue`→consumers) | 1.02× | 1.84× | 2.88× | **3.88×** | 3.14× | scales to 8, then the queue handoff dominates |
| `lock_contention` (one shared `Lock`) | 0.95× | 1.78× | **3.01×** | 2.48× | 2.82× | partial — the critical section serializes by construction; only work outside the lock parallelizes |
| `churn` (bare awaits, no work) | 1.41× | 1.23× | 1.29× | 1.42× | **1.60×** | faster than stdlib even with nothing to parallelize |
| `executor_offload` (`to_thread`) | 0.98× | 1.24× | 1.21× | **1.24×** | 1.17× | ~parity — both pools are genuinely parallel on a free-threaded build, so this is pure dispatch cost |
| `sock_echo` (socketpair ping-pong, no work) | 0.72× | 0.83× | **1.16×** | 0.99× | 1.15× | latency-bound; each `recv` pays a cross-thread reactor hop, so it only reaches parity |
| `timer_sleep` (many real `sleep`s) | 0.51× | 0.47× | **0.55×** | 0.43× | 0.38× | **loses** — see below |
| `cancel_heavy` (create+cancel+unwind) | **0.62×** | 0.41× | 0.38× | 0.27× | 0.20× | **loses**, and degrades with threads — cancellation is all coordination |

The I/O pair is the headline for servers: a handler doing per-request work
(`sock_work`) reaches 5.3× — one loop, many connections, request processing
across cores, which a single stdlib loop cannot do at all. The same code path
with *no* per-request work (`sock_echo`) only reaches parity, because there is
nothing to parallelize and every round-trip crosses the reactor.

**Read this honestly.** mt_asyncio is at rough parity with stdlib per thread and wins
by parallelizing, so the more real work a task does between awaits, the better it
does. Two areas genuinely lose:

- **Timers.** `sleep()` builds a `Future`, a `TimerHandle`, a contextvars copy
  and a *helper coroutine* that awaits a native waiter, then hops through a
  callback to resolve the future — where the runtime underneath needs a single
  native waiter with a timeout. `bench/tonio_regression.py` prices this at ~2×
  versus TonIO's native `sleep` on the same core. It is scaffolding, not an
  inherent cost, and is the clearest optimization target in the layer.
- **Cancellation.** `cancel_heavy` is pure coordination with no work to spread,
  so more threads make it *worse*.

Caveat on reading the table: the worst run-to-run spread was ~22%
(`cancel_heavy`, `gather_fanout@4t`, `sock_work@4t`), so single cells within
~20% of each other are not distinguishable.

### Versus upstream TonIO

mt_asyncio kept TonIO's Rust core, so "did the fork slow the core down?" is a separate
question from the table above. `bench/tonio_regression.py` drives both `.so`s
through their raw primitives with byte-for-byte identical Python
(tonio 0.8.3 from PyPI, both release builds):

| core workload | 1t | 4t | 8t |
|---|---|---|---|
| `yield_churn` (one coroutine, N suspensions) | **1.37×** | **1.68×** | **1.48×** |
| `spawn_join` (spawn N, join all) | **1.42×** | **1.54×** | **1.56×** |
| `offload` (blocking-pool dispatch) | **1.13×** | **1.12×** | **1.10×** |
| `sleep_timers` (real timers) | 1.00× | 1.09× | 0.88× |
| `cpu_scale` (CPU between suspensions) | 0.99× | 0.94× | 1.00× |

(> 1.00× means mt_asyncio is faster.) No regression: the suspend/resume and spawn
paths got faster, and workloads bounded by something other than the core sit at
parity. The lone sub-1.0 cell had 63% spread and is noise.
