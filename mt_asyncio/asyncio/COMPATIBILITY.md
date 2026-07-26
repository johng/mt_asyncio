# `mt_asyncio.asyncio` — asyncio compatibility & gaps

`mt_asyncio.asyncio` is a single asyncio-compatible event loop that runs tasks in
**true parallel** across mt_asyncio's worker threads on free-threaded CPython. It
presents the asyncio API so that

```python
import mt_asyncio.asyncio as asyncio
```

is a drop-in for code written against `asyncio`. This document records exactly
what is supported, what behaves differently, and what is not implemented.

Coverage of the public `asyncio` namespace: **49 / 119 names**. TCP and TLS
transports, streams and servers are covered; what is missing is Unix-domain
sockets, datagrams and pipes, subprocesses, and introspection and loop-policy
plumbing — see [Not implemented](#not-implemented).

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

### Networking (TCP and TLS)
`open_connection`, `start_server`, `StreamReader`, `StreamWriter`,
`StreamReaderProtocol`, `Server`, `loop.create_connection`, `loop.create_server`,
`loop.start_tls`, and `loop.add_reader`/`add_writer`/`remove_reader`/`remove_writer`
(level-triggered and persistent, as asyncio's contract requires — see `_loop.py`).
`happy_eyeballs_delay` is accepted and ignored, so connection attempts are made
sequentially; that is a latency difference, not a behavioural one.

Transport and protocol *base classes* are not re-exported and do not need to be:
our transports subclass `asyncio.Transport`, our `Server` subclasses
`asyncio.AbstractServer` and our `StreamReader` subclasses `asyncio.StreamReader`,
so `asyncio.Protocol` subclasses and `isinstance` checks against the stdlib ABCs
work unchanged.

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

### Unix-domain sockets, datagrams and pipes
Missing: `open_unix_connection`, `start_unix_server`,
`loop.create_unix_connection`/`create_unix_server`,
`loop.create_datagram_endpoint`, `DatagramProtocol`/`DatagramTransport`,
`loop.connect_read_pipe`/`connect_write_pipe`, `loop.sendfile`,
`SendfileNotAvailableError`. Each raises `NotImplementedError` from
`AbstractEventLoop`, which is the failure we want.

- **Why:** TCP is what the transport layer was built and measured against; each
  of these is a separate registration path over the same reactor, not yet ported.
  This is the largest remaining gap.
- **Workaround:** the low-level `sock_*` methods are fully supported and
  cancellable, and work on any socket — including `AF_UNIX` and datagram sockets.

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
  policies are deprecated in 3.14. `run()` covers `Runner`'s role. `EventLoop`
  *does* subclass `asyncio.AbstractEventLoop`, so third-party `isinstance` checks
  pass — only the name is not re-exported.
- **Workaround:** use `mt_asyncio.asyncio.run` / `new_event_loop` directly.

### Introspection / debugging
Missing: `capture_call_graph`, `format_call_graph`, `print_call_graph`,
`future_add_to_awaited_by`, `future_discard_from_awaited_by`,
`FrameCallGraphEntry`, `FutureCallGraph`.
- **Why:** the C `_asyncio` awaited-by graph is not maintained for our tasks.

### Eager tasks
Missing: `create_eager_task_factory`, `eager_task_factory`.
- **Why:** they close over the C `Task`; our tasks always start on the runtime.

### Queue shutdown
Missing: `QueueShutDown` and `Queue.shutdown()`.

### Stream exceptions: raised, not re-exported
`IncompleteReadError` and `LimitOverrunError` are **not** in this namespace, but
they are what our streams raise — the `StreamReader` parsing layer is CPython's,
reused untouched. Catch them as `asyncio.IncompleteReadError` /
`asyncio.LimitOverrunError`; `mt_asyncio.asyncio.IncompleteReadError` is an
`AttributeError`.

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

Transports are implemented — `add_reader`/`add_writer`, `create_connection`,
`create_server`, `start_tls` and the streams layer all work, and
`tests/test_network.py` runs 29 scenarios against both backends. What compat
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
their own locks — the same conclusion as §1, applied one level up.

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

Environment: free-threaded CPython 3.14.4, mt_asyncio **release** build, Apple
M5 Max — 18 CPUs, but only **6 of them are performance cores** (12 are efficiency
cores). Past 8 threads the runtime is scheduling onto efficiency cores, which is
why several workloads flatten between 8 and 16 threads rather than continuing to
climb. Absolute numbers are machine-specific; re-run before quoting. Run against
a release build — a debug build is several times slower, and the bench scripts
refuse to run on one.

Full speedup matrix vs stdlib asyncio, all 20 workloads:

| workload | 1t | 2t | 4t | 8t | 16t | verdict |
|---|---|---|---|---|---|---|
| `block_inline` (blocking call made in the coroutine) | 1.01× | 2.00× | 3.91× | 7.44× | **14.31×** | scales hardest — stdlib stalls its one loop thread for the whole call, we stall one worker of many |
| `gather_fanout` (fan-out + CPU) | 0.98× | 1.76× | 3.44× | 4.79× | **7.08×** | scales |
| `taskgroup_fanout` (fan-out via `TaskGroup`) | 0.99× | 1.86× | 3.07× | 3.41× | **6.67×** | scales |
| `mixed` (CPU between awaits) | 1.04× | 2.09× | 3.89× | 5.50× | **6.37×** | scales — the headline case |
| `wait_for_overhead` (each unit under `wait_for`) | 1.00× | 1.93× | 3.12× | 4.37× | **5.86×** | scales — the `wait_for` scaffolding is not the bottleneck |
| `sock_work` (I/O + per-request CPU — real server handler) | 0.99× | 1.80× | 3.26× | 3.70× | **5.47×** | scales |
| `stream_work` (transport + streams + per-request CPU) | 0.88× | 1.69× | 3.02× | 3.94× | **5.27×** | scales — the per-connection lock is not the ceiling |
| `tls_work` (same over TLS) | 0.91× | 1.74× | 3.19× | 4.15× | **5.27×** | scales — `sslproto` under the connection lock costs ~nothing extra |
| `semaphore_bounded` (bounded concurrency + CPU) | 1.01× | 1.85× | 2.92× | **4.96×** | 4.89× | scales |
| `db_query` (sync driver via `to_thread`) | 0.99× | 1.75× | 2.73× | 3.52× | **4.91×** | scales — one OS thread per in-flight query |
| `queue_pipeline` (producer→`Queue`→consumers) | 1.03× | 1.71× | 3.08× | **3.22×** | 2.56× | scales to 8, then the queue handoff dominates |
| `lock_contention` (one shared `Lock`) | 0.98× | 1.76× | **2.97×** | 2.19× | 2.35× | partial — the critical section serializes by construction; only work outside the lock parallelizes |
| `block_offload` (same blocking call via `to_thread`) | 2.54× | 2.59× | **2.61×** | 2.60× | 2.51× | flat and ahead — both pools are real threads; the win is dispatch, not width |
| `db_query_async` (psycopg async API on our reactor) | 0.93× | 1.46× | 2.02× | **2.12×** | 2.09× | partial — a reactor hop per round trip caps it below `db_query` |
| `churn` (bare awaits, no work) | 1.65× | 1.21× | **1.71×** | 1.43× | 1.60× | faster than stdlib even with nothing to parallelize |
| `executor_offload` (`to_thread`) | 0.77× | 0.99× | **1.02×** | 1.00× | 1.01× | ~parity — pure dispatch cost |
| `sock_echo` (socketpair ping-pong, no work) | 0.67× | 0.80× | **0.95×** | 0.67× | 0.57× | latency-bound; each `recv` pays a cross-thread reactor hop, so it only reaches parity |
| `timer_sleep` (many real `sleep`s) | 0.54× | 0.51× | **0.63×** | 0.55× | 0.53× | **loses** — see below |
| `cancel_heavy` (create+cancel+unwind) | **0.57×** | 0.38× | 0.33× | 0.25× | 0.20× | **loses**, and degrades with threads — cancellation is all coordination |
| `stream_echo` (transport + streams ping-pong, no work) | 0.17× | 0.29× | 0.47× | 0.46× | **0.55×** | **loses worst** — see below |

The I/O pair is the headline for servers: a handler doing per-request work
(`sock_work`) reaches 5.5× — one loop, many connections, request processing
across cores, which a single stdlib loop cannot do at all. The same code path
with *no* per-request work (`sock_echo`) only reaches parity, because there is
nothing to parallelize and every round-trip crosses the reactor.

`db_query_async` needs `MT_ASYNCIO_BENCH_DSN`; without it that row is skipped and
the rest of the suite still runs.

**Read this honestly.** mt_asyncio is at rough parity with stdlib per thread and wins
by parallelizing, so the more real work a task does between awaits, the better it
does. Three areas genuinely lose:

- **Timers.** `sleep()` builds a `Future`, a `TimerHandle`, a contextvars copy
  and a *helper coroutine* that awaits a native waiter, then hops through a
  callback to resolve the future — where the runtime underneath needs a single
  native waiter with a timeout. `bench/tonio_regression.py` prices this at ~2×
  versus TonIO's native `sleep` on the same core. It is scaffolding, not an
  inherent cost, and is the clearest optimization target in the layer.
- **Cancellation.** `cancel_heavy` is pure coordination with no work to spread,
  so more threads make it *worse*.
- **Idle streams.** `stream_echo` is the worst cell in the suite: 55 µs per
  round-trip against stdlib's 10 µs. It is *not* the per-connection locking —
  disabling the connection claims and `defer_wakes` together recovers only ~3%
  (352 ms → 342 ms), and per-round-trip call counts are exactly 2.00 of each of
  `_arm`/`_dispatch`/`_read_ready`/`feed_data`/`write`, one per side, with no
  re-arm spinning. It is scheduling hops. `sock_echo` costs 17 µs on the same
  runtime because `sock_recv` parks the task *directly* on fd readiness — one
  hop. The transport path takes two: reactor → worker runs `_read_ready` →
  `data_received` → `feed_data` completes the reader's waiter → a second handle →
  a worker steps the reading task. With `cpu=0` there is nothing to weigh against
  those hops, which is exactly why `stream_work` (same path, real handler work)
  reaches 5.3×.

Caveat on reading the table: the median run-to-run spread was 3.5% across all 120
cells, but the worst reached ~29% (`executor_offload@16t`, `queue_pipeline@16t`,
`timer_sleep`'s stdlib baseline, `taskgroup_fanout@8t`). Single cells within ~25%
of each other are not distinguishable; re-run with `--repeat 9` before believing
any one of them.

### Versus upstream TonIO

mt_asyncio kept TonIO's Rust core, so "did the fork slow the core down?" is a separate
question from the table above. `bench/tonio_regression.py` drives both `.so`s
through their raw primitives with byte-for-byte identical Python
(tonio 0.8.3 from PyPI, both release builds):

| core workload | 1t | 4t | 8t |
|---|---|---|---|
| `spawn_join` (spawn N, join all) | **1.28×** | **1.54×** | **1.65×** |
| `yield_churn` (one coroutine, N suspensions) | **1.22×** | **1.33×** | **1.45×** |
| `offload` (blocking-pool dispatch) | **1.11×** | **1.07×** | **1.19×** |
| `sleep_timers` (real timers) | 1.00× | 1.15× | 1.07× |
| `cpu_scale` (CPU between suspensions) | 0.96× | 0.98× | 0.97× |

(> 1.00× means mt_asyncio is faster.) No regression in the scheduler: the
suspend/resume and spawn paths got faster — and by more at higher thread counts,
which is where a core change would show — and the timer and blocking-pool paths
sit at or above parity.

The exception is `cpu_scale`, consistently 0.96–0.98× at every thread count with
run-to-run spreads of only 1–4%, so it is a real few-percent deficit rather than
noise. It is also the one workload here that is CPU-bound rather than
core-bound — 256 tasks burning 100k units between four suspensions each — so what
it prices is mostly the interpreter doing arithmetic, not the runtime scheduling
it. Worth re-checking if it ever grows.
