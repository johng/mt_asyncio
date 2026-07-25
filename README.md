# mt_asyncio

mt_asyncio is a **parallel asyncio runtime for free-threaded Python**: one event loop
that steps asyncio tasks across many OS threads, written in Rust on top of the
[mio crate](https://github.com/tokio-rs/mio).

```python
import mt_asyncio.asyncio as asyncio

async def handle(n):
    await asyncio.sleep(0.1)
    return n * 2

async def main():
    return await asyncio.gather(*[handle(i) for i in range(1000)])

asyncio.run(main())
```

That is the whole idea: `import mt_asyncio.asyncio as asyncio` and your existing
coroutines run on more than one core.

> **Experiment:** this is an experiment in whether asyncio can be made genuinely
> parallel on free-threaded Python — not production software. Releases are alpha
> and the APIs are subject to breaking changes.

> **Attribution:** mt_asyncio is derived from [TonIO](https://github.com/gi0baro/tonio)
> by Giovanni Barillari and keeps its Rust runtime core; TonIO's own `yield`- and
> `async`-flavoured APIs are not exposed here. mt_asyncio is not affiliated with or
> endorsed by the TonIO project. See [NOTICE](NOTICE).

> **Note:** free-threaded Python (3.14t+) and Unix systems only.

## Why

CPython's asyncio loop is single-threaded by design: one thread steps every
task, so an async workload cannot use more than one core no matter how many
tasks it has. On free-threaded Python that limit is no longer necessary.

mt_asyncio keeps asyncio's API and semantics but replaces the scheduler: tasks are
handed to a Rust work-stealing runtime and stepped **in parallel** on its worker
threads. I/O readiness comes from an edge-triggered `mio` reactor, and
`run_in_executor` from a native blocking thread-pool.

The trade-off is stated up front, because it is the one thing that changes:
**callbacks are no longer serialized**. Stdlib asyncio gives you implicit mutual
exclusion (everything runs on the loop thread); mt_asyncio does not. Shared state
touched from tasks or callbacks needs a lock — the ones in `mt_asyncio.asyncio` are
genuinely cross-thread. This is the price of using multiple cores.

## Install

```
pip install --pre mt-asyncio
```

Releases are alpha for now, so `--pre` is required until the first stable one.

Requires a free-threaded CPython build (`python3.14t` or `python3.15t`).

## Usage

Everything lives in `mt_asyncio.asyncio`, which mirrors the `asyncio` namespace:

```python
import mt_asyncio.asyncio as asyncio

async def worker(queue):
    while True:
        item = await queue.get()
        if item is None:
            return
        await process(item)

async def main():
    queue = asyncio.Queue(maxsize=100)

    async with asyncio.TaskGroup() as tg:
        for _ in range(8):
            tg.create_task(worker(queue))

        async for item in source():
            await queue.put(item)
        for _ in range(8):
            await queue.put(None)

asyncio.run(main())
```

Supported: `run`, `create_task`, `gather`, `wait`, `wait_for`, `shield`,
`as_completed`, `sleep`, `to_thread`, `TaskGroup`, `timeout`/`timeout_at`,
`Future`, `Task` (with `cancel`/`cancelling`/`uncancel`), `Lock`, `Event`,
`Condition`, `Semaphore`, `BoundedSemaphore`, `Queue`/`LifoQueue`/`PriorityQueue`,
`run_coroutine_threadsafe`, `wrap_future`, and the loop's `call_soon`,
`call_later`, `call_at`, `run_in_executor`, `getaddrinfo`, and `sock_*` methods.

Networking: `add_reader`/`add_writer`, `create_connection`, `create_server`,
`start_tls`, `open_connection`/`start_server` and TLS.

Not implemented: datagram endpoints, subprocesses, `Barrier`, and loop
policies. See [`mt_asyncio/asyncio/COMPATIBILITY.md`](mt_asyncio/asyncio/COMPATIBILITY.md)
for the full surface, the behavioural differences, and the workarounds.

### Third-party libraries

There are two ways to reach a database here, and which one fits depends on the
shape of your concurrency rather than on what is supported.

**Async driver.** psycopg's async API works under `compat.install()` — it waits
on `add_reader`/`add_writer`, which are implemented. Its `wait_async` loop is
covered in `tests/test_netlibs.py` against both backends, and concurrent queries
across many connections are verified against a real server.

```python
import mt_asyncio.asyncio as asyncio

asyncio.compat.install()   # before importing psycopg

import psycopg

async def fetch_user(conn, user_id):
    async with conn.cursor() as cur:
        await cur.execute('select name from users where id = %s', (user_id,))
        return await cur.fetchone()

async def main():
    async with await psycopg.AsyncConnection.connect('postgresql:///app') as conn:
        return await fetch_user(conn, 1)

asyncio.run(main())
```

No thread per query, so concurrency is bounded only by your connection pool. The
cost is a reactor hop per round trip.

**Sync driver behind `to_thread`.** One pool thread per in-flight query, capped
by `blocking_threadpool_size` (128 by default), and no event-loop work in the
query path at all. This is also the only option for clients with no async API —
`requests`, `boto3`, and plenty of vendor SDKs.

```python
import mt_asyncio.asyncio as asyncio
from psycopg_pool import ConnectionPool

pool = ConnectionPool('postgresql:///app', min_size=16, max_size=16)

async def fetch_user(user_id):
    def query():
        with pool.connection() as conn:
            return conn.execute('select name from users where id = %s', (user_id,)).fetchone()

    return await asyncio.to_thread(query)

async def main():
    return await asyncio.gather(*[fetch_user(i) for i in range(1000)])

asyncio.run(main())
```

`to_thread` hands `query` to the runtime's native blocking pool. Note this is a
better `to_thread` than the stdlib one rather than a fallback from it: on a
free-threaded build those threads run Python *concurrently*, so the queries are
genuinely in flight at once instead of taking turns under the GIL — and the
coroutines awaiting them are stepped in parallel too. Size the connection pool,
or bound it with a `Semaphore`, so you do not queue more work than the database
can take.

The `db_query` benchmark measures the `to_thread` pattern; point
`MT_ASYNCIO_BENCH_DSN` at a real server to run it against Postgres.

### Blocking calls

Blocking directly in a coroutine is survivable here, which it is not under
stdlib: it occupies **one worker**, and the others keep stepping tasks.

```python
async def handler():
    time.sleep(0.05)      # occupies a worker for 50ms, not the whole loop
    return 'done'
```

There are two ways to make that safe, and on a free-threaded build both are
legitimate — the choice is about failure modes, not speed.

**Offload it.** `await asyncio.to_thread(...)` moves the call to the blocking
pool and keeps every worker free.

**Or just have more workers.** Blocked threads are off-CPU, so raising `threads`
costs little. Measured with 64 concurrent 10ms blocking calls:

| | 8 workers | 128 workers |
| --- | --- | --- |
| blocking inline | 102.6 ms | **16.7 ms** |
| via `to_thread` | 14.9 ms | 18.5 ms |
| CPU-bound workload | 724.8 ms | **362.4 ms** |

At 128 workers, inline blocking matches `to_thread`, and CPU work did not suffer
from oversubscription on an 18-core machine.

What the split still buys is **isolation and elasticity**, not throughput:

- Exhaust the blocking pool and offloads simply queue — the scheduler keeps
  running. Exhaust the workers and the runtime stalls, because workers are also
  what run task steps, timers and I/O dispatch. Raising `threads` moves that
  cliff without removing it.
- Pool threads are created on demand and retire after
  `blocking_threadpool_idle_ttl`; workers are created at startup and live for the
  life of the process.

The cost of the split is that the two pools cannot help each other: a blocking
pool thread never steps a coroutine, and a worker never picks up offloaded work.

The cliff is worth seeing, because it is silent. With 4 workers, a 50ms blocking
call, and an unrelated task watching how long it is kept off the CPU:

| concurrent blockers | throughput | worst stall elsewhere |
| --- | --- | --- |
| 3 (`threads - 1`) | unaffected | 1 ms |
| 4 (`threads`) | *still* unaffected | **51 ms** |

At exactly the worker count the wall clock looks fine while everything else
freezes for the full duration. The default `threads` is `cpu_count() + 4` for
this reason — headroom so ordinary blocking does not reach the edge. Use
`to_thread` when you want that guarantee rather than a margin, and raise
`threads` when your workload is mostly blocking and you would rather not
partition your threads at all.

### Sizing the runtime

`run()` takes a `threads` argument, and `mt_asyncio.runtime()` configures the
process-wide runtime up front:

```python
import mt_asyncio
import mt_asyncio.asyncio as asyncio

# 8 worker threads, a smaller blocking pool
mt_asyncio.runtime(threads=8, blocking_threadpool_size=32, context=True)

asyncio.run(main())
```

| option | description | default |
| --- | --- | --- |
| `threads` | runtime worker threads (scheduler *and* execution) | # of CPU cores + 4 |
| `context` | propagate `contextvars` into coroutines (required by the loop) | `False` |
| `blocking_threadpool_size` | maximum blocking threads | 128 |
| `blocking_threadpool_idle_ttl` | idle timeout for blocking threads (seconds) | 30 |
| `signals` | signals the runtime listens for | |

### The drop-in boundary

`import mt_asyncio.asyncio as asyncio` parallelizes code that uses **those** names.
A third-party library does `import asyncio` internally and gets CPython's.

The C `Future`/`Task` are not the obstacle — on 3.14t the current-task slot lives
in thread state and `Future` is internally locked. The problem is the
*pure-Python* layer above them, which has no synchronisation at all:
`gather._done_callback` does an unsynchronised `nfinished += 1`, so one lost
increment means the outer future never resolves, and `Lock.acquire` check-then-sets
`_locked` over a bare deque. Those failures are **hangs**, not wrong answers.

Compat mode points those names at mt_asyncio's own locked implementations:

```python
import mt_asyncio.asyncio as asyncio

asyncio.compat.install()   # before importing libraries that should see it

import some_library
asyncio.run(some_library.main())
```

`gather`, `wait_for`, `TaskGroup`, `Lock`, `Event`, `Queue`, `Future`,
`create_task` and the rest are redirected, in `asyncio` and in the submodules that
re-export them. Install **before** importing anything that should see it —
shadowing only rebinds module attributes, so a module that already did
`from asyncio import Lock` keeps the original. Patching is process-global;
`compat.uninstall()` reverses it.

Transports are implemented — `add_reader`/`add_writer`, `create_connection`,
`create_server`, `start_tls` and streams all work, over TLS as well as plain TCP.
psycopg's async API, aiohttp and websockets all drive them correctly.

The remaining boundary is one level up, and no lock can close it: a library that
shares mutable state **between tasks** can race, because tasks step in parallel.
Both aiohttp and websockets keep a registry of live connections and iterate it
during server shutdown while another task mutates it, so their shutdown paths are
unreliable here even though serving traffic is not. `anyio` (and therefore
`httpx`) livelocks against our cooperative cancellation and is unsupported. See
[`COMPATIBILITY.md`](mt_asyncio/asyncio/COMPATIBILITY.md) for the measurements.

## Compatibility testing

`tests/test_parity.py` is a differential suite: every scenario is written once
and run against **both** stdlib `asyncio` and `mt_asyncio.asyncio`, asserting the same
observable outcome. A divergence that is not listed in `COMPATIBILITY.md` is
treated as a bug.

```
make test
```

## Performance

Speedup versus stdlib asyncio on the same coroutines (free-threaded CPython
3.14.4, Apple M5 Max — 6 performance + 12 efficiency cores; median of 3 runs via
`bench/asyncio_bench.py`, or `make bench`):

| workload | 1 thread | best |
| --- | --- | --- |
| CPU work between awaits | 1.21× | 7.34× @16 |
| each unit under `wait_for` | 0.97× | 6.59× @16 |
| fan-out with `TaskGroup` | 1.08× | 5.72× @16 |
| server handler (I/O + per-request work) | 1.00× | 5.28× @16 |
| producer → queue → consumers | 1.02× | 3.88× @8 |
| one shared `Lock` | 0.95× | 3.01× @4 |
| pure scheduling, no per-task work | 1.41× | 1.60× @16 |
| many real `sleep()` timers | 0.51× | 0.55× @4 |
| create + cancel + unwind | 0.62× | 0.62× @1 |

Read that honestly. mt_asyncio is at rough parity per-thread and wins by
parallelizing, so the more real work a task does between awaits, the better it
does. Two things are genuinely slower: **timers** — each `sleep()` builds a
`Future`, a `TimerHandle` and a helper coroutine where the runtime underneath
needs only one native waiter — and **cancellation**, which is pure coordination
and gets *worse* with more threads. Past 6 threads this machine is scheduling
onto efficiency cores, so the 16-thread column is not 16 equal cores.

Benchmarks must be run against a release build (`make build-release`); the
scripts refuse to run on a debug one, which is several times slower.

## License

mt_asyncio is released under the BSD-3-Clause License, the same license as TonIO,
whose copyright notice it retains. Ported CPython code is additionally covered
by the PSF License Agreement. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
