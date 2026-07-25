# mt_asyncio benchmarks

Five independent benchmark families live here.

> **Build a release extension first.** `make build-dev` (plain `maturin develop`)
> produces a **debug** build that is several times slower than release, which
> silently invalidates every measurement. Run `make build-release` before
> benchmarking. Both `asyncio_bench.py` and `tonio_regression.py` refuse to run
> against a debug build (they check `mt_asyncio._mt_asyncio.__build_profile__`) unless you
> pass `--allow-debug-build`.

## 1. `asyncio_bench.py` — stdlib asyncio vs `mt_asyncio.asyncio`

The one that matters for the drop-in claim. Every workload is written **once**
against the shared asyncio API and run under both backends by passing in the
module, so `asyncio` and `mt_asyncio.asyncio` execute byte-for-byte the same
coroutines — the only difference is which loop steps them.

```bash
python bench/asyncio_bench.py --list           # what's in the suite
python bench/asyncio_bench.py                  # everything, 1/2/4/8 threads
python bench/asyncio_bench.py -w mixed churn sock_work
python bench/asyncio_bench.py --threads 1 4 8 16 --repeat 5
python bench/asyncio_bench.py --scale 0.1 --repeat 1 --warmup 0   # quick smoke
```

or `make bench` / `make bench-quick` (both take `BENCH_ARGS=...`).

Method: each `(backend, workload, threads)` configuration runs in a **fresh
subprocess**, because mt_asyncio's runtime is a per-process singleton and a thread
count only takes effect in a process that hasn't started one. Inside that
process the workload runs `--warmup` times unmeasured and `--repeat` times
measured; the table reports the median, and `--json` keeps every sample plus the
min and the spread so you can see whether a number is trustworthy.

The workloads deliberately cover both sides of the trade-off:

| kind | workloads | expectation |
| --- | --- | --- |
| real work between awaits | `mixed`, `gather_fanout`, `taskgroup_fanout`, `queue_pipeline`, `semaphore_bounded`, `wait_for_overhead`, `sock_work` | scales with threads |
| serialized by construction | `lock_contention` | only the work outside the lock scales |
| pure scheduling, no work | `churn`, `cancel_heavy`, `timer_sleep`, `sock_echo` | mt_asyncio's heavier per-task machinery shows, with nothing to parallelize |
| offload | `executor_offload`, `db_query` | blocking-pool dispatch cost |

`db_query` is the third-party-library case: async drivers can't be driven here,
so a **sync** driver is offloaded per query behind a semaphore that stands in for
a connection pool. By default a stand-in driver sleeps per query, which keeps the
dispatch shape without needing a server. To measure real Postgres:

```bash
pip install 'psycopg[pool]'
MT_ASYNCIO_BENCH_DSN=postgresql:///bench python bench/asyncio_bench.py -w db_query
```

`tests/test_bench_smoke.py` runs every workload at micro-scale under both
backends, so the suite can't silently rot between benchmark runs.

## 2. `tonio_regression.py` — mt_asyncio vs upstream TonIO

mt_asyncio forked TonIO, kept the Rust core and replaced the Python surface. This
answers "did we make the shared core slower?" — which needs care, because the
two projects no longer ship the same API.

```bash
uv sync --group bench                              # installs tonio from PyPI
python bench/tonio_regression.py                   # both tiers
python bench/tonio_regression.py --tier core       # just the regression test
```

or `make bench-regression`. Two tiers, and **only the first is a regression
test**:

- **core** — `tonio-core` vs `mt-asyncio-core` drive the Rust runtime through its raw
  primitives (`Runtime._spawn_*`, `Event`, `Waiter`, `Result`) with byte-for-byte
  identical Python; only the loaded `.so` differs, so a gap here is a real change
  in the core. It deliberately avoids TonIO's native `Barrier`/`Lock`/`Semaphore`
  (which mt_asyncio removed from Rust) so neither side gets a primitive the other
  lacks — `spawn_join` is a plain Python counter on both.
- **api** — `tonio-async` (`tonio.colored`) vs `mt-asyncio-async` (`mt_asyncio.asyncio`)
  compare the shipped APIs. These are **not** like-for-like: `mt_asyncio.asyncio`
  implements cancellation, contextvars, exception groups and the asyncio
  `Future` protocol that `tonio.colored` does not, so it is expected to cost
  more. `tonio-async-ctx` runs TonIO with `context=True` to price in the
  per-step contextvar copy mt_asyncio always pays, and is the fairer of the two.

Result (tonio 0.8.3 from PyPI vs this tree, both release builds, Apple M5 Max,
free-threaded 3.14.4, median of 3). Ratio > 1.00× means mt_asyncio is faster:

| workload | 1t | 4t | 8t |
| --- | --- | --- | --- |
| `yield_churn` (one coroutine, N suspensions) | 1.37× | 1.68× | 1.48× |
| `spawn_join` (spawn N, join all) | 1.42× | 1.54× | 1.56× |
| `offload` (blocking-pool dispatch) | 1.13× | 1.12× | 1.10× |
| `sleep_timers` (real timers) | 1.00× | 1.09× | 0.88× |
| `cpu_scale` (CPU between suspensions) | 0.99× | 0.94× | 1.00× |

**No core regression.** The suspend/resume and spawn paths came out faster —
consistent with the fork deleting the generator-based handle variants and the
per-worker scratch buffer from the hot path — and the workloads dominated by
something other than the core (`cpu_scale` is bounded by the Python CPU kernel)
sit at parity. The single sub-1.0 cell, `sleep_timers@8t`, had 63% run-to-run
spread and is noise; the rest were mostly under 20%.

The `mt-asyncio-async` rows do show a real cost for the asyncio layer on cheap
operations — `sleep_timers` at 1 thread is 52.9 ms vs TonIO's 22.1 ms, i.e. ~2.4×
— which is the price of asyncio semantics, not a core regression.

## 3. `pg_tax.py` — psycopg against a real Postgres, ours vs TonIO's

`tonio_regression.py` compares the two runtimes on synthetic primitives. This one
asks the same question where it is load-bearing: **a third-party driver on a real
socket**. Both projects can run psycopg, by opposite routes:

- **ours** — `compat.install()` shadows the stdlib asyncio namespace once, so
  psycopg's own `waiting.wait_async` finds our loop and calls our
  `add_reader`/`add_writer`. No per-library work; every wait goes through the loop.
- **upstream's** — [`tonio-monkey`](https://github.com/gi0baro/tonio-monkey), a
  separate package that replaces `psycopg.waiting.wait_async` outright with a
  tonio-native coroutine over `io.register`, plus psycopg's internal `_acompat`
  Lock/Queue/spawn/gather. No asyncio at all; hand-written per library (it also
  ships patches for fastapi, httpx, redis, starlette and websockets).

```bash
docker run -d --rm --name mtaio-bench-pg -e POSTGRES_PASSWORD=bench \
    -e POSTGRES_DB=bench -p 55432:5432 postgres:17-alpine
uv pip install psycopg tonio==0.8.3 'tonio-monkey[psycopg]'
python bench/pg_tax.py                       # both workloads, 1 and 4 threads
python bench/pg_tax.py --tier monkey         # just the like-for-like pair
python bench/pg_tax.py --json bench/results/pg_tax.json
```

Eight arms, all driving the **same libpq work** against the same server,
differing only in who waits on the socket:

| arm | waits with |
| --- | --- |
| `tonio-io` | `tonio.colored` + a hand-written waiter on `tonio.io.register` |
| `mt-io` | mt_asyncio + **the same** hand-written waiter on `mt_asyncio.io` |
| `mt-raw` | mt_asyncio + psycopg's `waiting.wait_async` (compat installed) |
| `stdlib-raw` | stdlib asyncio + psycopg's `waiting.wait_async` |
| `tonio-monkey` | `tonio.colored` + tonio-monkey's patched psycopg, full async API |
| `mt-monkey` | mt_asyncio + **a port of that patch** onto `mt_asyncio.io` |
| `mt-api` | compat.install() + full async API — **what we ship** |
| `stdlib-api` | stdlib asyncio + full async API — the reference |

`-io`/`-raw` drive psycopg's `generators.execute` directly, so runtime differences
show undiluted; `-monkey`/`-api` use the full `AsyncConnection`/cursor path a user
actually writes.

Result (tonio 0.8.3 + tonio-monkey 0.4.0 from PyPI vs this tree, both release
builds, Apple M5 Max, free-threaded 3.14.4, psycopg 3.3.4 with the **pure-Python**
libpq binding, Postgres 17 in Docker on loopback, 8 connections × 250 queries,
median of 7):

| arm | `rtt` 1t | `rtt` 4t | `handler` 1t | `handler` 4t |
| --- | --- | --- | --- | --- |
| `tonio-io` | 64.7 ms | 72.8 ms | 731.5 ms | 216.7 ms |
| `mt-io` | 62.7 ms | 74.0 ms | 747.3 ms | 212.9 ms |
| `mt-raw` | 81.7 ms | 104.7 ms | 764.1 ms | 259.9 ms |
| `stdlib-raw` | 64.8 ms | 65.3 ms | 739.0 ms | 736.1 ms |
| `tonio-monkey` | 76.0 ms | 95.8 ms | 757.2 ms | 241.7 ms |
| `mt-monkey` | 73.7 ms | 93.5 ms | 768.1 ms | 247.6 ms |
| `mt-api` | 102.5 ms | 125.1 ms | 814.9 ms | 275.3 ms |
| `stdlib-api` | 76.9 ms | 80.5 ms | 760.3 ms | 757.4 ms |

**No tax on the runtime.** Two independent pairs say so: `mt-io` vs `tonio-io`
(hand-written waiter, identical code) lands within ±3%, and `mt-monkey` vs
`tonio-monkey` (upstream's strategy, ported) within ±3% as well — noise in both
directions across both workloads and thread counts. Same verdict as
`tonio_regression.py`'s core tier, now with a real driver on a real socket.

**The tax is the wait path, and it is the whole gap.** `mt-api` costs
**+34–39%** over `mt-monkey` on `rtt` and +6–11% on `handler` — same runtime,
same libpq work, same cursor code, differing only in whether psycopg's wait goes
through the asyncio loop or straight at the reactor. This is steady-state
per-wait cost, not setup: `compat.install()` and every import happen once per
process in `_run_worker` before anything is timed, two warmup runs are
discarded, and the clock in `_make_main` starts only after the connections are
open.

Routing the wait through the loop means psycopg's `wait_async` (an `Event`, a
`wait_for`, an `add_reader`/`remove_reader` pair per wait) landing on an
`add_reader` we
*emulate*: the reactor is edge-triggered and one-shot, so `_FdHandle` re-arms
after every dispatch and issues a `poll(2)` to recover the level (see
`_loop._FdHandle`). That is several extra syscalls and a Python callback hop per
round trip. `mt-raw` vs `mt-io` shows the same thing at the raw-libpq level
(+30% on `rtt` @4t) with the cursor machinery taken out of the way.

**End to end against upstream's answer**, `mt-api` is +31–35% slower than
`tonio-monkey` on `rtt` and +8–14% on `handler`. None of that is the core; all of
it is the convenience of not writing a per-library patch. A psycopg-native waiter
on our side — which is all `mt-monkey` is, ~40 lines — closes it, and would
compose with `compat.install()` rather than replace it.

**Against stdlib, the shape is the usual one.** With nothing to parallelize
(`rtt`) stdlib is ahead — 76.9 ms vs 102.5 ms — since every extra scheduling
feature is pure cost. Put per-row Python work in the handler and it inverts hard:
275.3 ms vs 757.4 ms at 4 threads, **2.8× faster**, because the handler work runs
in parallel and stdlib's cannot. `tonio-monkey` reaches 241.7 ms there, so the
parallelism is the runtime's, not the patching strategy's.

One caveat on absolute numbers: psycopg's libpq binding here is `impl: python`
(ctypes), which is slower than the C/binary one and compresses every runtime
difference. Install `psycopg[binary]` to see the gaps widen, not narrow — the
comparison between arms is unaffected, since every arm pays it equally.

### Where the cost is

Every arm pays for the same libpq round trip, so the gap is entirely the
machinery around the wait. Measured on this machine (Apple M5 Max, free-threaded
3.14.4, psycopg C binding), per operation:

| operation | cost |
| --- | --- |
| `wait_for(Event.wait(), 0.1)` that blocks — pre-3.12 shape | 22.0 µs |
| ... the same call on stdlib asyncio | 1.1 µs |
| bare cross-task park + wake (the floor for any wait here) | 6.6 µs |
| `add_reader` + `remove_reader` pair | 6.4 µs |
| ... of which `io.register` + `close` | 2.5 µs |
| `poll(2)` (zero timeout) when the fd **is** ready | 0.44 µs |
| `poll(2)` (zero timeout) when the fd is **not** ready | **14.9 µs** |
| `arm_r` + `consume_r` — the whole native wait | 0.83 µs |
| `ScheduledIO.resample` | 0.79 µs |
| `os.fstat` | 0.32 µs |

And per psycopg query, counted by instrumenting the loop:

| per query | 1 thread | 12 threads |
| --- | --- | --- |
| `asyncio.Event()` / `wait_for()` | 1.0 / 1.0 | 1.0 / 1.0 |
| `add_reader` / `remove_reader` | 1.0 / 1.0 | 1.0 / 1.0 |
| `_FdHandle._arm` | 2.04 | 2.90 |
| `poll(2)` | 1.02 | 2.27 |

Three things follow.

**A query is exactly one wait**, so there is nothing to amortise these against.
That is why the gap is worst on `rtt` (+24% after the `wait_for` fix) and
smallest on `handler` (+7%), where real per-row work dwarfs the wait. The
overhead is a fixed per-wait cost, not a proportional one.

**It grows with threads because the re-arm count does** — 2.04 arms and 1.02
polls per query at one thread, 2.90 and 2.27 at twelve. `_dispatch_soon`'s
docstring explains the mechanism: psycopg's `add_reader` callback only sets an
`Event`, it does not drain the socket, so the fd stays readable until the
*waiting task* gets a turn and runs libpq's read. More workers widen that
window, so the re-arm keeps finding the fd ready and dispatches again, each
repeat costing an injector round trip and a `poll(2)`.

**The `poll(2)` asymmetry is why a registration cache backfires.** Ready costs
0.44 µs, not-ready costs 14.9 µs — and "not ready" is exactly the stale-bit case.
A *fresh* mio registration re-samples the level for free (kqueue and epoll both
report current readiness at registration), so building a `ScheduledIO` per wait
was doing the level check as a side effect and the first arm parked cleanly.
Caching the registration keeps the previous wait's bits, pushing every wait onto
the expensive branch: polls went 1.02 → 2.85 per query and `rtt`@1t went
138 ms → 254 ms. Adding a `resample()` on reuse fixes the poll count but only
buys back 1.4 µs against 2.5 µs of register/close, which the `os.fstat` identity
check and an extra lock round trip eat again. Both attempts are on the branch
(`git log --grep="fd registration"`) with their numbers.

By contrast `tonio-monkey` pays 0.83 µs of machinery per wait, because psycopg's
generator tells it exactly when to block: it arms once, parks, and consumes. No
`Event`, no `wait_for` timer, no `add_reader` contract to emulate, and no level
to recover. That difference *is* the benchmark result.

A note the benchmark had to learn the hard way, in `_wait_io`'s docstring: a
consumer of `io.register` must drain the cached readiness bit **before** the
syscall that may return `EAGAIN`, not after. Draining after loses the wakeup that
arrives in between and deadlocks above one worker thread; not draining at all
turns the wait into a busy-loop (~3× slower). tonio-monkey's `_wait_async` gets
this right, and it is why the port in `_mt_wait_async_factory` follows its order
exactly. mt_asyncio's `ScheduledIO` also grew tick-guarded `clear_r`/`clear_w`,
which are safe either way; upstream's exposes only `consume_*`.

## 4. `fastapi_tax.py` — FastAPI over a real socket, three runtimes

`pg_tax.py` asks what the runtime costs a driver. This asks the question a user
actually has: **I have a FastAPI service, its handler does a little work per
request, what changes if I swap the loop underneath it?**

Three arms serve the *same* FastAPI app over HTTP/1.1 with keep-alive, driven by
the same load generator (`oha`, so the client is Rust and not competing for the
GIL-free Python the server is using):

| arm | runtime | how FastAPI runs unmodified on it |
| --- | --- | --- |
| `stdlib` | stdlib asyncio | it just does |
| `mt` | mt_asyncio | `compat.install()` — the asyncio namespace is shadowed, and starlette's few anyio touchpoints keep working because what is underneath is still asyncio-shaped |
| `tonio` | tonio.colored | [`tonio-monkey`](https://github.com/gi0baro/tonio-monkey), which rewrites those touchpoints against tonio primitives |

Neither project patches the *handler*: the endpoint is one `async def` written
once and imported by every arm.

**The server is ours, on purpose.** tonio-monkey patches fastapi and starlette
but ships no ASGI server, and granian's loop registry is asyncio-only, so there
is no server all three arms could share off the shelf. Letting uvicorn serve two
arms and something hand-rolled serve the third would measure the servers. So the
file contains one minimal HTTP/1.1 server whose parsing, scope construction, ASGI
call and response encoding are a single shared code path (`_serve_conn`); only
the calls that move bytes differ — `StreamReader.read`/`StreamWriter.write` on
the asyncio arms, `SocketStream.receive_some`/`send_all` on tonio's. Each sits on
its runtime's real connection machinery. Before measuring, every arm is probed
once and the response bodies compared, so an arm cannot look fast by answering
something cheaper.

```bash
uv pip install fastapi uvicorn psycopg tonio==0.8.3 'tonio-monkey[fastapi]'
brew install oha                            # or run with --client python

python bench/fastapi_tax.py                 # ping + compute, 1/2/4/8 threads
python bench/fastapi_tax.py -w compute -t 1 2 4 8 --conns 16
python bench/fastapi_tax.py --setup-db      # fixture table for the `pg` workload
python bench/fastapi_tax.py --json bench/results/fastapi_tax.json
```

Three workloads, all `GET`, all differing only in what the handler does:

- **`ping`** — an empty handler. The floor: HTTP parse, routing, response encode.
- **`compute`** — the realistic one, and the point of the exercise. Path and
  query params validated, 32 order lines aggregated in an ordinary Python loop,
  a pydantic response model serialised. Tens of microseconds — the thing that
  queues behind a single thread on stdlib asyncio.
- **`pg`** — the same handler with the rows coming from Postgres, so a real
  driver and a real socket are in the request path.

Result (median of 5 runs of 20,000 requests over 16 keep-alive connections;
tonio 0.8.3 + tonio-monkey 0.4.0 from PyPI vs this tree, release build; fastapi
0.140.0, starlette 1.3.1; free-threaded 3.14.4, Apple M5 Max, 6 performance + 12
efficiency cores). **req/s, higher is better:**

| workload | arm | 1t | 2t | 4t | 8t | best vs stdlib |
| --- | --- | --- | --- | --- | --- | --- |
| `ping` | `stdlib` | 39,759 | · | · | · | 1.00× |
| | `mt` | 19,038 | 29,808 | **45,489** | 42,904 | 1.14× |
| | `tonio` | 44,034 | 47,920 | 58,496 | **58,723** | 1.48× |
| `compute` | `stdlib` | 23,809 | · | · | · | 1.00× |
| | `mt` | 14,306 | 23,822 | **35,992** | 31,261 | 1.51× |
| | `tonio` | 25,443 | 33,000 | **42,214** | 37,531 | 1.77× |
| `pg` | `stdlib` | 11,397 | · | · | · | 1.00× |
| | `mt` | 7,803 | 9,385 | 10,501 | **10,900** | 0.96× |
| | `tonio` | 12,399 | 12,629 | 13,291 | **14,815** | 1.30× |

**The handler work does parallelise, and that is the whole case.** On `compute`,
mt_asyncio goes 14,306 → 35,992 req/s from one worker to four — **2.5×** — and
ends up 1.51× stdlib on the same app with the same handler. Nothing in the
application was written for it.

**The per-request floor is the price.** At one worker mt_asyncio serves 0.60× as
many requests as stdlib on `compute` and 0.48× on `ping`, so it spends the first
two workers buying back its own overhead and only the third and fourth are
profit. That ratio is the same story `asyncio_bench.py` tells on `churn`: with
nothing to parallelise, the heavier per-task machinery is all there is. The
thinner the handler, the more workers it takes to break even — `ping` needs four
to beat stdlib by 14%, `compute` needs two.

**TonIO is ahead at every point**, and by more at one thread (1.07× stdlib on
`compute`, where mt_asyncio is 0.60×) than at four (1.77× vs 1.51×). The gap is
the asyncio layer, not the shared Rust core — `tonio_regression.py` puts the core
at parity or better — and it is the same trade `pg_tax.py` prices: cancellation,
contextvars, exception groups and the `Future` protocol cost something per
operation, and `tonio.colored` does not implement them.

**On `pg`, mt_asyncio never beats stdlib.** It tops out at 10,900 req/s against
stdlib's 11,397, scaling only 1.4× across eight workers while `compute` scales
2.5× across four. This is `pg_tax.py`'s wait-path tax showing up end to end: with
`compat.install()`, psycopg's `wait_async` lands on an `add_reader` we emulate,
costing several syscalls and a Python callback hop per round trip — and a query
is exactly one wait, so there is nothing to amortise it against. tonio-monkey's
psycopg patch goes straight at the reactor and reaches 1.30×. A psycopg-native
waiter on our side (`bench/pg_tax.py`'s `mt-monkey`, ~40 lines) is the fix, and it
composes with `compat.install()` rather than replacing it.

**8 workers is past the knee on this machine.** Both parallel arms lose
throughput from 4t to 8t on `compute` (mt 35,992 → 31,261; tonio 42,214 →
37,531). There are 6 performance cores, the client wants some of them, and the
efficiency cores are slower — this is the machine, not the runtimes.

### The server is not a strawman

The `-uvicorn` arms run the same app under a real server, on the two arms that
can host one (median of 3, separate run — compare within this table, not against
the one above):

| workload | arm | 1t | 2t | 4t | 8t |
| --- | --- | --- | --- | --- | --- |
| `ping` | `stdlib` / `stdlib-uvicorn` | 42,742 / 15,703 | · | · | · |
| | `mt` / `mt-uvicorn` | 20,418 / 10,199 | 30,937 / 17,860 | 46,145 / 28,005 | 43,216 / 28,799 |
| `compute` | `stdlib` / `stdlib-uvicorn` | 24,549 / 12,636 | · | · | · |
| | `mt` / `mt-uvicorn` | 15,259 / 8,382 | 23,941 / 15,020 | 35,866 / 24,542 | 31,656 / 24,685 |

The hand-written server is ~2× uvicorn in absolute terms — it is a GET-only,
no-body, one-write server against h11 — but the **shape is preserved**:
mt_asyncio scales uvicorn 8,382 → 24,542 req/s on `compute` (2.9×, versus 2.4× for
the hand-written one), so the parallelism is not an artifact of the server. Worth
saying plainly: that is stock uvicorn off PyPI, scaled across four cores with two
lines at the top of the file.

### A defect this turned up

**mt_asyncio drops connections at 8 workers.** Roughly 1 request in 20,000 —
`connection error`, `connection closed before message completed`, `operation was
canceled` — in 2 runs out of 5. Never at 1, 2 or 4 workers; never on stdlib;
never on tonio. Three runs of `mt-uvicorn` at 8 workers came through clean, which
points at the streams layer (`StreamReader`/`StreamReaderProtocol`) rather than
the transports uvicorn drives directly, but three runs is a lead and not a
finding. The harness reports these as `!n` beside the throughput number and keeps
the run rather than discarding it: discarding would hide the defect *and* bias
the survivors towards the lucky ones. Losses above 1% fail the measurement.

### Caveats

- **The asyncio arms carry more framework than tonio's.** `asyncio.start_server`
  means transport + protocol + `StreamReader`/`StreamWriter`; tonio's
  `SocketStream` is socket operations on the reactor. That is what each project
  ships, and the asyncio route is the one uvicorn uses — but part of the `tonio`
  column is that it has less machinery in the way, not only that it is faster.
- **`pg` is "what each project ships", not a runtime comparison.** mt_asyncio
  routes psycopg through the loop; tonio uses tonio-monkey's hand-written
  psycopg waiter. `pg_tax.py` is where those are separated properly.
- **Client and server share the box.** `oha` is native and cheap next to the
  Python server, but it is not free, and it competes for the same 6 performance
  cores at the higher thread counts.
- Run-to-run spread on this machine was under ~10% for most cells; the JSON keeps
  every sample plus the spread, so a suspicious number can be checked.

## 5. `benchmarks.py` + `runbench.sh` — subprocess harness

The original TonIO harness, measuring "1 million coroutines" and a TCP echo
server against stdlib `asyncio`. Driven by `./bench/runbench.sh`, which builds a
release wheel into a throwaway venv and writes `results/data.json`.

> **Stale:** the tables below are generated output from a run that predates the
> fork — they still list the removed `yield`/`async` runtimes. Regenerate with
> `bench/runbench.sh` before quoting any of it.

Run at: Sun 12 Jul 2026, 13:40    
Environment: AMD Ryzen 7 5700X @ Gentoo Linux 6.12.93 (CPUs: 16)    
Python version: 3.14    
mt_asyncio version: 0.8.2    

### Running 1 million coroutines

Time to run 1 million coroutines (lower is better).


| Runtime | Creation time | Exec time | Total time | Relative performance |
| --- | --- | --- | --- | --- |
| mt_asyncio yield | 112.109ms | 448.233ms | 560.342ms | 5.18x |
| mt_asyncio async | 84.707ms | 788.682ms | 873.389ms | 3.32x |
| mt_asyncio yield (context) | 71.866ms | 645.002ms | 716.869ms | 4.05x |
| mt_asyncio async (context) | 51.756ms | 926.25ms | 978.006ms | 2.97x |
| AsyncIO | 40.371ms | 2860.729ms | 2901.1ms | 1.0x |

### Sockets

TCP echo server with raw sockets comparison using 1KB, 10KB and 100KB messages.


| Runtime | Throughput (1KB) | Throughput (10KB) | Throughput (100KB) |
| --- | --- | --- | --- |
| mt_asyncio yield | 127575.7 (2.37x) | 107360.6 (2.24x) | 44795.5 (1.6x) | 
| mt_asyncio async | 134070.3 (2.49x) | 109253.5 (2.28x) | 43632.7 (1.56x) | 
| mt_asyncio yield (context) | 124513.7 (2.31x) | 106177.1 (2.22x) | 43509.3 (1.56x) | 
| mt_asyncio async (context) | 127584.6 (2.37x) | 106695.0 (2.23x) | 45045.6 (1.61x) | 
| AsyncIO | 53800.8 (1.0x) | 47837.7 (1.0x) | 27913.1 (1.0x) | 

#### 1KB details

| Runtime | Total requests | Throughput | Mean latency | 99p latency | Latency stdev |
| --- | --- | --- | --- | --- | --- |
| mt_asyncio yield | 1275757 | 127575.7 (2.37x) | 0.03ms | 0.04ms | 0.001 |
| mt_asyncio async | 1340703 | 134070.3 (2.49x) | 0.03ms | 0.04ms | 0.002 |
| mt_asyncio yield (context) | 1245137 | 124513.7 (2.31x) | 0.03ms | 0.04ms | 0.001 |
| mt_asyncio async (context) | 1275846 | 127584.6 (2.37x) | 0.03ms | 0.042ms | 0.002 |
| AsyncIO | 538008 | 53800.8 (1.0x) | 0.071ms | 0.095ms | 0.005 |


#### 10KB details

| Runtime | Total requests | Throughput | Mean latency | 99p latency | Latency stdev |
| --- | --- | --- | --- | --- | --- |
| mt_asyncio yield | 1073606 | 107360.6 (2.24x) | 0.039ms | 0.05ms | 0.004 |
| mt_asyncio async | 1092535 | 109253.5 (2.28x) | 0.035ms | 0.05ms | 0.006 |
| mt_asyncio yield (context) | 1061771 | 106177.1 (2.22x) | 0.04ms | 0.05ms | 0.002 |
| mt_asyncio async (context) | 1066950 | 106695.0 (2.23x) | 0.037ms | 0.05ms | 0.005 |
| AsyncIO | 478377 | 47837.7 (1.0x) | 0.082ms | 0.102ms | 0.005 |


#### 100KB details

| Runtime | Total requests | Throughput | Mean latency | 99p latency | Latency stdev |
| --- | --- | --- | --- | --- | --- |
| mt_asyncio yield | 447955 | 44795.5 (1.6x) | 0.09ms | 0.103ms | 0.004 |
| mt_asyncio async | 436327 | 43632.7 (1.56x) | 0.09ms | 0.108ms | 0.004 |
| mt_asyncio yield (context) | 435093 | 43509.3 (1.56x) | 0.091ms | 0.108ms | 0.004 |
| mt_asyncio async (context) | 450456 | 45045.6 (1.61x) | 0.089ms | 0.1ms | 0.004 |
| AsyncIO | 279131 | 27913.1 (1.0x) | 0.141ms | 0.167ms | 0.01 |


### Concurrency

#### 1 million coros


| Mode | Threads | Total time |
| --- | --- | --- |
| mt_asyncio yield | 1 | 577.176ms |
| mt_asyncio async | 1 | 876.986ms |
| mt_asyncio yield | 2 | 747.713ms |
| mt_asyncio async | 2 | 903.686ms |
| mt_asyncio yield | 4 | 1039.288ms |
| mt_asyncio async | 4 | 924.032ms |
| mt_asyncio yield | 8 | 1281.226ms |
| mt_asyncio async | 8 | 1049.043ms |

#### Sockets


| Mode | Threads | Throughput (10KB) |
| --- | --- | --- |
| mt_asyncio yield | 1 | 108001.2 |
| mt_asyncio async | 1 | 110366.7 |
| mt_asyncio yield | 2 | 180453.6 |
| mt_asyncio async | 2 | 196350.9 |
| mt_asyncio yield | 4 | 257195.4 |
| mt_asyncio async | 4 | 271309.1 |
| mt_asyncio yield | 8 | 346740.4 |
| mt_asyncio async | 8 | 369812.7 |
