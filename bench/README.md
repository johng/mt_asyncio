# mt_asyncio benchmarks

Three independent benchmark families live here.

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

## 3. `benchmarks.py` + `runbench.sh` — subprocess harness

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
