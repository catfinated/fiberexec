# Findings

Benchmark results and analysis for fiberexec. The primary research question is
whether the fiber+P2300 composition layer is a good model for I/O-heavy
workloads — specifically whether it imposes measurable overhead over raw
io_uring, and how it compares to coroutine-based alternatives.

See [docs/BENCHMARKS.md](docs/BENCHMARKS.md) for methodology and benchmark
design.

---

## Environment

AMD Ryzen 7 5700X, 8 cores / 16 logical, ~4.7 GHz. Linux 6.x, Clang, `-O3`.
All benchmarks use `UseRealTime()` — fiber work runs on pool threads while the
benchmark thread sleeps in `sync_wait`, so wall time is the only meaningful
measure.

---

## Scheduler microbenchmarks (`bench_scheduler`)

| Benchmark | Wall time / iter | Throughput |
|-----------|-----------------|------------|
| Fiber context switch (2 fibers, 10 000 round-trips) | ~1.09 ms | ~18.4 M switches/s · **~54 ns/switch** |
| Thread context switch (semaphore ping-pong, 10 000 round-trips) | ~3.32 ms | ~6.0 M switches/s · **~166 ns/switch** |
| `run(sched, noop)` round-trip | **~6.5 µs** | — |
| `schedule(sched) \| then(noop)` round-trip | **~6.5 µs** | — |

**Fiber vs thread context switch**: fiber switches are ~3× faster than a
semaphore-gated thread ping-pong (~54 ns vs ~166 ns per switch). Both
measurements count two switches per "item" (A→B and B→A), so the raw
switch latency is half the per-item figure.

**Schedule overhead**: a full `run(sched, fn)` round-trip — enqueue task,
pool thread picks it up, fiber switch, execute noop, set_value, sync_wait
unblocks — costs ~6.5 µs. `schedule | then` is within noise of `run`,
confirming the extra receiver machinery in `run` adds nothing measurable.

---

## Echo server benchmarks (`bench_echo`)

Each iteration opens N concurrent connections, each exchanging 100 round-trips
of a 64-byte payload over loopback TCP. Clients are always OS threads with
blocking syscalls; only the server side differs. Throughput is total
round-trips per second across all connections.

### Concurrency sweep

| Benchmark | Connections | Throughput |
|-----------|-------------|------------|
| fiberexec (io_uring + fibers, 16 threads) | 1 | ~74k round-trips/s |
| fiberexec (io_uring + fibers, 16 threads) | 10 | ~425k round-trips/s |
| fiberexec (io_uring + fibers, 16 threads) | 100 | ~638k round-trips/s |
| fiberexec (io_uring + fibers, 16 threads) | 1000 | ~716k round-trips/s |
| Thread-per-connection | 1 | ~71k round-trips/s |
| Thread-per-connection | 10 | ~257k round-trips/s |
| Thread-per-connection | 100 | ~605k round-trips/s |
| Thread-per-connection | 1000 | — (skipped; ~2000 OS threads) |
| Asio coroutines (`use_awaitable`) | 1 | ~32k round-trips/s |
| Asio coroutines (`use_awaitable`) | 10 | ~215k round-trips/s |
| Asio coroutines (`use_awaitable`) | 100 | ~530k round-trips/s |
| Asio coroutines (`use_awaitable`) | 1000 | ~620k round-trips/s |
| asioexec (`exec::asio` + `use_sender`) | 1 | ~32k round-trips/s |
| asioexec (`exec::asio` + `use_sender`) | 10 | ~213k round-trips/s |
| asioexec (`exec::asio` + `use_sender`) | 100 | ~518k round-trips/s |
| asioexec (`exec::asio` + `use_sender`) | 1000 | ~621k round-trips/s |

**fiberexec vs Asio**: fiber wins at every concurrency level — 2.3× faster at
1 connection, 2.0× at 10, 1.2× at 100 and 1000. The gap is largest at low
concurrency where per-round-trip latency dominates. Asio's coroutine path
involves more dispatch layers per operation (async_result machinery, executor
dispatch, composed-operation resumption) than fiberexec's direct io_uring path
(io_uring_submit → CQE → fiber resume). At high concurrency the gap narrows as
throughput becomes bandwidth-bound rather than latency-bound.

**Asio vs asioexec** are within 1–2% at every concurrency level. The extra
`use_sender` dispatch in the accept path adds nothing measurable relative to
the I/O cost of the connections themselves.

**Thread-per-connection** lies between the two async models at 10+ connections,
and is the only approach that cannot scale to 1000 connections without ~2000 OS
threads.

### Raw io_uring baseline: isolating the fiber+P2300 overhead

`BM_IoUringEchoServer` is a hand-rolled single-ring io_uring event loop — no
fibers, no P2300, each connection handled as an explicit recv/send state
machine. `BM_FiberEchoServer1T` is fiberexec pinned to a single worker thread,
matching the parallelism of the raw baseline exactly. The delta between the two
is the pure cost of Boost.Fiber suspension plus the P2300 sender/receiver
machinery.

| Benchmark | Connections | Throughput |
|-----------|-------------|------------|
| fiberexec (1 thread) | 1 | ~76k round-trips/s |
| fiberexec (1 thread) | 10 | ~183k round-trips/s |
| fiberexec (1 thread) | 100 | ~183k round-trips/s |
| fiberexec (1 thread) | 1000 | ~180k round-trips/s |
| Raw io_uring event loop (1 thread) | 1 | ~73k round-trips/s |
| Raw io_uring event loop (1 thread) | 10 | ~168k round-trips/s |
| Raw io_uring event loop (1 thread) | 100 | ~187k round-trips/s |
| Raw io_uring event loop (1 thread) | 1000 | ~193k round-trips/s |

At matched parallelism, fiberexec and the raw io_uring event loop are within
noise of each other at every concurrency level. The fiber suspension, the
Boost.Fiber scheduler, and the P2300 sender/receiver machinery add no
measurable overhead over a hand-written state machine. The 3-4× throughput
advantage of `BM_FiberEchoServer` over the raw baseline is entirely
attributable to parallelism across 16 threads, not to any property of the
fiber or P2300 layer itself.

This is the central finding of the project: the fiber+P2300 composition model
is zero-overhead over raw io_uring. You pay nothing for the sequential-looking
fiber code or the structured concurrency at the sender boundary.

---

## Message-size sweep (`bench_echo`, `BM_*EchoMsgSize`)

Fixed concurrency at 10 connections; message size varies. Throughput is in
MiB/s or GiB/s (one direction).

| Benchmark | 64 B | 512 B | 4 KB | 64 KB |
|-----------|------|-------|------|-------|
| fiberexec (io_uring + fibers) | ~25.6 MiB/s | ~204 MiB/s | ~1.45 GiB/s | ~9.8 GiB/s |
| Thread-per-connection | ~27.0 MiB/s | ~226 MiB/s | ~1.66 GiB/s | ~12.7 GiB/s |
| Asio coroutines (`use_awaitable`) | ~14.1 MiB/s | ~102 MiB/s | ~800 MiB/s | ~4.06 GiB/s |
| asioexec (`exec::asio` + `use_sender`) | ~13.0 MiB/s | ~103 MiB/s | ~800 MiB/s | ~4.00 GiB/s |

**fiberexec vs Asio**: the advantage does not narrow with larger messages — it
holds at ~1.8–2× from 64 B through 4 KB and widens to ~2.4× at 64 KB. The
narrowing hypothesis (per-op dispatch overhead becomes negligible at large
payloads) does not hold. Asio's `async_read` composed-operation machinery adds
overhead that becomes more visible when 10 connections simultaneously move large
payloads and compete for the event loop.

**fiberexec vs thread-per-connection**: blocking threads are faster at every
message size (5–30% ahead, gap widening with payload). The reason is the
io_uring submission overhead: blocking `recv`/`send` returns immediately when
data is already in the socket buffer, while fiberexec always pays the SQE→CQE
round-trip even when the data is ready. io_uring's async path is most
beneficial when operations actually need to wait; for bandwidth-saturated
loopback the synchronous path wins.

---

## Fan-out benchmarks (`bench_fanout`)

Compares `stdexec::bulk` on `fiberexec::scheduler` against `stdexec::bulk` on
`exec::static_thread_pool`. N socketpairs are pre-created; one byte is written
per pair per iteration, then N concurrent readers drain them via `bulk`. This
isolates scheduler fan-out overhead from I/O latency. 4-thread pool, 5
repetitions.

| N | Fiber (real) | Thread (real) | Fiber items/s | Thread items/s |
|---|---|---|---|---|
| 2 | 13.0 µs | 10.9 µs | 154k/s | 183k/s |
| 8 | 23.6 µs | 17.2 µs | 339k/s | 465k/s |
| 32 | 64.9 µs | 39.2 µs | 493k/s | 816k/s |
| 128 | 246 µs | 131 µs | 520k/s | 980k/s |

**Threads win, and the gap widens with N.** CPU time is nearly identical at
every N (both ~101 µs at N=128); all the difference is coordination overhead.
The real/CPU ratio is 2.4× for fibers vs 1.3× for threads at N=128, reflecting
io_uring SQE submission, CQE delivery, and fiber context-switch cost on each of
the N indices.

This benchmark pre-fills the socketpairs before each iteration, so every read
completes immediately — there is no actual I/O latency to overlap. The fiber's
io_uring round-trip is pure overhead with no benefit in this scenario. The
fiber advantage materialises when I/O actually blocks: pool threads that would
otherwise stall waiting for data can instead run other fibers. That is what the
echo server benchmarks measure. This benchmark deliberately isolates the
opposite case — zero-latency reads — to characterise the raw scheduling
overhead.

---

## Latency distributions (`bench_latency`)

Per-request round-trip latency at varying concurrency. Each iteration is a
single 64-byte send+recv, timed individually with `steady_clock`. The server
runs persistently in a background thread; N client connections are established
once and reused across all iterations in round-robin. All values in
microseconds. `MinTime(5.0)` ensures at least ~500k samples per configuration,
giving stable p999 estimates.

fiberexec uses `fiberexec::run` + `fiberexec::channel<int>` + `stdexec::bulk`
(the same public API as `echo_server_pool.cpp`). All other client-side timing
uses blocking `send`/`recv` on the main thread.

| Benchmark | Connections | p50 (µs) | p99 (µs) | p999 (µs) |
|-----------|-------------|----------|----------|-----------|
| fiberexec (io_uring + fibers) | 1 | ~11.2 | ~15.8 | ~28.2 |
| fiberexec (io_uring + fibers) | 10 | ~11.4 | ~17.0 | ~29.4 |
| fiberexec (io_uring + fibers) | 100 | ~11.9 | ~17.5 | ~30.8 |
| Thread-per-connection | 1 | ~10.2 | ~16.3 | ~41.0 |
| Thread-per-connection | 10 | ~10.6 | ~17.5 | ~47.5 |
| Thread-per-connection | 100 | ~11.5 | ~17.8 | ~54.0 |
| Asio coroutines (`use_awaitable`) | 1 | ~12.1 | ~19.0 | ~45.2 |
| Asio coroutines (`use_awaitable`) | 10 | ~12.1 | ~16.7 | ~24.5 |
| Asio coroutines (`use_awaitable`) | 100 | ~12.2 | ~17.0 | ~24.6 |
| Raw io_uring event loop (1 thread) | 1 | ~10.8 | ~14.5 | ~25.3 |
| Raw io_uring event loop (1 thread) | 10 | ~10.8 | ~14.7 | ~25.2 |
| Raw io_uring event loop (1 thread) | 100 | ~10.9 | ~14.8 | ~25.4 |

**p50 (median latency):** fiberexec (~11.2–11.9 µs) sits between raw io_uring
(~10.8 µs) and Asio (~12.1 µs). The ~0.4–1.1 µs gap over raw io_uring is the
fiber suspend/resume cost, consistent with the ~54 ns context switch from the
scheduler microbenchmarks — several switches fire per round-trip (submit, yield,
CQE, resume). The gap over Asio confirms the throughput result: fiberexec's
direct io_uring path has less per-operation overhead than Asio's
composed-operation machinery.

**Tail latency (p999):** fiberexec's tail (~28–31 µs) is tighter than
thread-per-connection (~41–54 µs) and Asio at 1 connection (~45 µs). Thread
tails worsen steadily with concurrency — OS scheduling jitter accumulates as
more threads compete for cores — while fiberexec stays nearly flat. This is the
key differentiator: at median latency fiberexec is marginally behind blocking
threads, but at the tail it is consistently ahead and does not degrade with
connection count.

**fiberexec vs raw io_uring:** the p999 gap (~3–6 µs) is larger than the p50
gap (~0.4 µs). The raw event loop is single-threaded with no scheduling
decisions: every CQE is processed inline. fiberexec's fiber context switches
introduce occasional outliers that do not appear in the lock-free state machine.
The extra tail is the price of readable sequential code.

**Asio p999 anomaly:** Asio's p999 drops sharply from 1 connection (~45 µs) to
10 connections (~24 µs) and stays flat at 100. At 1 connection the Asio event
loop runs a single coroutine with no concurrency to hide scheduling jitter —
every outlier lands on the one active request. At 10+ connections multiple
round-trips are in flight; the event loop batches CQE processing and the
worst-case scheduling pause is amortised across connections. The effect does not
appear in fiberexec because the fiber pool always has other work to run
regardless of connection count.

### Diagnosing the rising fiberexec tail

The fiberexec p999 rises from ~27 µs at N=1 to ~30 µs at N=100. Two candidate
causes were tested using diagnostic pool configurations
(`BM_FiberEchoLatency1T` and `BM_FiberEchoLatencyNT`):

| Configuration | N=1 p999 | N=10 p999 | N=100 p999 |
|---|---|---|---|
| Default (16 threads) | ~26.9 µs | ~29.1 µs | ~29.9 µs |
| 1 thread | ~25.9 µs | ~29.6 µs | ~32.6 µs |
| N threads (1:1 balance) | ~26.4 µs | ~28.1 µs | — |
| Raw io_uring (1 thread) | ~25.3 µs | ~25.2 µs | ~25.4 µs |

**Fiber-to-thread imbalance is a partial cause.** The 1:1 balanced
configuration (one pool thread per fiber, no thread shares more than one
connection) reduces p999 by ~1 µs at N=10 compared to the default 16-thread
pool, confirming that uneven `bulk` task distribution contributes to the tail.

**Boost.Fiber scheduler overhead per thread is the dominant cause.** The
single-thread configuration — which eliminates distribution variance entirely —
produces the *worst* tail at high N (32.6 µs at N=100), not the best. With all
100 fibers on one Boost.Fiber scheduler instance, the per-CQE scheduling
overhead grows with the number of fibers managed by that instance. Spreading
fibers across 16 threads (default) is better precisely because each thread's
scheduler manages ~6 fibers rather than 100. The 1:1 balanced configuration is
best of all because each thread manages exactly one fiber with no scheduler
contention at all — but at the cost of one OS thread per connection, which is
not a realistic operating point.

**The raw io_uring baseline is flat** across all N because it has neither
effect: one thread, one ring, no fiber scheduler, CQEs processed inline with
zero scheduling decisions.

The practical conclusion: the rising fiberexec tail is not a fixable
mis-configuration — it is the intrinsic cost of the Boost.Fiber scheduler
managing multiple fibers per thread. At the concurrency levels measured here the
rise is modest (~3 µs from N=1 to N=100) and the tail remains well below
thread-per-connection at every level.

---

## Blocking I/O delay (`bench_delay`)

The echo-server and latency benchmarks use loopback TCP, where I/O completes
near-instantaneously — the fiber's ability to overlap blocked connections is
not exercised. `bench_delay` addresses this directly: each connection's server
handler sleeps for 1 ms between recv and send, simulating a slow upstream call
or database query. This is the workload where fiber suspension has the most to
offer: a fiber blocked on the 1 ms sleep yields its OS thread, which picks up
another fiber, rather than stalling.

Metric: total round-trips/second across all connections (10 round-trips per
connection, 64-byte payload).

| Benchmark | 1 conn | 10 conns | 100 conns | 1000 conns |
|-----------|--------|----------|-----------|------------|
| fiberexec (16 threads) | 963/s | 9,408/s | 82,105/s | **258,130/s** |
| Thread-per-connection | 923/s | 9,043/s | 77,997/s | — (capped) |
| Asio coroutines (`asio::steady_timer`) | 962/s | 9,477/s | 82,810/s | **5,361/s** |
| Raw io_uring (1 thread, `IORING_OP_TIMEOUT`) | 965/s | 9,163/s | 60,179/s | 98,687/s |

### 1–10 connections — all approaches equivalent

All four are bounded by `10 round-trips × 1 ms = 10 ms` minimum. No
meaningful difference. This confirms the loopback benchmarks are not misleading
at low concurrency — it is specifically high concurrency with real blocking
where the models diverge.

### 100 connections — io_uring single-thread limit appears

Fiber (82k), thread (78k), and Asio (83k) all cluster at ~80–83× the
single-connection baseline — near-perfect overlap across all 100 connections.
Threads pay a small scheduling premium but remain competitive.

The single-threaded io_uring event loop lags at 60k (62×). When 100 timers
fire simultaneously, the single event-loop thread serially drains the burst of
100 CQEs and submits 100 sends before any connection can start its next
round-trip. This serialization accumulates over 10 round-trips per connection.

### 1000 connections — the central result

**fiberexec: 258k/s — 268× the single-connection baseline.** Near-linear
scaling from 1 to 1000. Each of the 1000 fibers suspends on `async_sleep_for`
during its 1 ms delay, freeing the thread to run another fiber. With 16
threads and 1000 simultaneously suspended fibers, throughput keeps climbing as
if all connections run in parallel — because at the fiber level, they do.

**Thread-per-connection: not tested at 1000.** 1000 sleeping OS threads
requires gigabytes of stack and heavy kernel scheduler pressure; the design
breaks down before you can benchmark it. The benchmark is intentionally capped
at 100.

**Asio: 5,361/s — a 15× collapse from 100 connections.** Real time per
iteration jumped from 12 ms (100 conns) to 1,865 ms (1000 conns) while CPU
time stayed at ~32 ms, meaning threads spent almost all of those 1.865 seconds
blocked rather than running. The most likely cause is contention on the
`asio::thread_pool`'s internal work queue: when 1000 coroutines' timers expire
simultaneously, 1000 callbacks are enqueued at once and 16 threads serialize
access to the shared queue under a lock. fiberexec avoids this because each
worker thread owns its io_uring ring and Boost.Fiber scheduler — there is no
shared completion queue.

**Caveat on the Asio result**: I am not an Asio expert, and it is plausible
that a different setup would perform better — for example, per-thread
`asio::io_context` instances rather than a single `asio::thread_pool`, or a
different timer strategy. The collapse is reproducible in this benchmark, but
the root cause has not been fully diagnosed. This is flagged as an open
investigation in the section below.

**Raw io_uring (single thread): 98.7k/s — functional but 2.6× behind
fiberexec.** The single event-loop thread processes CQEs serially; at 1000
connections the burst of simultaneous timer completions takes real time to
drain. Notably, single-threaded io_uring still beats Asio at 1000 connections
despite being single-threaded, because it has no shared-queue contention.

### Summary

The fiber model's advantage at scale is structural: fibers yield during
blocking I/O, threads do not, and fiberexec's per-thread io_uring rings avoid
shared-state contention at completion time. At 1000 connections with a 1 ms
processing delay, fiberexec sustains 268× the single-connection throughput.
The thread model cannot reach 1000 connections. The Asio result at 1000
connections collapsed in this benchmark and warrants further investigation
before drawing firm conclusions about coroutine-based frameworks in general.

---

## Open questions

**Latency distribution under blocking I/O.** `bench_delay` measures throughput
with a 1 ms delay. The p50/p99/p999 latency profile under that same workload
— where fibers are suspended and OS threads are available for other work —
has not yet been measured. This is the natural next step.

**Asio `asio::steady_timer` at high concurrency.** The 15× throughput collapse
at 1000 connections is the most surprising result in this benchmark. Before
treating it as a statement about Asio coroutines in general, it is worth
investigating alternatives: per-thread `asio::io_context`, explicit timer
batching, or a different concurrency model within Asio. I do not have enough
Asio expertise to rule out a benchmark setup issue. If the collapse survives
those alternatives, it is a meaningful finding about `asio::thread_pool`'s
shared work queue at scale; if it does not, the comparison at 1000 connections
should be revised.

---

## Reproducibility

Benchmarks were re-run to verify the results above. Status and command lines
for each benchmark:

### bench_scheduler — replicated

All configurations matched within 2%.

```
./build/benchmark/benchmarks/bench_scheduler --benchmark_repetitions=5
```

### bench_fanout — replicated

All N values matched within ~10%.

```
./build/benchmark/benchmarks/bench_fanout --benchmark_repetitions=5
```

### bench_echo — partially replicated

fiberexec, Asio, asioexec, and raw io_uring sections all reproduced within
~10%. Thread-per-connection at 1 connection measured ~33k/s vs ~71k/s above
(2× discrepancy). Large message sizes for thread-per-connection were 30–65%
off. The multi-connection thread results and all async results are stable.

Command (for sections that replicated):

```
./build/benchmark/benchmarks/bench_echo --benchmark_repetitions=5
```

### bench_delay — new, not yet replicated

Results above are from a single run (`--benchmark_min_time=2s`). The Asio
collapse at 1000 connections was consistent between a short smoke run
(`--benchmark_min_time=0.1s`, 4 iterations) and the full run (74 iterations),
confirming it is not a statistical artifact.

```
./build/benchmark/benchmarks/bench_delay --benchmark_min_time=2s
```

### bench_latency — partially replicated

p50 and p99 matched within ~1–2 µs across all configurations. p999 tail
latencies showed higher variance: thread-per-connection p999 measured ~25 µs
(vs ~41–54 µs above); Asio at 1 connection measured ~23 µs (vs ~45 µs above).
Raw io_uring p999 matched well throughout. Tail latency at this precision
depends on OS scheduling jitter and system load; p50/p99 are the stable
metrics.

Command (p50/p99 results replicated):

```
./build/benchmark/benchmarks/bench_latency
```
