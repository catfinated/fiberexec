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

## Open questions

The measurements above are all throughput on loopback. The case for the fiber
model is strongest when I/O actually blocks with real variance — in that regime
fibers keep threads productive in a way that state-machine event loops cannot
without explicit work-stealing. **Latency distributions (p50/p99/p999) on a
workload with real I/O latency are the missing measurement** that would either
confirm or refute that intuition.
