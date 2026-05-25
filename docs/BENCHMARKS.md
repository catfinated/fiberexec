# Benchmarking plan

The core research question is whether fibers + senders is a good model for
I/O-heavy workloads. The benchmarks below are organized around three questions
that speak directly to that: what does the scheduler itself cost, does the I/O
path scale, and how does fan-out behave.

---

## Framework

**Google Benchmark** (via vcpkg). Handles warm-up, iteration counting, and
statistical reporting. Output formats work with CI. Benchmarks live in a new
`benchmarks/` directory, built as a separate CMake target excluded from the
default `all` target (so they don't slow normal builds).

---

## 1. Scheduler overhead

These microbenchmarks isolate the scheduler itself from I/O.

### 1a. Fiber context switch latency

Two fibers ping-pong via `boost::fibers::channel_op_status` or explicit
`boost::this_fiber::yield()`. Measure the round-trip time per switch.

```
BM_FiberContextSwitch   // fiber → yield → resume round-trip
BM_ThreadContextSwitch  // std::thread ping-pong via futex, for comparison
```

Expected result: fiber switch should be ~10–50× faster than thread switch.
The thread baseline puts the number in context.

### 1b. Schedule-and-run overhead

Cost of putting work onto the fiber pool and collecting the result.

```
BM_ScheduleNoop         // schedule(sched) | then([] {}) via sync_wait
BM_FiberSyncWaitNoop    // fiber_sync_wait(schedule(sched) | then([] {}))
```

This measures the combined cost of: task enqueue, fiber wake, fiber context
switch, completion signal back to the waiting thread. Useful as a floor for
any workload that uses the scheduler.

---

## 2. I/O throughput and latency

The main event. A loopback echo server where both client and server run in the
same process (as in the existing `echo_server` example), avoiding OS scheduling
interference between separate processes.

### 2a. Echo server at varying concurrency

Fixed message size (64 bytes). Vary the number of concurrent connections:
1, 10, 100, 1000. Measure throughput (round-trips per second) at each level.

```
BM_FiberEchoServer/1    BM_FiberEchoServer/10    BM_FiberEchoServer/100    BM_FiberEchoServer/1000
BM_ThreadEchoServer/1   BM_ThreadEchoServer/10   BM_ThreadEchoServer/100
BM_AsioEchoServer/1     BM_AsioEchoServer/10     BM_AsioEchoServer/100     BM_AsioEchoServer/1000
BM_AsioExecEchoServer/1 BM_AsioExecEchoServer/10 BM_AsioExecEchoServer/100 BM_AsioExecEchoServer/1000
```

Baselines:

- **Thread-per-connection with blocking syscalls** (`BM_ThreadEchoServer`):
  one `std::thread` per connection, plain `recv`/`send`. Shows where fibers
  help: at high concurrency, thread creation and stack overhead become the
  bottleneck. Capped at 100 connections (~200 OS threads total).
- **Asio coroutines** (`BM_AsioEchoServer`): `asio::thread_pool` with
  `co_await async_read`/`async_write`. Represents a mature, production async
  I/O framework using the same underlying io_uring kernel path.
- **asioexec** (`BM_AsioExecEchoServer`): `exec::asio::asio_thread_pool` with
  `use_sender` in the accept path. Tests whether the P2300 composition layer
  adds measurable overhead over plain Asio.

**Why there is no raw io_uring baseline**: a correct raw io_uring server needs
a thread pool where each thread owns one long-lived ring and multiplexes many
connections over it — the same ring-per-OS-thread structure fiberexec uses.
The only real difference from fiberexec would be an explicit connection state
machine instead of fibers. Asio's `io_context` implements this design, so the
Asio benchmarks already cover this point in the design space.

### 2b. Echo server at varying message sizes

Fix concurrency (e.g. 64). Vary message size: 64 B, 512 B, 4 KB, 64 KB.
Checks whether the per-op scheduler overhead becomes negligible at larger
payload sizes.

---

## 3. Fan-out and cancellation

### 3a. `when_all` fan-out scalability

N concurrent fibers each doing one `async_recv`, collected via `fiber_sync_wait
(when_all(...))`. Vary N: 2, 8, 32, 128. Measure total time to collect all
results. Should scale roughly linearly with N on a multi-thread pool; deviation
indicates contention in the scheduler or io_uring submission path.

```
BM_WhenAllFanOut/n:2
BM_WhenAllFanOut/n:32
BM_WhenAllFanOut/n:128
```

### 3b. Cancellation cost

One fiber blocks on `async_recv`; a second fiber triggers cancellation via a
shared `stop_source`. Measure time from `request_stop()` to the cancelled
fiber's exception being caught. Tests the IORING_OP_ASYNC_CANCEL round-trip
through io_uring and the cancel queue.

```
BM_CancellationLatency
```

---

## Metrics

| Benchmark | Primary metric | Secondary |
|---|---|---|
| Context switch | ns/switch | — |
| Schedule-and-run | ns/op | — |
| Echo server | round-trips/sec | p50/p99/p999 latency |
| Fan-out | total time (µs) | per-branch overhead |
| Cancellation | ns from request_stop to catch | — |

---

## Methodology

### Environment setup

- Set CPU governor to `performance` to fix clock frequency:
  ```sh
  sudo cpupower frequency-set -g performance
  ```
- Disable CPU frequency boost if present (`/sys/devices/system/cpu/cpufreq/boost`).
- Pin the benchmark process to a fixed set of cores (`taskset`) to reduce
  scheduler noise. Use as many cores as the `fiber_context` thread count.
- Run benchmarks several times and check for variance before trusting numbers.

### Avoiding measurement artifacts

- **Compiler elimination**: fiber switch benchmarks must ensure the compiler
  cannot eliminate the yield. Use `benchmark::DoNotOptimize` or a volatile
  side-effect inside the fiber body.
- **Warm-up**: Google Benchmark runs a warm-up phase automatically; ensure it
  is not disabled.
- **Clock resolution**: `std::chrono::steady_clock` is sufficient for
  microsecond-and-above measurements. For nanosecond-range context switch
  benchmarks, use `RDTSC` via `benchmark::cycleclock` or accept that the
  measurement floor is ~10 ns.
- **Loopback echo**: both client and server fibers run in the same
  `fiber_context`. This avoids cross-process OS scheduling but means the
  thread pool must have at least 2 threads (client and server can land on
  different threads). Use `fiber_context{4}` for echo benchmarks.

### What to record

For each benchmark, record: machine spec (CPU model, core count, kernel
version), `fiber_context` thread count, Google Benchmark output (mean, median,
stddev), and the preset used (`release` — never benchmark a debug build).

---

## Implementation order

1. Add `benchmarks/CMakeLists.txt` and wire it into the root CMake with a
   `FIBEREXEC_BUILD_BENCHMARKS` option (off by default).
2. Implement scheduler microbenchmarks (1a, 1b) first — they have no
   dependencies and validate the benchmark harness.
3. Implement echo server benchmark (2a) — the most important result.
4. Add baselines (thread-per-connection, Asio coroutines, asioexec) for comparison.
5. Add fan-out and cancellation benchmarks (3a, 3b) last.
