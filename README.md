# fiberexec

A fiber-based scheduler for [stdexec](https://github.com/NVIDIA/stdexec) (P2300), built on [Boost.Fiber](https://github.com/boostorg/fiber).

## Table of Contents

- [What is this?](#what-is-this)
- [Background](#background)
  - [Fibers vs threads](#fibers-vs-threads)
  - [std::execution (P2300)](#stdexecution-p2300)
  - [Why I/O matters here](#why-io-matters-here)
- [Features](#features)
  - [Fibers + senders](#fibers--senders)
  - [stdexec::bulk — parallel fan-out](#stdexecbulk--parallel-fan-out)
  - [channel\<T\> — producer/consumer and accept loops](#channelt--producerconsumer-and-accept-loops)
- [Building](#building)
  - [Prerequisites](#prerequisites)
  - [Configure and build](#configure-and-build)
  - [Run the tests](#run-the-tests)
  - [Run the examples](#run-the-examples)
- [Benchmarks](#benchmarks)
  - [Building](#building-1)
  - [Running](#running)
  - [Scheduler microbenchmarks (bench\_scheduler)](#scheduler-microbenchmarks-bench_scheduler)
  - [Echo server benchmarks (bench\_echo)](#echo-server-benchmarks-bench_echo)
  - [Message-size sweep (bench\_echo, BM\_\*EchoMsgSize)](#message-size-sweep-bench_echo-bm_echomsgsize)
  - [Fan-out benchmarks (bench\_fanout)](#fan-out-benchmarks-bench_fanout)
- [Status](#status)
- [Dependencies](#dependencies)
- [License](#license)

## What is this?

fiberexec is a research project exploring two things at once:

1. **Fiber-based runtimes for I/O-heavy workloads.** Fibers (cooperative, userspace-scheduled lightweight threads) let you run thousands of concurrent tasks on a handful of OS threads. When a task blocks on I/O, the fiber yields and another one picks up — no kernel context switch, no wasted thread sitting idle. This makes fibers a natural fit for servers, pipelines, and anything else that spends most of its time waiting.
2. **The `std::execution` sender/receiver model for async I/O.** The C++26 `std::execution` framework (P2300) gives us a composable, structured way to express async work — but the standard doesn't include networking or I/O yet. That's expected to come in C++29, and the sender/receiver model was explicitly designed with I/O in mind. This project is an experiment in what that integration might look like in practice: taking the execution model and running it on top of a fiber runtime where "blocking" I/O calls are actually cooperative yields.

As of right now, nobody has published a `std::execution` scheduler backed by fibers. The two ecosystems (fiber runtimes like Boost.Fiber and Marl on one side, sender/receiver implementations like stdexec and libunifex on the other) exist independently. fiberexec is an attempt to bridge them and see what falls out.

## Background

### Fibers vs threads

Threads are preemptively scheduled by the OS kernel — the kernel can interrupt any thread at any time and switch to another. Each thread carries a kernel-managed stack (typically 1–8MB) and context switches go through the kernel. This gives you true parallelism but makes threads expensive to create and switch between.

Fibers are cooperatively scheduled in userspace. A fiber only yields when it explicitly chooses to — by calling yield, a fiber-aware mutex, a channel read, or similar. The runtime swaps stack pointers in userspace, which takes nanoseconds rather than the microseconds a kernel context switch costs. Fiber stacks can be small (4–64KB), so you can have thousands or millions of them. The tradeoff: a fiber that never yields will block its OS thread, and fibers on the same thread interleave rather than run in parallel.

By running fibers on a thread pool, you get both: lightweight cooperative scheduling within each thread, and real parallelism across threads. fiberexec uses a custom per-thread Boost.Fiber scheduling algorithm backed by io_uring, so the scheduler itself never blocks on a condition variable — it sleeps directly in `io_uring_wait_cqe` and wakes only when I/O completes or new work arrives.

### std::execution (P2300)

`std::execution` is the async programming model accepted into C++26. Its core abstractions are:

- **Schedulers** — lightweight handles to an execution context (thread pool, event loop, GPU, etc.)
- **Senders** — lazy descriptions of async work that can be composed into pipelines
- **Receivers** — callbacks that handle the result (value, error, or cancellation)

You compose senders with algorithms like `then`, `when_all`, `let_value`, and `starts_on`, then launch the pipeline with `sync_wait`. The framework handles connecting everything, managing lifetimes, and propagating cancellation.

The key insight is that `std::execution` is deliberately agnostic about what the execution context actually is. A scheduler just needs to provide a `schedule()` function that returns a sender. That sender, when started, transitions to the scheduler's context and completes. Everything else composes on top. This means plugging in a fiber pool as the execution context is a natural fit — and that's exactly what fiberexec does.

### Why I/O matters here

The C++26 standard includes the core execution model but not networking or async I/O — that's being explored for C++29. But the sender/receiver design was built with I/O in mind from the start: senders naturally represent "an operation that completes later," which is exactly what an async read or write is.

Fibers make this particularly interesting because they let you write I/O code that looks synchronous (blocking calls in a straight line) while actually being async under the hood (the fiber yields, another fiber runs, the original resumes when I/O completes). Putting a sender/receiver frontend on a fiber-based I/O runtime could give you the composability and structure of `std::execution` with the ergonomics of blocking code.

Each OS thread in the fiberexec pool owns its own `io_uring` instance. When a fiber wants to do I/O, it submits a request to the thread-local ring and yields; the scheduler reaps completions and resumes the fiber with the result. From the fiber's perspective, the call looks blocking — `auto bytes = fiberexec::async_read(fd, buf, len);` — but no OS thread is ever actually blocked. This is similar in spirit to what [Seastar](https://seastar.io/) and [glommio](https://github.com/DataDog/glommio) do, but with `std::execution` as the composition layer on top.

Whether this combination is actually better than a sender-only model is the research question this project exists to explore.

## Features

### Fibers + senders

When async I/O lands in the standard, a pure sender/receiver approach to sequential I/O will look something like this — every async operation is a separate sender in the chain:

```cpp
auto work = schedule(sched)
          | async_read(fd, buf)
          | then([](auto buf) { return parse(buf); })
          | async_write(out_fd, response)
          | then([] { /* ... */ });
```

With fiberexec, the `then` lambda runs on a fiber, so you can call what looks like a blocking API that actually yields the fiber under the hood. Sequential I/O becomes straight-line code inside a fiber. fiberexec provides three ways to schedule work on the pool:

**`schedule | then` — familiar, not recommended**

The most familiar pattern for stdexec users, but cancelled I/O surfaces as
`set_error(ECANCELED)` rather than `set_stopped`, so `upon_stopped` and
`let_stopped` won't fire on cancellation:

```cpp
auto work = stdexec::schedule(sched) | stdexec::then([&] {
    auto n = fiberexec::async_read(client_fd, buf, sizeof(buf));
    fiberexec::async_write(client_fd, buf, static_cast<std::size_t>(n));
});
```

**`fiberexec::run(sched, fn)` — recommended**

The canonical fiber entry point (`run` installs the receiver's stop token as
the fiber-local stop token and maps `ECANCELED` → `set_stopped`, completing
the stdexec cancellation signal loop):

```cpp
auto work = fiberexec::run(sched, [&] {
    auto n = fiberexec::async_read(client_fd, buf, sizeof(buf));
    fiberexec::async_write(client_fd, buf, static_cast<std::size_t>(n));
});
```

**`stdexec::schedule(sched) | fiberexec::run(fn)` — pipe form**

Identical semantics to `run(sched, fn)` with the familiar stdexec pipe syntax:

```cpp
auto work = stdexec::schedule(sched) | fiberexec::run([&] {
    auto n = fiberexec::async_read(client_fd, buf, sizeof(buf));
    fiberexec::async_write(client_fd, buf, static_cast<std::size_t>(n));
});
```

Because `run` sends `set_stopped` on cancellation, it composes directly with
`upon_stopped` and `let_stopped`:

```cpp
auto result = fiberexec::run(sched, [rfd, tok = ss.get_token()] {
    std::array<char, 64> buf{};
    auto n = fiberexec::async_read(rfd, buf.data(), buf.size(), tok);
    return std::string(buf.data(), static_cast<std::size_t>(n));
}) | stdexec::upon_stopped([] { return std::string{"(timed out)"}; });
```

No intermediate senders, no continuation chains. Just sequential code that happens to be async underneath. The sender/receiver layer gets you onto the fiber pool and collects the result; once inside the fiber, you write normal code.

**`fiberexec::sync_wait(sender)` — await a sender graph from inside a fiber**

The two models complement each other. When you need structured concurrency, you drop back into senders. `fiberexec::sync_wait` lets you await a sender graph from inside a fiber without blocking the OS thread — only the calling fiber suspends:

```cpp
auto work = fiberexec::run(sched, [&] {
    // Sequential setup — just normal code
    auto n = fiberexec::async_read(config_fd, buf, sizeof(buf));
    auto endpoints = parse_endpoints(std::string_view{buf, static_cast<std::size_t>(n)});

    // Fan-out — use senders for concurrency; sync_wait suspends this
    // fiber (not the OS thread) while the inner senders run
    auto [a, b, c] = *fiberexec::sync_wait(stdexec::when_all(
        fiberexec::run(sched, [&] { return fetch(endpoints[0]); }),
        fiberexec::run(sched, [&] { return fetch(endpoints[1]); }),
        fiberexec::run(sched, [&] { return fetch(endpoints[2]); })
    ));

    return merge(a, b, c);
});
```

Fibers for sequential I/O flow, senders for structured concurrency and fan-out. The scheduler is the bridge between them.

See `examples/sync_wait_fanout.cpp` for a self-contained demonstration.

### `stdexec::bulk` — parallel fan-out

`fiberexec::scheduler` registers a `fiber_domain` that customizes `stdexec::bulk`. When you call `bulk` with `stdexec::par`, each index becomes a separate fiber dispatched across pool threads — all running concurrently, each able to do async I/O:

```cpp
fiberexec::context ctx{4};
auto sched = ctx.get_scheduler();

std::vector<std::uint32_t> results(N);

stdexec::sync_wait(
    stdexec::bulk(stdexec::schedule(sched), stdexec::par,
        static_cast<std::size_t>(N),
        [&](std::size_t i) {
            // Each index runs as an independent fiber.
            // async_recv suspends this fiber; pool threads remain free.
            fiberexec::async_recv(fds[i], &results[i], sizeof(results[i]), MSG_WAITALL);
        })
);
// All N recvs have completed here.
```

This is the P2300-idiomatic way to express runtime-variable fan-out. `stdexec::when_all` requires a compile-time-fixed set of senders; `bulk` takes the count at runtime. Swapping out the scheduler — `exec::static_thread_pool` for threads, `scheduler` for fibers — is the only change needed to get the other execution model. The `fiber_domain` customization ensures the default sequential fallback is never used.

`sync_wait` with `when_all` remains the right choice when you have a small, fixed number of heterogeneous operations and need each result as a typed value. Use `bulk` when the operations are homogeneous and the count is a runtime variable.

### `channel<T>` — producer/consumer and accept loops

`when_all` and `bulk` share a key constraint: they require the full set of concurrent operations to be known upfront. An accept loop is a *sequence* — it produces an unbounded stream of connections over time — and has no natural expression in single-shot P2300 senders.

The C++26 standard defers this. stdexec includes experimental sequence senders (`exec::sequence_senders`, `exec::iterate`, `exec::transform_each`) under `experimental::execution`, but `exec::iterate` only wraps C++ ranges, not open-ended async producers. A future `async_accept_all` sequence sender backed by `IORING_ACCEPT_MULTISHOT` is the natural end state, but the composition model for spawning concurrent handlers from an unbounded async source is still being designed.

`fiberexec::channel<T>` fills that gap today. It is a bounded MPMC channel whose blocking `push` and `pop` operations *suspend the calling fiber* rather than the OS thread — the thread stays free to run other fibers while a producer or consumer waits. This is the key property that makes it composable with the rest of fiberexec: an accept loop fiber and a worker pool can share a channel without any OS thread ever blocking on it.

The accept loop pattern uses this directly:

```cpp
fiberexec::channel<int> conn_ch{8}; // bounded queue, 7 usable slots
std::stop_source ss;

stdexec::sync_wait(stdexec::when_all(
    // Accept loop: runs until stop is requested.
    // push() suspends this fiber (not the OS thread) if the queue is full.
    fiberexec::run(sched, [&] {
        try {
            while (true) {
                int fd = fiberexec::async_accept(server_fd, nullptr, nullptr, ss.get_token());
                if (conn_ch.push(fd) != fiberexec::channel_op_status::success) {
                    ::close(fd);
                    break;
                }
            }
        } catch (std::system_error const& e) {
            if (e.code().value() != ECANCELED) throw;
        }
        conn_ch.close();
    }),
    // Worker pool: pop() suspends each fiber until a connection arrives.
    fiberexec::run(sched, [&] {
        int fd{};
        while (conn_ch.pop(fd) == fiberexec::channel_op_status::success) {
            handle_connection(fd); // async I/O inside — fiber suspends, thread stays free
        }
    }),
    // ... more workers ...
));
```

The channel capacity provides natural backpressure: if all workers are busy handling connections, `push` suspends the acceptor cooperatively rather than accepting connections that will immediately queue unhandled. Shutdown is coordinated through `close()`: the accept loop closes the channel after catching `ECANCELED`, workers drain any remaining connections, and `when_all` completes.

See `examples/echo_server_pool.cpp` for the full working server and `examples/channel_backpressure.cpp` for an isolated demonstration of the cooperative suspension behaviour under backpressure.

## Building

### Prerequisites

- CMake 3.25+
- Clang (the presets default to `clang++`)
- [vcpkg](https://github.com/microsoft/vcpkg) with `VCPKG_ROOT` set in your environment
- Linux kernel 5.10+ (io_uring)

Boost.Fiber, Boost.Context, liburing, and Catch2 are installed automatically by
vcpkg on first configure. stdexec is fetched from GitHub via CMake's
`FetchContent`.

### Configure and build

```sh
cmake --preset debug
cmake --build build/debug
```

Other presets: `release`, `asan` (AddressSanitizer + UBSan), `coverage`, `clang-tidy`.

### Run the tests

```sh
ctest --preset debug
```

Or run the test binary directly with tag filtering:

```sh
./build/debug/tests/fiberexec_tests "[networking]"
./build/debug/tests/fiberexec_tests "[sync_wait]"
./build/debug/tests/fiberexec_tests "[bulk]"
./build/debug/tests/fiberexec_tests --order rand   # randomise order
```

### Run the examples

```sh
./build/debug/examples/hello_fiber
./build/debug/examples/async_pipeline
./build/debug/examples/echo_server
./build/debug/examples/cancellation
./build/debug/examples/parallel_gather
./build/debug/examples/sync_wait_fanout
./build/debug/examples/channel_backpressure
./build/debug/examples/echo_server_pool
```

`parallel_gather` starts 16 producer threads each writing a value into its own
socketpair, then fans out 16 concurrent `async_recv` fibers via `stdexec::bulk`
to gather all results. Output looks like:

```
result[0] = 0
result[1] = 1
result[2] = 4
...
result[15] = 225
sum = 1240  (expected 1240)
```

`sync_wait_fanout` demonstrates `fiberexec::sync_wait` called from inside a
fiber. Four producer fibers each write one integer after a different delay; a
collector fiber fans out four concurrent `async_read` calls via
`fiberexec::sync_wait(when_all(...))`. Only the collector fiber suspends — the
OS thread stays free to run the producer fibers during the wait. Results arrive
out of order but are collected in original order. Total runtime ≈ max(delay) =
40 ms, not sum(delays) = 100 ms. Output looks like:

```
[+0ms] collector: submitting reads
[+10ms] wrote 200
[+20ms] wrote 400
[+30ms] wrote 300
[+40ms] wrote 100
[+40ms] collected: 100 200 300 400
sum = 1000  (expected 1000)
```

`echo_server` starts a TCP server on loopback, fans out three concurrent
client connections, and echoes each message back. Output looks like:

```
Echo server listening on 127.0.0.1:PORT
[server] echoed: "hello from client 1"
[client 1] echo: "hello from client 1"
...
Done.
```

`cancellation` demonstrates `fiberexec::run` + `stdexec::upon_stopped`. A
reader fiber tries to read from a pipe before a deadline; a timer fiber signals
cancellation if the deadline expires. Output:

```
scenario 1 (data arrives):  hello from writer
scenario 2 (timeout fires): (timed out)
```

`channel_backpressure` demonstrates cooperative producer suspension via
`channel`. A fast producer fills a small bounded channel then blocks on
`push()` until the slow consumer (`async_sleep_for` per item) makes room —
the OS thread is never blocked. Total runtime is consumer-driven, visible in
elapsed timestamps. Output:

```
[+0ms] produced 0
[+0ms] produced 1
[+0ms] produced 2
[+0ms] produced 3
[+30ms] consumed 0
[+30ms] produced 4
...
total: 360 ms  (expected ~360 ms)
```

`echo_server_pool` is a realistic TCP echo server using `channel` as a
bounded connection queue. An accept loop pushes each accepted fd into the
channel; four worker fibers drain it, each handling one connection to
completion before taking the next. The channel bounds the pending-connection
backlog and suspends the acceptor cooperatively when all workers are busy. A
`stop_source` coordinates shutdown: the last client fires `request_stop()`,
the accept loop catches `ECANCELED`, closes the channel, and workers drain and
exit cleanly.

## Benchmarks

### Building

Benchmarks are excluded from the default build. Use the `benchmark` preset,
which enables `-O3` and turns off tests and examples:

```sh
cmake --preset benchmark
cmake --build build/benchmark --target bench_scheduler bench_echo bench_fanout
```

### Running

```sh
./build/benchmark/benchmarks/bench_scheduler
./build/benchmark/benchmarks/bench_echo
```

Pass `--benchmark_repetitions=N` to collect multiple runs and report mean /
median / stddev. Pin the process to a core and disable CPU frequency scaling
for stable numbers:

```sh
# disable scaling (requires root)
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

./build/benchmark/benchmarks/bench_scheduler \
    --benchmark_repetitions=5 \
    --benchmark_report_aggregates_only=true
```

### Scheduler microbenchmarks (`bench_scheduler`)

Measured on an AMD Ryzen 7 5700X (8-core, 16 logical, ~4.7 GHz), Linux
6.x, Clang, `-O3`. All benchmarks use `UseRealTime()` — fiber work runs on
pool threads while the benchmark thread sleeps in `sync_wait`, so wall time
is the only meaningful measure.

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

### Echo server benchmarks (`bench_echo`)

Four server implementations measured side-by-side. Each iteration opens N
concurrent connections, each exchanging 100 round-trips of a 64-byte payload
over loopback TCP. Clients are always OS threads with blocking syscalls; only
the server side differs. Throughput is total round-trips per second across all
connections.

| Benchmark | Connections | Wall time / iter | Throughput |
|-----------|-------------|-----------------|------------|
| fiberexec (io_uring fibers) | 1 | ~1.35 ms | ~74.0k round-trips/s |
| fiberexec (io_uring fibers) | 10 | ~2.35 ms | ~425k round-trips/s |
| fiberexec (io_uring fibers) | 100 | ~15.7 ms | ~638k round-trips/s |
| fiberexec (io_uring fibers) | 1000 | ~140 ms | ~716k round-trips/s |
| Thread-per-connection | 1 | ~1.27 ms (median; 35% CV) | ~71.3k round-trips/s |
| Thread-per-connection | 10 | ~3.89 ms | ~257k round-trips/s |
| Thread-per-connection | 100 | ~16.5 ms | ~605k round-trips/s |
| Thread-per-connection | 1000 | — (skipped; ~2000 OS threads) | — |
| Asio coroutines (`use_awaitable`) | 1 | ~3.09 ms | ~32.4k round-trips/s |
| Asio coroutines (`use_awaitable`) | 10 | ~4.65 ms | ~215k round-trips/s |
| Asio coroutines (`use_awaitable`) | 100 | ~18.9 ms | ~530k round-trips/s |
| Asio coroutines (`use_awaitable`) | 1000 | ~161 ms | ~620k round-trips/s |
| asioexec (`exec::asio` + `use_sender`) | 1 | ~3.10 ms | ~32.3k round-trips/s |
| asioexec (`exec::asio` + `use_sender`) | 10 | ~4.70 ms | ~213k round-trips/s |
| asioexec (`exec::asio` + `use_sender`) | 100 | ~19.3 ms | ~518k round-trips/s |
| asioexec (`exec::asio` + `use_sender`) | 1000 | ~161 ms | ~621k round-trips/s |

**Asio vs asioexec** are within 1–2% at every concurrency level. The extra
`use_sender` dispatch in the accept path adds nothing measurable relative to
the I/O cost of the connections themselves. This confirms the P2300 composition
layer imposes no runtime penalty.

**fiberexec vs Asio**: fiber wins at every concurrency level — 2.3× faster at
1 connection, 2.0× at 10, 1.2× at 100 and 1000. The gap is largest at low
concurrency where per-round-trip latency dominates. Asio's coroutine path
(`co_await async_read → co_await async_write`) involves more dispatch layers per
operation (async_result machinery, executor dispatch, composed-operation
resumption) than fiberexec's direct io_uring path (`io_uring_submit → CQE →
fiber resume`). At high concurrency the gap narrows as throughput becomes
bandwidth-bound rather than latency-bound.

**Thread-per-connection** lies between the two async models at 10+ connections,
and is the only approach that cannot reach 1000 concurrent connections (~2000
OS threads required). Both fiberexec and Asio scale to 1000 connections on a
fixed pool of 16 OS threads.

**Why there is no raw io_uring thread-per-connection baseline**: a naive version
that creates one `io_uring` ring per connection is not a meaningful comparison —
ring setup (a syscall plus several mmaps) costs more than the blocking syscalls it
replaces, producing numbers worse than plain `recv`/`send`. A correct raw io_uring
baseline would need a thread pool where each thread owns one long-lived ring and
multiplexes many connections over it, processing CQEs in a loop and dispatching
completions by connection ID. That design *is* fiberexec's architecture, minus the
fiber scheduler: the same ring-per-OS-thread structure, the same event loop, the
same fan-out of connections across threads. The only difference is that fiberexec
uses fibers to express each connection's recv→send→recv cycle as straight-line code
rather than an explicit state machine. Asio's `io_context` is also this design, so
the Asio benchmarks already cover this point in the design space.

### Message-size sweep (`bench_echo`, `BM_*EchoMsgSize`)

Fixed concurrency at 10 connections; message size varies across 64 B, 512 B,
4 KB, and 64 KB. Throughput is in MiB/s or GiB/s (one direction; multiply by 2
for round-trip bandwidth). Median values across 3 repetitions.

| Benchmark | 64 B | 512 B | 4 KB | 64 KB |
|-----------|------|-------|------|-------|
| fiberexec (io_uring fibers) | ~25.6 MiB/s | ~204 MiB/s | ~1.45 GiB/s | ~9.8 GiB/s |
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
message size (5–30% ahead, gap widening with payload). The reason is not partial
reads — adding `MSG_WAITALL` to `async_recv` produces identical numbers because
data arrives atomically on loopback regardless. The gap is the io_uring
submission overhead: blocking `recv`/`send` returns immediately when data is
already in the socket buffer, while fiberexec always pays the SQE→CQE
round-trip even when the data is ready. io_uring's async path is most beneficial
when operations actually need to wait; for bandwidth-saturated loopback the
synchronous path wins.

**Asio vs asioexec** stay within 1–3% at all message sizes, consistent with
the concurrency-sweep results.

### Fan-out benchmarks (`bench_fanout`)

Compares `stdexec::bulk` on `fiberexec::scheduler` against `stdexec::bulk` on `exec::static_thread_pool`. Both use identical P2300 code; only the scheduler differs. N socketpairs are pre-created; one byte is written per pair per iteration, then N concurrent readers (fibers or thread-pool tasks) drain them via `bulk`. This isolates scheduler fan-out overhead from I/O latency.

16 × 4.7 GHz cores, 4-thread pool, 5 repetitions (`--benchmark_repetitions=5 --benchmark_report_aggregates_only=true`):

| N | Fiber (real) | Thread (real) | Fiber items/s | Thread items/s |
|---|---|---|---|---|
| 2 | 13.0 µs | 10.9 µs | 154k/s | 183k/s |
| 8 | 23.6 µs | 17.2 µs | 339k/s | 465k/s |
| 32 | 64.9 µs | 39.2 µs | 493k/s | 816k/s |
| 128 | 246 µs | 131 µs | 520k/s | 980k/s |

**Threads win, and the gap widens with N.** CPU time is nearly identical at every N (both ~101 µs at N=128); all the difference is coordination overhead. The real/CPU ratio is 2.4× for fibers vs 1.3× for threads at N=128, reflecting io_uring SQE submission, CQE delivery, and fiber context-switch cost on each of the N indices.

The core reason is that this benchmark pre-fills the socketpairs before each iteration, so every read completes immediately — there is no actual I/O latency to overlap. The fiber's io_uring round-trip is pure overhead with no benefit in this scenario. At N=128, each iteration submits 128 SQEs and harvests 128 CQEs; the thread-pool version calls `::read` 128 times on data that is already in the socket buffer.

The fiber advantage materialises when I/O actually blocks: pool threads that would otherwise stall waiting for data can instead run other fibers, keeping all cores productive. That is precisely what `bench_echo` measures. This benchmark deliberately isolates the opposite case — zero-latency reads — to characterise the raw scheduling overhead.

## Status

This is a research project and learning exercise. It is not production-ready.

## Dependencies

| Dependency    | Managed by   | Version | Purpose                           |
|---------------|--------------|---------|-----------------------------------|
| stdexec       | FetchContent | `main`  | P2300 reference implementation    |
| Boost.Fiber   | vcpkg        | 1.84+   | Cooperative fiber runtime         |
| Boost.Context | vcpkg        | 1.84+   | Low-level context switching       |
| Boost.Asio    | vcpkg        | 1.84+   | Asio benchmarks (`bench_echo`)    |
| Google Benchmark | vcpkg     | 1.8+    | Benchmark framework               |
| liburing      | vcpkg        | 2.4+    | io_uring userspace interface      |
| Catch2        | vcpkg        | 3.9+    | Test framework                    |
| C++ standard  | —            | C++20   | Concepts, constraints, coroutines |
| Linux kernel  | —            | 5.10+   | io_uring support                  |

## License

[MIT](LICENSE)
