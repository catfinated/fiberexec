# fiberexec

io_uring async I/O with fiber ergonomics and P2300 structured concurrency.

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
  - [multishot_recv — streaming zero-copy receive](#multishot_recv--streaming-zero-copy-receive)
  - [fixed_buffer_pool and async_send_zc — zero-copy sends](#fixed_buffer_pool-and-async_send_zc--zero-copy-sends)
- [Building](#building)
  - [Prerequisites](#prerequisites)
  - [Configure and build](#configure-and-build)
  - [Run the tests](#run-the-tests)
  - [Run the examples](#run-the-examples)
- [Benchmarks](#benchmarks)
- [Status](#status)
- [Dependencies](#dependencies)
- [License](#license)

## What is this?

fiberexec is a research project exploring two things at once:

1. **Fiber-based runtimes for I/O-heavy workloads.** Fibers (cooperative, userspace-scheduled lightweight threads) let you run thousands of concurrent tasks on a handful of OS threads. When a task blocks on I/O, the fiber yields and another one picks up — no kernel context switch, no wasted thread sitting idle. This makes fibers a natural fit for servers, pipelines, and anything else that spends most of its time waiting.
2. **The `std::execution` sender/receiver model for async I/O.** The C++26 `std::execution` framework (P2300) gives us a composable, structured way to express async work — but the standard doesn't include networking or I/O yet. That's expected to come in C++29, and the sender/receiver model was explicitly designed with I/O in mind. This project is an experiment in what that integration might look like in practice: taking the execution model and running it on top of a fiber runtime where "blocking" I/O calls are actually cooperative yields.

Several prior projects are worth knowing about. [pika](https://github.com/pika-org/pika) (ETH Zurich / CSCS) publishes a `std::execution` scheduler backed by its own stackful fiber engine, aimed at HPC workloads — GPU task scheduling, MPI, and NUMA-aware thread pools — but has no async I/O layer. [helio](https://github.com/romange/helio) uses the same fiber+io_uring pattern fiberexec uses — a fiber suspends itself, stores a resume callback in the SQE's user_data, and the event loop resumes it on CQE — and ships a production-complete framework (HTTP, TLS, DNS) on top of it, but has no P2300 integration at all: its composition model is purely imperative fiber code with no senders, receivers, or structured cancellation. On the P2300+io_uring side without fibers: [libunifex](https://github.com/facebookexperimental/libunifex) ships a full io_uring scheduler with socket and file I/O, and stdexec includes a proof-of-concept io_uring scheduler with timer support — but neither brings fibers into the picture. fiberexec sits at the intersection of all three: the fiber+io_uring I/O bridge from helio's design space, the `std::execution` structured concurrency model from pika's design space, and the io_uring P2300 scheduler work from libunifex and stdexec — with `run(sched, fn)` and the `ECANCELED` → `set_stopped` mapping as the seam between the fiber and sender worlds.

| | Fibers | io_uring | P2300 (`std::execution`) |
|---|:---:|:---:|:---:|
| pika | ✓ | | ✓ |
| helio | ✓ | ✓ | |
| libunifex | | ✓ | ✓ (pre-P2300) |
| stdexec | | ✓ (PoC) | ✓ |
| fiberexec | ✓ | ✓ | ✓ |

**What fiberexec is not.** It is not a general-purpose execution runtime, not a replacement for pika, Asio, stdexec, or libunifex, and not a production server framework. It is a focused experiment at one specific design point: fibers as the local execution substrate for io_uring-backed I/O, with P2300 senders as the outer composition model. The intended framing is *fibers inside, senders outside* — you write blocking-looking sequential code inside a fiber, and that fiber participates in structured concurrency as a sender. The primary deliverable is the findings: whether this model is a good one, where it wins, and where it loses.

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

`std::execution` also integrates directly with C++20 stackless coroutines. stdexec ships `stdexec::task<T>`, a coroutine type that is itself a sender. You can `co_await` any sender from inside a task, and the task itself composes into any sender pipeline. This gives sender/receiver the sequential ergonomics of coroutines without a separate runtime: the coroutine suspends at each `co_await` and the scheduler decides when to resume it. Stackless coroutines are zero-cost at the language level (no heap allocation beyond the initial frame, no dynamic dispatch), but carry a different tradeoff from fibers. Each coroutine type must be explicitly declared, the stack depth is bounded at compile time, and `co_await`-ing a blocking call suspends only the coroutine frame, not the OS thread. fiberexec takes the complementary bet: stackful fibers that can call any blocking-looking API anywhere in the call tree, at the cost of a per-fiber stack allocation and a context switch on every yield.

### Why I/O matters here

The C++26 standard includes the core execution model but not networking or async I/O — that's being explored for C++29. But the sender/receiver design was built with I/O in mind from the start: senders naturally represent "an operation that completes later," which is exactly what an async read or write is.

Fibers make this particularly interesting because they let you write I/O code that looks synchronous (blocking calls in a straight line) while actually being async under the hood (the fiber yields, another fiber runs, the original resumes when I/O completes). Putting a sender/receiver frontend on a fiber-based I/O runtime could give you the composability and structure of `std::execution` with the ergonomics of blocking code.

Each OS thread in the fiberexec pool owns its own `io_uring` instance. When a fiber wants to do I/O, it submits a request to the thread-local ring and yields; the scheduler reaps completions and resumes the fiber with the result. From the fiber's perspective, the call looks blocking — `auto bytes = fiberexec::async_read(fd, buf);` — but no OS thread is ever actually blocked. This is similar in spirit to what [Seastar](https://seastar.io/) and [glommio](https://github.com/DataDog/glommio) do, but with `std::execution` as the composition layer on top.

Whether this combination is actually better than a sender-only model is the research question this project exists to explore.

## Features

### Fibers + senders

When async I/O lands in the standard, a pure sender/receiver approach to sequential I/O — setting aside `co_await` integration — will look like every async operation as a separate sender in the chain:

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
    auto n = fiberexec::async_read(client_fd, buf);
    fiberexec::async_write(client_fd, buf.first(static_cast<std::size_t>(n)));
});
```

**`fiberexec::run(sched, fn)` — recommended**

The canonical fiber entry point (`run` installs the receiver's stop token as
the fiber-local stop token and maps `ECANCELED` → `set_stopped`, completing
the stdexec cancellation signal loop):

```cpp
auto work = fiberexec::run(sched, [&] {
    auto n = fiberexec::async_read(client_fd, buf);
    fiberexec::async_write(client_fd, buf.first(static_cast<std::size_t>(n)));
});
```

**`stdexec::schedule(sched) | fiberexec::run(fn)` — pipe form**

Identical semantics to `run(sched, fn)` with the familiar stdexec pipe syntax:

```cpp
auto work = stdexec::schedule(sched) | fiberexec::run([&] {
    auto n = fiberexec::async_read(client_fd, buf);
    fiberexec::async_write(client_fd, buf.first(static_cast<std::size_t>(n)));
});
```

Because `run` sends `set_stopped` on cancellation, it composes directly with
`upon_stopped` and `let_stopped`:

```cpp
auto result = fiberexec::run(sched, [rfd, tok = ss.get_token()] {
    std::array<char, 64> buf{};
    auto n = fiberexec::async_read(rfd, std::as_writable_bytes(std::span{buf}), std::nullopt, tok);
    return std::string(buf.data(), static_cast<std::size_t>(n));
}) | stdexec::upon_stopped([] { return std::string{"(timed out)"}; });
```

No intermediate senders, no continuation chains. Just sequential code that happens to be async underneath. The sender/receiver layer gets you onto the fiber pool and collects the result; once inside the fiber, you write normal code.

**`fiberexec::sync_wait(sender)` — await a sender graph from inside a fiber**

The two models complement each other. When you need structured concurrency, you drop back into senders. `fiberexec::sync_wait` lets you await a sender graph from inside a fiber without blocking the OS thread — only the calling fiber suspends:

```cpp
auto work = fiberexec::run(sched, [&] {
    // Sequential setup — just normal code
    auto n = fiberexec::async_read(config_fd, buf);
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
            fiberexec::async_recv(fds[i], std::as_writable_bytes(std::span{&results[i], 1}), MSG_WAITALL);
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
fiberexec::channel<int> conn_ch{8}; // bounded queue, backlog of 8
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

### `multishot_recv` — streaming zero-copy receive

`multishot_recv` arms a single `IORING_RECV_MULTISHOT` SQE against a kernel-managed buffer ring (`IORING_REGISTER_PBUF_RING`). The kernel selects a free buffer slot, fills it with incoming data, and delivers a CQE — with no copy through an intermediate kernel buffer. The SQE stays armed; the kernel re-uses it for subsequent data without the fiber needing to resubmit.

```cpp
fiberexec::multishot_recv mr{conn_fd, /*buf_size=*/4096, /*buf_count=*/256};
while (auto buf = mr.next()) {   // suspends fiber until data or EOF
    process(buf->data());        // zero-copy span into the kernel buffer slot
    // buf destroyed here → slot returned to the kernel ring
}
```

`next()` returns `std::nullopt` on EOF or connection close, making it natural to drive with a `while` loop. Because the kernel fills each buffer slot with however much data is in the socket receive buffer at selection time, the number of CQEs is often fewer than the number of sends — multiple small sends that arrive before the kernel selects the next slot land in a single buffer and produce one CQE.

`multishot_recv` requires Linux 6.0+. See `examples/stream_recv_multishot.cpp` for a self-contained demonstration.

### `fixed_buffer_pool` and `async_send_zc` — zero-copy sends

`fixed_buffer_pool` pre-registers a set of user-space buffers with the kernel once via `IORING_REGISTER_BUFFERS`. Subsequent sends reference them by index rather than pointer, eliminating the per-operation memory pin/unpin that ordinary sends incur. `async_send_zc` submits `IORING_OP_SEND_ZC` with `IORING_RECVSEND_FIXED_BUF`, reading directly from the registered buffer without an intermediate copy.

```cpp
fiberexec::fixed_buffer_pool pool{/*buf_size=*/64, /*buf_count=*/8};

for (int i = 0; i < kMessages; ++i) {
    auto fb = pool.borrow();          // suspends fiber if all slots are in flight
    fill(fb.data(), i);               // write directly into the registered buffer
    fiberexec::async_send_zc(fd, fb, fb.data().size());
    // fb destroyed → slot returned to pool, ready for the next borrow
}
```

`borrow()` suspends the calling fiber (not the OS thread) when all slots are in flight, providing natural backpressure. `async_send_zc` waits for both the send completion CQE and the kernel's buffer-release notification CQE before returning, ensuring the buffer is safe to reuse. Only one `fixed_buffer_pool` may be active per worker thread at a time (the kernel allows one registered buffer table per ring); constructing a second throws `std::system_error(EBUSY)`.

Combined with `multishot_recv`, these two primitives eliminate the userspace copy on both ends of a streaming path: no intermediate kernel socket buffer on the send side, and no copy-out from a kernel receive buffer on the receive side. On a real NIC the kernel can DMA directly from the sender's pinned pages to the wire and from the wire into the receiver's buffer ring slot; on loopback there is no DMA and the kernel still copies between the two memory regions.

`async_send_zc` requires Linux 6.0+ and a TCP socket (AF_UNIX is not supported). See `examples/stream_recv_multishot.cpp` for the full zero-copy pipeline.

## Building

### Prerequisites

- CMake 3.25+
- Clang (the presets default to `clang++`)
- [vcpkg](https://github.com/microsoft/vcpkg) with `VCPKG_ROOT` set in your environment
- Linux kernel 6.0+ (io_uring with multishot recv)

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
./build/debug/examples/echo_to_file
./build/debug/examples/echo_server_multishot
./build/debug/examples/stream_recv_multishot
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

`echo_to_file` extends the pool pattern with `async_openat` and `async_close`:
instead of echoing data back to the client, each worker fiber asynchronously
opens a dedicated log file (`connection_N.log`), writes all received bytes to
it, then asynchronously closes it. The number of connections is not known
upfront. From inside the fiber, open, write, and close are three sequential
statements that each yield the fiber without blocking the OS thread.

`echo_server_multishot` is the multishot-acceptor variant of `echo_server_pool`.
One `IORING_ACCEPT_MULTISHOT` SQE stays armed in the ring; the kernel delivers
one CQE per accepted connection without the fiber resubmitting, reducing
round-trips through the ring compared to a loop of individual `async_accept`
calls.

`stream_recv_multishot` demonstrates the full zero-copy pipeline: a producer
fiber sends 10,000 × 64-byte records via `fixed_buffer_pool` + `async_send_zc`;
a consumer fiber drains the stream with `multishot_recv`. Neither side copies
data through an intermediate buffer. Output reports total bytes received and
CQE count — typically fewer CQEs than sends because the kernel batches data into
buffer slots on the receive side:

```
stream_recv_multishot: 10000 records × 64 bytes  (buf ring: 256 × 4096)
received 640000 / 640000 bytes across 9984 CQEs — OK
```

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
./build/benchmark/benchmarks/bench_fanout
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

See [FINDINGS.md](FINDINGS.md) for full results and analysis, including the
raw io_uring baseline comparison that isolates the fiber+P2300 overhead from
the io_uring machinery itself.

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
| Linux kernel  | —            | 6.0+    | io_uring with multishot recv      |

## License

[MIT](LICENSE)
