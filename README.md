# fiberexec

A fiber-based scheduler for [stdexec](https://github.com/NVIDIA/stdexec) (P2300), built on [Boost.Fiber](https://github.com/boostorg/fiber).

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

The two models complement each other. When you need structured concurrency, you drop back into senders. `fiberexec::fiber_sync_wait` lets you await a sender graph from inside a fiber without blocking the OS thread — only the calling fiber suspends:

```cpp
auto work = fiberexec::run(sched, [&] {
    // Sequential setup — just normal code
    auto n = fiberexec::async_read(config_fd, buf, sizeof(buf));
    auto endpoints = parse_endpoints(std::string_view{buf, static_cast<std::size_t>(n)});

    // Fan-out — use senders for concurrency; fiber_sync_wait suspends this
    // fiber (not the OS thread) while the inner senders run
    auto [a, b, c] = *fiberexec::fiber_sync_wait(stdexec::when_all(
        fiberexec::run(sched, [&] { return fetch(endpoints[0]); }),
        fiberexec::run(sched, [&] { return fetch(endpoints[1]); }),
        fiberexec::run(sched, [&] { return fetch(endpoints[2]); })
    ));

    return merge(a, b, c);
});
```

Fibers for sequential I/O flow, senders for structured concurrency and fan-out. The scheduler is the bridge between them. Whether this combination is actually better than a sender-only model is the research question this project exists to explore.

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
./build/debug/tests/fiberexec_tests "[fiber_sync_wait]"
./build/debug/tests/fiberexec_tests --order rand   # randomise order
```

### Run the examples

```sh
./build/debug/examples/hello_fiber
./build/debug/examples/async_pipeline
./build/debug/examples/echo_server
./build/debug/examples/cancellation
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

## Benchmarks

### Building

Benchmarks are excluded from the default build. Use the `benchmark` preset,
which enables `-O3` and turns off tests and examples:

```sh
cmake --preset benchmark
cmake --build build/benchmark --target bench_scheduler
```

### Running

```sh
./build/benchmark/benchmarks/bench_scheduler
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
| Fiber context switch (2 fibers, 10 000 round-trips) | ~1.09 ms | ~18.3 M switches/s · **~54 ns/switch** |
| Thread context switch (semaphore ping-pong, 10 000 round-trips) | ~3.87 ms | ~5.2 M switches/s · **~193 ns/switch** |
| `run(sched, noop)` round-trip | **~6.6 µs** | — |
| `schedule(sched) \| then(noop)` round-trip | **~6.6 µs** | — |

**Fiber vs thread context switch**: fiber switches are ~3.6× faster than a
semaphore-gated thread ping-pong (~54 ns vs ~193 ns per switch). Both
measurements count two switches per "item" (A→B and B→A), so the raw
switch latency is half the per-item figure.

**Schedule overhead**: a full `run(sched, fn)` round-trip — enqueue task,
pool thread picks it up, fiber switch, execute noop, set_value, sync_wait
unblocks — costs ~6.6 µs. `schedule | then` is within noise of `run`,
confirming the extra receiver machinery in `run` adds nothing measurable.

## Status

This is a research project and learning exercise. It is not production-ready.

## Dependencies

| Dependency    | Managed by   | Version | Purpose                           |
|---------------|--------------|---------|-----------------------------------|
| stdexec       | FetchContent | `main`  | P2300 reference implementation    |
| Boost.Fiber   | vcpkg        | 1.84+   | Cooperative fiber runtime         |
| Boost.Context | vcpkg        | 1.84+   | Low-level context switching       |
| liburing      | vcpkg        | 2.4+    | io_uring userspace interface      |
| Catch2        | vcpkg        | 3.x     | Test framework                    |
| C++ standard  | —            | C++20   | Concepts, constraints             |
| Linux kernel  | —            | 5.10+   | io_uring support                  |

## License

[MIT](LICENSE)
