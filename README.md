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
 
By running fibers on a thread pool with work-stealing (which is what fiberexec does via Boost.Fiber), you get both: lightweight cooperative scheduling within each thread, and real parallelism across threads.
 
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
 
fiberexec starts as a scheduler, but the goal is to integrate [io_uring](https://kernel.dk/io_uring.pdf) as the I/O backend. The plan is for each OS thread in the fiber pool to own its own `io_uring` instance. When a fiber wants to do I/O, it submits a request to the thread-local ring and yields; the scheduler reaps completions and resumes the fiber with the result. From the fiber's perspective, the call looks blocking — `auto bytes = fiberexec::async_read(fd, buf, len);` — but no OS thread is ever actually blocked. This is similar in spirit to what [Seastar](https://seastar.io/) and [glommio](https://github.com/DataDog/glommio) do, but with `std::execution` as the composition layer on top.

### Fibers + senders: the end goal
 
When async I/O lands in the standard, a pure sender/receiver approach to sequential I/O will look something like this — every async operation is a separate sender in the chain:
 
```cpp
auto work = schedule(sched)
          | async_read(fd, buf)
          | then([](auto buf) { return parse(buf); })
          | async_write(out_fd, response)
          | then([] { /* ... */ });
```
 
With fiberexec, the `then` lambda runs on a fiber, so you can call what looks like a blocking API that actually yields the fiber under the hood. Sequential I/O becomes straight-line code:
 
```cpp
auto work = schedule(sched) | then([&] {
    auto request = fiberexec::read(client_fd, buf, len);
    auto parsed = parse(request);
    auto response = handle(parsed);
    fiberexec::write(client_fd, response.data(), response.size());
});
```
 
No intermediate senders, no continuation chains. Just sequential code that happens to be async underneath. The sender/receiver layer gets you onto the fiber pool and collects the result; once inside the fiber, you write normal code.
 
The two models complement each other. When you need structured concurrency, you drop back into senders:
 
```cpp
auto work = schedule(sched) | then([&] {
    // Sequential setup — just normal code
    auto config = fiberexec::read(config_fd, buf, len);
    auto endpoints = parse_endpoints(config);
 
    // Fan-out — use senders for concurrency
    auto reads = stdexec::when_all(
        schedule(sched) | then([&] { return fiberexec::read(endpoints[0], ...); }),
        schedule(sched) | then([&] { return fiberexec::read(endpoints[1], ...); }),
        schedule(sched) | then([&] { return fiberexec::read(endpoints[2], ...); })
    );
    auto [a, b, c] = fiberexec::fiber_sync_wait(std::move(reads));
 
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

Or run the test binary directly for more verbose output:

```sh
./build/debug/tests/fiberexec_tests --reporter console
```

### Run the examples

```sh
./build/debug/examples/hello_fiber
```

Expected output:

```
Hello from a fiber!
```

## Status
 
This is a research project and learning exercise. It is not production-ready.
 
## Dependencies
 
| Dependency    | Managed by  | Version  | Purpose                        |
|---------------|-------------|----------|--------------------------------|
| stdexec       | FetchContent| `main`   | P2300 reference implementation |
| Boost.Fiber   | vcpkg       | 1.84+    | Fiber runtime + work stealing  |
| Boost.Context | vcpkg       | 1.84+    | Low-level context switching    |
| liburing      | vcpkg       | 2.4+     | io_uring userspace interface   |
| Catch2        | vcpkg       | 3.x      | Test framework                 |
| C++ standard  | —           | C++20    | Concepts, constraints          |
| Linux kernel  | —           | 5.10+    | io_uring support               |
 
## License
 
[MIT](LICENSE)
