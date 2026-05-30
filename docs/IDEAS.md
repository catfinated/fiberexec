# Ideas and future investigations

A running list of things to build, explore, or research. Not prioritised — just
a capture of everything that came up during development or that seems worth
investigating.

---

## Deferred implementation work

These are concrete items with designs already sketched in the ADRs.

### ~~ADR-0001 step 4 — ECANCELED → `set_stopped` mapping~~ ✅ done

Implemented as `fiberexec::run(sched, fn)` (Option B from ADR-0001).
See `include/fiberexec/run.hpp` and `examples/cancellation.cpp`.

### ~~ADR-0002 step 3 — `channel<T>`~~ ✅ done

Implemented as a thin wrapper over `boost::fibers::buffered_channel<T>` with
a fiberexec-namespaced `channel_op_status` enum. See
`include/fiberexec/channel.hpp`, `tests/test_channel.cpp`, and
`examples/channel_backpressure.cpp`.

---

## Benchmarks

The benchmark suite is the primary deliverable of the project; hardening it is
the highest-leverage work remaining.

### ~~Raw io_uring baseline~~ ✅ done

The current benchmarks compare fiberexec against thread-per-connection, Asio
coroutines, and asioexec, but there is no hand-rolled io_uring control — a
minimal event loop with one ring per thread and no fibers or P2300 overhead.
Without it, the benchmark cannot isolate the cost of fibers from the cost of
io_uring itself, and a systems audience will probe this first.

### ~~Latency distributions (p50 / p99 / p999)~~ ✅ done

Implemented in `benchmarks/bench_latency.cpp`. `UseManualTime()` + per-iteration
`SetIterationTime` accumulates individual round-trip timings; p50/p99/p999 are
sorted and reported as custom counters after the loop. Key finding: fiberexec
tail latency (p999) is tighter than thread-per-connection at every concurrency
level and does not degrade with connection count, while thread tails worsen
steadily. See `FINDINGS.md` for full tables and analysis.

### ~~Non-loopback workload~~ ✅ done

Implemented as `benchmarks/bench_delay.cpp`. A 1 ms server-side
`async_sleep_for` between recv and send simulates a slow upstream call without
requiring `tc netem` or cross-host setup. At 1000 connections fiberexec
sustains 268× the single-connection throughput; Asio collapses 15× from its
100-connection performance (likely timer-queue contention — flagged as an open
investigation in FINDINGS.md). See FINDINGS.md for full results and analysis.

### Head-to-head against pika on an I/O workload

Running pika's fiber scheduler against fiberexec on a socket workload would
demonstrate that pika's fibers are tuned for compute rather than I/O. It turns
the "but pika exists" question into a supporting data point and directly answers
"why does this exist when pika exists?" with numbers.

---

## io_uring features

### ~~Timeouts on individual async ops~~ ✅ done

`IORING_OP_LINK_TIMEOUT` attaches a deadline to any SQE via `IOSQE_IO_LINK`.
All async ops now accept `std::optional<std::chrono::nanoseconds> timeout = std::nullopt`.
`submit_and_wait_with_timeout` in `src/fiber_context.cpp` sets `IOSQE_IO_LINK`
on the op SQE, submits a linked timeout SQE tagged with `k_cancel_tag`, and
submits both atomically. Timeout fires → op gets `-ECANCELED` (throws
`std::system_error(ECANCELED)`). Buffers were also migrated to
`std::span<std::byte>` / `std::span<std::byte const>` in the same commit.

### ~~Multi-shot accept (`IORING_ACCEPT_MULTISHOT`)~~ ✅ done

Implemented as `fiberexec::multishot_acceptor` (`include/fiberexec/multishot_acceptor.hpp`,
`src/multishot_acceptor.cpp`). One SQE stays armed for the lifetime of the
object; the scheduler rearms it automatically when `IORING_CQE_F_MORE` is
absent (resource pressure). `next()` suspends the calling fiber until a
connection arrives, draining any buffered fds from prior CQEs before blocking.
See `examples/echo_server_multishot.cpp` for a drop-in replacement of the
`echo_server_pool` accept loop.

The implementation prompted a general CQE dispatch refactor: `drain_cqes()`
now routes all non-sentinel CQEs through a `detail::cqe_handler` virtual
dispatch rather than casting to `io_awaitable*` directly. This makes adding
`IORING_RECV_MULTISHOT` and `IORING_POLL_ADD_MULTI` purely additive — new
subclasses with no changes to `drain_cqes()`. See ADR-0003.

The sequence-sender angle (a sender that emits `set_next(fd)` once per
connection) remains an open research question. `exec::merge_each` (stdexec
experimental) is the intended combinator but its interaction with an unbounded
async source is not yet proven in practice. The `multishot_acceptor` + bounded
`channel` + fixed worker pool pattern is the correct approach today and would
be the reference point for any future sequence-sender port.

### Registered buffers / buffer rings

`io_uring_register_buffers` pins user buffers so the kernel can DMA directly
without an extra copy on each op. For high-throughput recv/send this can matter.
`IORING_OP_PROVIDE_BUFFERS` (buffer rings) goes further: lets the kernel pick
from a pool of pre-registered buffers, avoiding a recv-per-fiber allocation.
Both are niche but interesting to benchmark.

### Fixed file descriptors (`io_uring_register_files`)

Registered fds skip the per-op fd table lookup in the kernel. For a server that
holds a large, stable set of open connections this is a measurable win.

### Linked SQEs (`IOSQE_IO_LINK`)

Linked SQEs let you chain operations that the kernel executes sequentially
without round-tripping through userspace. A `send` followed immediately by a
`recv` could be submitted as a linked pair. Useful for request/response
protocols where round-trip latency matters.

---

## Scheduling and pool behaviour

### ~~What happens when a fiber throws an unhandled exception?~~ ✅ done

`run_sender::operation::start` (in `include/fiberexec/run.hpp`) catches all
exceptions at the fiber entry point: `std::system_error` with `ECANCELED` maps
to `set_stopped`; any other exception is forwarded as `set_error(exception_ptr)`.
`boost::context::detail::forced_unwind` (injected by Boost.Fiber to unwind
suspended fiber stacks on destruction) is explicitly re-thrown before the
catch-all so the runtime can complete its stack unwinding. No exception escapes
to `std::terminate` when using the canonical `fiberexec::run` entry point.

**Design constraint**: `set_stopped` is called on the receiver before
re-throwing, preserving the P2300 guarantee that every started operation
completes exactly once. All fibers should still be driven to completion through
normal paths (value, error, or stop via cancellation token) before the pool
destructor runs — relying on force-unwind as a cancellation mechanism is a
design smell. The `echo_server_pool` shutdown sequence (stop token →
`ECANCELED` → channel close → workers drain) is the correct pattern.

### ~~Fiber stack size configuration~~ ✅ done

`context` now accepts a `stack_size` second constructor parameter
(default `context::default_stack_size` = 128 KiB, matching `boost::context::stack_traits::default_size()`). The value is
threaded through `fiber_pool` and `io_uring_scheduler` and used via
`std::allocator_arg, boost::fibers::fixedsize_stack{stack_size_}` at
each fiber launch site.

### ~~Graceful shutdown drain~~ ✅ done

`fiber_pool` tracks live user fibers with an `in_flight_` atomic counter.
`launch_fiber()` wraps each fiber to decrement the counter on completion and
signal `shutdown_cv_`; `worker()` waits for both `running_ == false` and
`in_flight_ == 0` before exiting. A pool-level `stop_source` is also wired
into `submit_and_wait` so all pending I/O is cancelled before the drain
completes. The pool now guarantees every started fiber reaches a terminal
completion before the context destructs.

### Lazy stop-token installation

ADR-0001 notes that `fiber_specific_ptr` incurs a heap allocation per fiber that
installs a stop token. If the receiver's token is not stoppable (e.g. `sync_wait`
with no external cancellation source), the `std::stop_source` starts with
`std::nostopstate` and no stop callback is registered. Verify that this path
actually skips the `fiber_specific_ptr` heap allocation, or make it do so
explicitly.

### Thread pool sizing

`context` currently takes a fixed thread count. Worth investigating:
- What is the right default? `std::thread::hardware_concurrency()`?
- Should the pool be allowed to grow dynamically under load?
- Is there a meaningful interaction between fiber count and thread count for
  I/O-heavy vs CPU-heavy workloads?

---

## API and ergonomics

### ~~A realistic echo server with a proper accept loop~~ ✅ done

Implemented as `examples/echo_server_pool.cpp`. The accept loop runs
indefinitely, pushing accepted fds into a `channel<int>`; a fixed pool
of worker fibers drains the channel. Channel capacity bounds the connection
backlog and provides backpressure to the acceptor when all workers are busy.
Shutdown is coordinated server-side: the last test client calls
`::shutdown(server_fd, SHUT_RDWR)`, which causes `async_accept` to throw;
the accept loop catches the error, closes the channel, and the worker pool
drains and exits cleanly.

### `async_read` / `async_write` vs `async_recv` / `async_send`

Currently both sets exist. The distinction (`read`/`write` work on any fd;
`recv`/`send` are socket-specific and carry flags) is standard POSIX, but it is
worth documenting clearly and deciding if fiberexec should expose the flags
parameter on `recv`/`send`.

### ~~`std::expected`-based error API~~ — won't do

Exceptions are the correct model for fiber call sites. The fiber value
proposition is sequential code that reads like blocking calls; pervasive
`expected` checks at every `async_read`/`async_write` call site would
undermine that. Cancellation also integrates cleanly through the exception
path — `ECANCELED` throws, `run` catches and maps to `set_stopped` — which
would require a separate convention with `expected`. Additionally,
`std::expected` requires C++23, and the project targets C++20.

---

## CUDA Integration

It is worth noting upfront that nvexec (part of stdexec) already provides
first-class P2300 support for NVIDIA GPUs: CUDA streams as schedulers, sender
algorithms that execute on the GPU, and proper cancellation. Anyone who needs
production-grade GPU + P2300 integration should start there. The exploration
below is about a different, narrower question.

The goal here is not to replicate what pika, HPX, or nvexec already do well — scheduling
CPU-bound compute tasks across fibers and accelerators. CPU-heavy workloads are
not the focus of fiberexec. The interesting angle is narrower: from userspace,
GPU communication looks a lot like any other I/O. A `cudaMemcpyAsync` followed
by a kernel launch and a device-to-host transfer is conceptually a sequence of
async operations waiting on completion events — exactly the pattern fiberexec
already handles for sockets and files. Whether this has been done elsewhere with
io_uring as the notification path is worth investigating; the point is not to
copy an existing design but to explore how these puzzle pieces fit together.

The mechanism that makes it plausible: each GPU operation submits work to a CUDA
stream, then registers a host callback via `cudaLaunchHostFunc`. The callback
runs on a CUDA-internal thread and writes to the owning worker thread's
notification eventfd. This generates an io_uring CQE, the scheduler wakes up,
and the fiber resumes with the result. No new mechanisms are needed beyond what
already exists — the same eventfd and io_uring wait path that handles network
and file I/O handles GPU completions too.

From the fiber's perspective, GPU work looks like sequential, blocking code. A
series of allocations, uploads, kernel launches, and downloads reads like
textbook CUDA — but each call yields the fiber under the hood, freeing the
thread to run other fibers while the GPU works. Multiple independent GPU
workloads can interleave on the same thread pool with no coordination code.

```cpp
// A single fiber doing a vector add — reads like synchronous CUDA,
// but yields at every GPU operation.
stdexec::sync_wait(fiberexec::run(sched, [&] {
    float *d_a, *d_b, *d_c;
    cudaMalloc(&d_a, N * sizeof(float));
    cudaMalloc(&d_b, N * sizeof(float));
    cudaMalloc(&d_c, N * sizeof(float));

    cudaStream_t stream;
    cudaStreamCreate(&stream);

    // Each of these yields the fiber until the GPU operation completes.
    fiberexec::gpu_memcpy_async(d_a, h_a.data(), N * sizeof(float),
                                cudaMemcpyHostToDevice, stream);
    fiberexec::gpu_memcpy_async(d_b, h_b.data(), N * sizeof(float),
                                cudaMemcpyHostToDevice, stream);

    fiberexec::gpu_launch(vec_add_kernel, {blocks, threads}, stream,
                          N, d_a, d_b, d_c);

    fiberexec::gpu_memcpy_async(h_c.data(), d_c, N * sizeof(float),
                                cudaMemcpyDeviceToHost, stream);

    // h_c now contains the result.
    cudaFree(d_a);
    cudaFree(d_b);
    cudaFree(d_c);
    cudaStreamDestroy(stream);
}));
```

```cpp
// Multiple independent GPU workloads interleaving on the same thread pool.
// Each fiber writes sequential code; the scheduler handles concurrency.
stdexec::sync_wait(stdexec::when_all(
    fiberexec::run(sched, [&] { vec_add_on_gpu(a1, b1, c1); }),
    fiberexec::run(sched, [&] { vec_add_on_gpu(a2, b2, c2); }),
    fiberexec::run(sched, [&] { vec_add_on_gpu(a3, b3, c3); })
));
```

---

## Research questions

### Comparison with Seastar and glommio

Both Seastar (C++) and glommio (Rust) are io_uring-based runtimes with
per-thread rings and no cross-thread I/O. fiberexec currently allows any fiber
to submit I/O to the thread-local ring regardless of which thread it landed on,
which is correct — but it is worth studying how Seastar/glommio handle
cross-shard I/O and what their scheduling invariants are. The comparison might
surface constraints or optimisations I haven't considered.

### Coroutine integration

C++20 coroutines (`co_await`) and P2300 senders are both models for async
composition. stdexec has `stdexec::task` (a coroutine-based sender). It would
be interesting to explore whether fiberexec fibers and C++20 coroutines can
coexist — specifically, whether a coroutine running on a fiberexec worker could
`co_await` a sender and have that suspend the coroutine (not the fiber/thread).

### ~~Interaction with `stdexec::when_all` cancellation at scale~~ ✅ done

Implemented as the `"cancel queue drains correctly under load with many
concurrent async_recv operations"` stress test in `tests/test_networking.cpp`.
Fans out 100 fibers blocked on `async_recv`; a trigger thread fires
`request_stop()` after 10 ms and all 100 cancellations are verified.

---

## Housekeeping

### Isolate `fiber_domain` stdexec internal header dependency

`fiber_domain`'s `transform_sender` customization reaches into stdexec's
internal headers. This will break silently on stdexec updates. The shim should
be isolated behind a version-pinned boundary so breakage is contained and easy
to repair.

### ~~README scope disclaimer~~ ✅ done

Added a "What fiberexec is not" paragraph to the README immediately after the
comparison table. States explicitly that it is not a general-purpose runtime or
a replacement for pika, Asio, stdexec, or libunifex, and frames the deliverable
as findings rather than a library. The design point is summarised as "fibers
inside, senders outside."

### ~~Sanitizer coverage~~ ✅ done

A `tsan` CMake preset was added (`CMakePresets.json`). Boost.Fiber's userspace
spinlock and condvar are invisible to TSan and suppressed via
`.tsan-suppressions`. One real race in `sync_wait` (`done` bool under a
fiber mutex) was fixed with `std::atomic<bool>` and release/acquire ordering.

