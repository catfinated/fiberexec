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

**Update**: a head-to-head echo throughput benchmark was implemented 
out of band. Short version: fiberexec wins at high
concurrency (643 k vs 479 k round-trips/s at 1000 connections), but the
comparison is not as clean as originally framed.

The core problem is that pika has **no async I/O layer** — connection handlers
use blocking `::recv`/`::send`, so every in-flight connection occupies a pool OS
thread. fiberexec wins because io_uring multiplexes all connections
concurrently regardless of pool size, not because of anything specific to its
scheduler or P2300 integration. The result is predetermined: any async I/O
runtime beats blocking I/O at scale. This makes the benchmark more of an
illustration of *why async I/O exists* than a scheduler comparison.

A few additional nuances from the results:
- At 100 connections, **thread-per-connection beats both** (628 k/s vs fiberexec
  596 k, pika 461 k). Pika's work-stealing task dispatch adds overhead on top of
  blocking I/O that raw OS threads don't pay.
- fiberexec's throughput *increases* from 100 → 1000 connections (io_uring
  batches more CQEs per wakeup with higher fan-out). Pika plateaus once the pool
  is saturated.
- For **CPU-bound bulk work** the comparison would flip. Pika's
  `thread_pool_scheduler::bulk` has carefully tuned work-stealing, chunk sizing,
  and NUMA-aware placement that fiberexec's fiber-based bulk does not attempt.
  fiberexec's bulk advantage is specific to workloads where invocations do async
  I/O and yield.

The more meaningful comparison for fiberexec's scheduler and P2300 integration
is Asio (already in `benchmarks/bench_echo.cpp`) — both target async I/O, both
have P2300 wrappers, and the results there are not predetermined.

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

### ~~Multi-shot recv (`IORING_RECV_MULTISHOT`)~~ ✅ done

Implemented as `fiberexec::multishot_recv` (`include/fiberexec/multishot_recv.hpp`,
`src/multishot_recv.cpp`). One SQE stays armed; each arriving message causes the
kernel to select a buffer from a pre-registered buffer ring, write data into it,
and deliver a CQE. `next()` returns a `received_buffer` RAII handle whose
`data()` span points directly into the kernel-selected buffer — zero extra copies.
The buffer is returned to the ring when the handle is destroyed. `next()` returns
`nullopt` on EOF, cancellation, or `EINVAL`. The destructor cancels any in-flight
SQE and drains remaining data automatically.

### ~~Kernel buffer rings~~ ✅ done (via multishot_recv)

`multishot_recv` uses `io_uring_setup_buf_ring`, which registers a shared
ring buffer via `IORING_REGISTER_PBUF_RING` — the newer mmap-based mechanism
introduced in kernel 5.19. The kernel selects a slot from the ring for each
arriving message and writes data directly into it; userspace accesses the data
via `received_buffer::data()` with no intermediate copy and no per-recv heap
allocation. Buffers are returned to the ring when the `received_buffer` handle
is destroyed.

The older `IORING_OP_PROVIDE_BUFFERS` SQE-based approach requires a round-trip
through the submission queue to replenish the pool; `IORING_REGISTER_PBUF_RING`
avoids this entirely by sharing the ring between kernel and userspace via a
memory mapping.

`io_uring_register_buffers` (`IORING_REGISTER_BUFFERS`) is implemented as
`fiberexec::fixed_buffer_pool`. Buffers are pinned once at construction;
`async_send_zc` references them by index via `IORING_OP_SEND_ZC` with
`IORING_RECVSEND_FIXED_BUF`, eliminating the per-op memory pin/unpin on the
send side. See ADR-0004.

### ~~Fixed file descriptors (`io_uring_register_files`)~~ ✅ done

Implemented as `fiberexec::fixed_fd_table` and `fiberexec::fd_slot`
(`include/fiberexec/fixed_fd_table.hpp`, `src/fixed_fd_table.cpp`). The table
is a per-thread singleton configured via `context_options::registered_fd_capacity`;
fibers call `acquire_fd_slot(fd)` to borrow a slot, then pass the returned
`fd_slot` directly to `async_recv` / `async_send` / `async_read` / `async_write`
/ `async_connect` (implicit conversion to `fixed_fd` sets `IOSQE_FIXED_FILE` on
each SQE). `fd_slot::update(new_fd)` swaps the registered fd without releasing
the slot — the key primitive for the file-rotation pattern where a fiber
periodically closes its current output file and opens the next one. Slots are
returned to the per-thread free list on `fd_slot` destruction.

The design and the decision to scope both `fixed_fd_table` and `fixed_buffer_pool`
as thread-local singletons (rather than fiber-scoped objects) are documented in
ADR-0005.

### ~~Linked SQEs (`IOSQE_IO_LINK`)~~ ✅ done

Implemented as `detail::submit_linked_and_wait` in `src/fiber_context.cpp`, with
two public-API entry points in `src/async_io.cpp`:

- **`async_send_recv(fd, send_buf, recv_buf)`** — submits send + recv as a linked
  pair on one fd; returns `std::pair<ssize_t, ssize_t>`. One `io_uring_submit`
  call, one fiber suspension. Useful for request/response protocols (ping/pong,
  HTTP/1.1 pipelining) where the round-trip through userspace would add latency.
- **`async_write_fsync(fd, buf)`** — submits write + fsync as a linked pair;
  returns the byte count of the write. The fsync starts the instant the write
  completes without waiting for userspace to re-enter the ring.

`submit_linked_and_wait` is generic: it accepts a `std::span<io_uring_sqe*>` of
up to `k_max_linked_ops` (8) SQEs, sets `IOSQE_IO_LINK` on all but the last,
submits them atomically, and suspends the calling fiber until all N CQEs arrive.
Cancellation is fully supported: both the pool-wide and fiber-local stop tokens
cancel all N awaitables (completed ones return `-ENOENT` and are silently
discarded; the in-flight one is cancelled and the kernel cascades `-ECANCELED`
to subsequent linked SQEs). See `tests/test_linked.cpp` for the test suite.

### `IORING_OP_MSG_RING` — replace eventfd inter-thread wakeup

Each fiber context currently uses an eventfd to wake the io_uring event loop on a
different thread (e.g. when a channel push unblocks a waiting fiber on another
thread).  `IORING_OP_MSG_RING` posts a CQE directly into another ring without a
round-trip through the kernel's fd table — no `write(eventfd)` syscall, no
`read(eventfd)` SQE on the receiving side.  Replacing the eventfd wakeup path
with `MSG_RING` removes two syscalls per cross-thread notification.

### `IORING_SETUP_DEFER_TASKRUN` / `IORING_SETUP_COOP_TASKRUN`

By default the kernel may deliver task_work (CQE completions) to the application
thread at any point, including inside unrelated syscalls.  `COOP_TASKRUN`
suppresses involuntary delivery; `DEFER_TASKRUN` (kernel 6.1+) goes further and
defers all task_work until the application explicitly calls into io_uring.
Together these reduce context switches and can lower tail latency.  Low
implementation cost — ring setup flags only.

### `IORING_SETUP_SQPOLL`

A kernel thread polls the submission ring continuously, eliminating the
`io_uring_submit` syscall on the hot path entirely.  At very high submission
rates this removes measurable overhead.  Trade-off: the kernel thread spins,
burning a CPU even during idle periods.  Should be opt-in via a `context`
constructor flag rather than the default.  Requires `CAP_SYS_NICE` on older
kernels (relaxed in later versions).

### `IORING_OP_SPLICE` — zero-copy fd-to-fd transfer

`splice(2)` moves data between two file descriptors through the kernel page cache
without copying to userspace.  io_uring exposes this as `IORING_OP_SPLICE`.
Useful for proxy workloads (pipe accepted socket data to an upstream fd) and file
serving (splice a file fd directly to a socket).  No equivalent exists in the
current API; adding `async_splice(fd_in, fd_out, nbytes)` would cover these cases.

### `IORING_OP_ACCEPT_DIRECT` / `IORING_OP_OPENAT_DIRECT`

Accept a connection or open a file directly into a slot in the registered fd
table, skipping the `io_uring_register_files_update` call that currently follows
every `async_accept` / `async_openat` when fixed fds are needed.  io_uring
supports this natively; the gap is on the fiberexec side — the fixed-fd free-list
design would need to reserve a slot before the op and commit it on completion.
Noted in ADR-0006 as a known limitation of the current table design.

### `IORING_OP_FUTEX_WAIT` / `IORING_OP_FUTEX_WAKE`

Async futex operations available since kernel 6.7.  Speculative: could serve as
a lower-level building block for fiber synchronization primitives (channel, mutex)
that suspend in io_uring rather than in Boost.Fiber's scheduler, avoiding a
fiber context switch on the unblocking side.  Worth prototyping once the kernel
version is common enough.

### `IORING_OP_SOCKET`

Create a socket via io_uring rather than a plain `socket(2)` syscall.  On its own
the benefit is modest, but it enables chaining with `IORING_OP_CONNECT` via
linked SQEs — socket creation + connect in one submission, with the result landing
directly in the fixed fd table via the `_DIRECT` variant.

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

### ~~Lazy stop-token installation~~ ✅ done

`install_fiber_stop_token` previously allocated unconditionally via
`new std::stop_token(...)` even when passed a non-stoppable token (the common
case when `sync_wait` is used without an external stop source).
`current_fiber_stop_token()` already returns `std::stop_token{}` when the
fiber-local pointer is null — behaviorally identical to a non-stoppable token —
so the fix was a one-line guard: `if (!tok.stop_possible()) return;` at the top
of `install_fiber_stop_token`. Fibers launched without a stoppable receiver now
incur no heap allocation for stop-token machinery.

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

### ~~`async_read` / `async_write` vs `async_recv` / `async_send`~~ — won't do / documented

Both sets are intentionally kept. The distinction follows POSIX:

- `async_read` / `async_write` — work on any file descriptor (files, pipes,
  sockets). No flags parameter; maps directly to `IORING_OP_READ` /
  `IORING_OP_WRITE`.
- `async_recv` / `async_send` — socket-specific. Expose a `flags` parameter
  passed through to the underlying recv/send (e.g. `MSG_WAITALL`, `MSG_NOSIGNAL`,
  `MSG_PEEK`). Maps to `IORING_OP_RECV` / `IORING_OP_SEND`.

This item was written when `async_recv`/`async_send` had no flags parameter and
were therefore redundant with `async_read`/`async_write`. The flags parameter
has since been added, making the distinction both correct and necessary.

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

### ~~Isolate `fiber_domain` stdexec internal header dependency~~ ✅ done

stdexec provides no public API for sender decomposition, so two layout
assumptions in `fiber_domain::transform_sender` (structured bindings into the
`bulk_chunked_t` sender and its data) are unavoidable. The risk is now
contained:

- stdexec is pinned to a specific commit in `cmake/Dependencies.cmake`.
  Breakage on an stdexec update is a loud compile error at a clearly marked
  site rather than a silent misbehaviour at runtime.
- `stdexec::__sender_for` replaced with `exec::sender_for` from
  `<exec/sender_for.hpp>` — the stdexec-blessed public replacement.
- A `STDEXEC INTERNAL DEPENDENCY` comment block in `fiber_bulk.hpp` names the
  pinned commit, documents each layout assumption and where it appears, and
  tells a future maintainer exactly what to verify when advancing the pin.

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

