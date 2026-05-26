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

## io_uring features

### Timeouts on individual async ops

`IORING_OP_LINK_TIMEOUT` lets you attach a deadline to any SQE by setting
`IOSQE_IO_LINK` on the op SQE and submitting a timeout SQE immediately after.
If the op doesn't complete in time, io_uring cancels it and completes both with
`-ECANCELED` / `-ETIME`. This would give `async_recv`, `async_accept`, etc. a
natural `deadline` or `timeout` overload without a separate cancel path:

```cpp
fiberexec::async_recv(fd, buf, len, std::chrono::seconds{5});
```

Needs care around SQE ordering — the timeout SQE must be submitted atomically
with the op SQE.

### Multi-shot accept (`IORING_ACCEPT_MULTISHOT`)

Today `async_accept` submits one SQE per accepted connection. For a server with
many short-lived connections this is wasteful. `IORING_ACCEPT_MULTISHOT` (5.19+)
keeps a single SQE in flight and posts a CQE for each new connection. This
requires a different API shape (the call doesn't return a single fd; it's more
like a generator) but would dramatically reduce SQE overhead for accept-heavy
workloads.

`IORING_ACCEPT_MULTISHOT` is also the natural kernel primitive for a future
P2300 *sequence sender* — a sender that emits `set_next(fd)` once per
accepted connection and terminates only on error or cancellation. Sequence
senders are under active development in stdexec under `experimental::execution`
(sequence_senders.hpp). `exec::iterate` exists today but only wraps C++ ranges,
not open-ended async producers. Bridging `IORING_ACCEPT_MULTISHOT` to the
sequence sender model would require a custom sequence sender built with
`exec::create` or a coroutine-based async generator — neither of which has a
standard shape yet. The open composition problem remains: structured concurrency
requires the sequence to own each handler's lifetime, but waiting for one
handler before accepting the next serialises the server. `exec::merge_each`
(also in stdexec experimental) is the intended combinator for running handlers
concurrently from a sequence, but its interaction with an unbounded async source
is not yet proven in practice. The `echo_server_pool` pattern — accept loop
pushing into a bounded `channel`, drained by a fixed worker pool —
implements this manually today and would be the reference point for any future
sequence-sender port.

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
Shutdown is coordinated through a `std::stop_source`: the last test client
calls `request_stop()`, cancelling `async_accept` (ECANCELED), which closes
the channel and lets the worker pool drain and exit cleanly.

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

### ~~Sanitizer coverage~~ ✅ done

A `tsan` CMake preset was added (`CMakePresets.json`). Boost.Fiber's userspace
spinlock and condvar are invisible to TSan and suppressed via
`.tsan-suppressions`. One real race in `sync_wait` (`done` bool under a
fiber mutex) was fixed with `std::atomic<bool>` and release/acquire ordering.

