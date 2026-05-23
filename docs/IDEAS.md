# Ideas and future investigations

A running list of things to build, explore, or research. Not prioritised — just
a capture of everything that came up during development or that seems worth
investigating.

---

## Deferred implementation work

These are concrete items with designs already sketched in the ADRs.

### ADR-0001 step 4 — ECANCELED → `set_stopped` mapping

The async ops throw `std::system_error(ECANCELED)` on cancellation, but that
surfaces as `set_error`, not `set_stopped`. Most users are not blocked by this
today (see ADR-0001 for why), but algorithms that branch on `set_stopped` (e.g.
`let_stopped`, or a `sync_wait` caller distinguishing cancel from success) will
see the wrong completion.

Three options are sketched in ADR-0001:

- **Option A** — `fiberexec::fiber_then(fn)`: custom drop-in for `stdexec::then`
  that catches ECANCELED and calls `set_stopped` instead.
- **Option B** — `fiberexec::run(sched, fn)`: single compound sender that
  schedules, installs the stop token, runs `fn`, and maps ECANCELED to
  `set_stopped`. Cleanest long-term API.
- **Option C** — `fiberexec::map_cancelled_to_stopped(sender)`: outer wrapper
  applied at the composition site; least invasive but most boilerplate at call
  sites.

Options A and B use the same underlying machinery; B is preferred.

### ADR-0002 steps 2 & 3 — fiber sync primitives

- **`fiber_mutex`** + **`fiber_condition_variable`**: thin wrappers over
  `boost::fibers::mutex` / `boost::fibers::condition_variable` to keep Boost
  out of public headers. Straightforward to implement.
- **`fiber_channel<T>`**: bounded MPMC channel; builds on mutex/condvar.
  Enables structured producer/consumer pipelines within the fiber pool.
  `boost::fibers::buffered_channel<T>` is the likely backing type.

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

### What happens when a fiber throws an unhandled exception?

Currently an unhandled exception in a fiber likely terminates the process via
`std::terminate`. It is worth making this explicit: either document it as a
programming error (by contract, all fiber callables must not let exceptions
escape) or catch at the pool boundary and deliver via `set_error` or a
configurable handler.

### Fiber stack size configuration

Boost.Fiber defaults to a fixed stack size (usually 64KB via `fixedsize_stack`
or `pooled_fixedsize_stack`). High-depth call chains (e.g. fibers doing complex
parsing) may need larger stacks. Exposing a `stack_size` option on
`fiber_context` construction would let callers tune this.

### Lazy stop-token installation

ADR-0001 notes that `fiber_specific_ptr` incurs a heap allocation per fiber that
installs a stop token. If the receiver's token is not stoppable (e.g. `sync_wait`
with no external cancellation source), the `std::stop_source` starts with
`std::nostopstate` and no stop callback is registered. Verify that this path
actually skips the `fiber_specific_ptr` heap allocation, or make it do so
explicitly.

### Thread pool sizing

`fiber_context` currently takes a fixed thread count. Worth investigating:
- What is the right default? `std::thread::hardware_concurrency()`?
- Should the pool be allowed to grow dynamically under load?
- Is there a meaningful interaction between fiber count and thread count for
  I/O-heavy vs CPU-heavy workloads?

---

## API and ergonomics

### A realistic echo server with a proper accept loop

The current `echo_server` example accepts exactly 3 clients and exits. A more
realistic version would loop on `async_accept` indefinitely, spawn a fiber per
connection, and handle errors gracefully (ECONNRESET, etc.). This would also
exercise `fiber_sync_wait` in a real accept loop:

```cpp
while (running) {
    int client = fiberexec::async_accept(server_fd, nullptr, nullptr);
    detail::schedule_task(pool, [client] {
        // per-connection fiber
        handle_client(client);
        ::close(client);
    });
}
```

### `async_read` / `async_write` vs `async_recv` / `async_send`

Currently both sets exist. The distinction (`read`/`write` work on any fd;
`recv`/`send` are socket-specific and carry flags) is standard POSIX, but it is
worth documenting clearly and deciding if fiberexec should expose the flags
parameter on `recv`/`send`.

### `std::expected`-based error API

The current API throws `std::system_error` on all errors, including
cancellation. An alternative would be returning `std::expected<ssize_t,
std::error_code>` so callers can handle errors inline without exceptions. This
is orthogonal to the `set_stopped` mapping problem (see ADR-0001) but affects
calling code ergonomics significantly. Worth a deliberate decision rather than
accumulating technical debt.

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

### Performance benchmarking

No benchmarks exist yet. Interesting questions:
- Fiber context-switch overhead vs thread context-switch (should be ~10–50×
  faster).
- Throughput on a loopback echo server at varying concurrency levels.
- SQE submission overhead: one SQE per op vs batched submission.
- Comparison with an equivalent Asio or liburing-based implementation.

### Interaction with `stdexec::when_all` cancellation at scale

The existing tests cover `when_all` cancellation with two branches. It is worth
testing with many branches (e.g. 100 concurrent `async_recv` calls) to verify
that the cancel queue drains correctly and no CQEs are lost or misattributed.

---

## Housekeeping

### Sanitizer coverage

The `asan` preset enables AddressSanitizer + UBSan. It is worth running the
full test suite under ThreadSanitizer (TSan) as well — the cross-thread
promise/future and cancel-queue paths have non-trivial concurrent access that
TSan might flag.

