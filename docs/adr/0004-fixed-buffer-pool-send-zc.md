# ADR-0004: fixed_buffer_pool and async_send_zc — zero-copy send with registered buffers

**Status**: Implemented

## Context

`IORING_OP_SEND` copies data from a user buffer into a kernel socket buffer on
every call.  For high-throughput workloads that send the same kind of fixed-size
records repeatedly, this per-operation copy is the dominant cost.  io_uring
offers two mechanisms that together eliminate it:

- **`IORING_REGISTER_BUFFERS`** — a one-time registration call that pins a set
  of user buffers in the kernel.  Subsequent operations reference them by index
  rather than pointer; the kernel skips the per-op memory pin/unpin.
- **`IORING_OP_SEND_ZC`** with `IORING_RECVSEND_FIXED_BUF` — a zero-copy send
  that reads directly from a pre-registered buffer by index, with no intermediate
  copy into the socket buffer.

The complement on the receive side (`IORING_RECV_MULTISHOT` with
`IORING_REGISTER_PBUF_RING`) was already implemented via `multishot_recv`.
This ADR covers the send side.

## Problem

Several design questions arise when integrating these kernel primitives into
the fiber model:

1. **Buffer lifecycle**: a registered buffer cannot be reused until the kernel
   signals it has finished reading from it.  `IORING_OP_SEND_ZC` delivers two
   CQEs — a send-completion CQE and a buffer-release notification CQE — but the
   existing `io_awaitable` / `submit_and_wait` path handles exactly one CQE per
   operation.

2. **Buffer pool management**: with a fixed count of pre-registered slots, a
   producer must block (not spin) when all slots are in flight, and be woken
   when one is returned.  This needs a fiber-aware counting mechanism.

3. **Scoped buffer ownership**: the registered buffer index must travel with the
   buffer data so `async_send_zc` can reference it by index; and the slot must
   be returned to the pool automatically when the caller is done, even on
   exception.

4. **Registration lifetime**: the kernel allows only one registered buffer table
   per ring.  The API must enforce this and clean up on destruction.

## Options considered

### Two-CQE handling: futures vs. custom cqe_handler

**Option A — `submit_and_wait` twice**: submit the SQE, call `submit_and_wait`
for the send CQE, then call it again for the notification CQE.

_Problem_: the two CQEs for a single SEND_ZC share the same `user_data`.
`submit_and_wait` installs a new `io_awaitable` for each call with a different
address; the second CQE would arrive with the first `io_awaitable`'s address,
which has already been destroyed.  Not viable.

**Option B — custom `cqe_handler` subclass (chosen)**: define `send_zc_handler
: detail::cqe_handler` that holds a `boost::fibers::promise<void>` and an `int
result_`.  Its `complete()` method:

- On the send CQE (`IORING_CQE_F_NOTIF` absent): records `res`; if
  `IORING_CQE_F_MORE` is absent (inline completion, no notification coming),
  fulfils the promise immediately.
- On the notification CQE (`IORING_CQE_F_NOTIF` set): fulfils the promise,
  waking the fiber.

The handler lives on the suspended fiber's stack for the full duration of both
CQE deliveries.  Because Boost.Fiber preserves the stack while a fiber is
suspended, the handler address stored in `user_data` remains valid until
`handler.wait()` returns.  No heap allocation is needed.

This reuses the `cqe_handler` virtual-dispatch infrastructure from ADR-0003
and requires no changes to `drain_cqes()`.

### Buffer free-list: channel vs. semaphore vs. atomic counter

**Option A — `fiberexec::channel<uint16_t>` (chosen)**: pre-populate a bounded
channel with all buffer indices at construction.  `borrow()` calls `pop()`
(suspends the fiber if all slots are in flight); `return_buffer()` calls
`push()` (wakes the next waiter, if any).  The channel doubles as a counting
semaphore with zero additional machinery.

**Option B — promise-per-waiter**: maintain a queue of
`boost::fibers::promise<uint16_t>`.  Each `borrow()` that finds no free buffer
enqueues a promise and suspends; `return_buffer()` dequeues the oldest promise
and fulfils it.

_Comparison_: option A requires no bespoke waiter queue; the channel is
self-contained, already fiber-aware, and already present in the project.
Option B adds code for the same semantics.

**Option C — `boost::fibers::condition_variable` + `std::queue<uint16_t>`**:
a mutex-protected queue with a condition variable for blocking.  More verbose
and no more capable than the channel for this use case.

### Buffer ownership: index-only vs. RAII handle

Passing a raw `uint16_t` index to `async_send_zc` would require the caller to
manually return it to the pool.  Instead, `fixed_buffer::fixed_buffer` is a
move-only RAII handle: its destructor calls `pool_->return_buffer(idx_)`, so
the slot is reclaimed automatically on scope exit or exception.  The destructor
is also the natural point to signal `IORING_CQE_F_NOTIF` has already been
received and the buffer is safe to reuse.

### iovec storage: in pool vs. temporary

`io_uring_register_buffers` copies the `iovec` array into the kernel
immediately and returns; the user-space array is not referenced after the call.
The pool therefore builds the `iovec` array as a local variable in the
constructor and discards it, keeping `fixed_buffer_pool` free of a
`std::vector<iovec>` member.

## Decision

- **`send_zc_handler`**: a stack-allocated `cqe_handler` subclass holds the
  promise; `complete()` handles both CQEs in a single handler instance.
- **`fiberexec::channel<uint16_t>`**: pre-populated at construction; provides
  fiber-aware blocking with no additional code.
- **`fixed_buffer`**: move-only RAII handle; destructor returns the index.
- **Temporary `iovec`**: built in the constructor, not stored.

## Implementation

- `include/fiberexec/fixed_buffer_pool.hpp` — public API: `fixed_buffer_pool`
  (owns storage and channel), `fixed_buffer` (RAII index handle), and the free
  function `async_send_zc`.
- `src/fixed_buffer_pool.cpp` — `send_zc_handler : detail::cqe_handler`;
  `fixed_buffer_pool` constructor calls `io_uring_register_buffers` and
  pre-fills the channel; destructor calls `free_list_.close()` then
  `io_uring_unregister_buffers`; `async_send_zc` prepares the SQE via
  `io_uring_prep_send_zc_fixed`, submits it, and blocks in `handler.wait()`.
- `include/fiberexec/fiberexec.hpp` — adds `#include
  <fiberexec/fixed_buffer_pool.hpp>` to the umbrella header.

**Kernel version requirement**: `IORING_OP_SEND_ZC` and
`IORING_RECVSEND_FIXED_BUF` require Linux 6.0.  `IORING_OP_SEND_ZC` is not
supported on AF_UNIX sockets; tests use TCP loopback.

## Consequences

### Channel capacity invariant

`boost::fibers::buffered_channel<T>` uses a ring buffer that reserves one slot
to distinguish full from empty (`is_full = cidx == (pidx+1) % capacity`).  A
channel constructed with capacity `N` can therefore hold only `N − 1` items.
Pre-filling a channel of capacity `N` with `N` indices blocks on the final
push, deadlocking the constructor fiber.

This was not caught until the feature was implemented because the project had
not previously used `buffered_channel` as a pre-populated pool.  The fix was
applied to the `fiberexec::channel<T>` wrapper: the constructor now passes
`std::bit_ceil(capacity + 1)` to the underlying Boost channel, ensuring the
ring always has room for at least `capacity` items.  The actual limit before
`push` blocks is `bit_ceil(capacity + 1) − 1`, which is ≥ `capacity`.

### One registered buffer table per ring

Constructing a second `fixed_buffer_pool` on a thread while the first is alive
causes `io_uring_register_buffers` to return `-EBUSY`, which the constructor
converts to `std::system_error`.  The header documents this constraint; no
additional enforcement mechanism is needed.

### Buffer index width

Buffer indices are `uint16_t`, matching the `buf_index` field in the SQE
struct and capping the pool at 65 535 slots — far above any practical use.

### No iovec member

Because the kernel copies the `iovec` array synchronously during registration,
`fixed_buffer_pool` stores only the flat `std::vector<std::byte> storage_` and
the channel.  This keeps the pool small and avoids a redundant allocation that
would otherwise live for the pool's entire lifetime.
