# ADR-0003: cqe_handler virtual dispatch for io_uring CQE routing

**Status**: Implemented

## Context

Every in-flight fiber I/O operation parks an `io_awaitable` on the fiber's
stack and stores its address in the SQE's `user_data` field.  When
`drain_cqes()` processes a completion, it casts `user_data` back to
`io_awaitable*` and calls `promise.set_value(res)` to resume the fiber.  Three
small integer sentinel values (0, 1, 2) occupy the bottom of the `uint64_t`
tag space for internal bookkeeping ops (work eventfd, notify eventfd, and
async-cancel completions); every other tag value is assumed to be an
`io_awaitable*`.

This one-shot model is correct for all existing async ops: one SQE submission
produces exactly one CQE.

## Problem

`IORING_ACCEPT_MULTISHOT` (and the planned `IORING_RECV_MULTISHOT` and
`IORING_POLL_ADD_MULTI`) break the one-SQE-one-CQE invariant:

- A single submission stays armed in the ring and produces **one CQE per
  event** (accepted connection, received data, or poll readiness).
- Each CQE carries `IORING_CQE_F_MORE` in `flags` when more completions will
  follow.  When `IORING_CQE_F_MORE` is absent, the SQE has been consumed by
  the kernel and must be resubmitted to continue receiving events.
- The completion handler therefore needs both `res` **and** `flags` to decide
  whether to rearm, and it may be invoked many times before the owning object
  is destroyed.

These properties are fundamentally different from `io_awaitable` and cannot
be shoehorned into it.  `drain_cqes()` needs a way to dispatch each CQE to
the right handler without knowing at compile time which op types exist.

## Options considered

### 1. Tagged pointer — low bit (initial implementation)

Store `reinterpret_cast<uint64_t>(accept_state*) | 1` in `user_data`.  Since
heap allocations are at least 8-byte aligned, bit 0 of any real pointer is
always 0; setting it distinguishes multishot accept CQEs from `io_awaitable`
CQEs.  `drain_cqes()` checks `tag & 1` before the existing fallthrough.

**Pros**: zero overhead, no additional allocation.

**Cons**: only one free bit distinguishes two types.  Adding
`IORING_RECV_MULTISHOT` would require a second bit (making the scheme
`tag & 3` with four cases).  `IORING_POLL_ADD_MULTI` would require a third.
Each new multi-CQE op type modifies `drain_cqes()` and steals another bit,
capping out at three op types before 8-byte alignment assumptions no longer
hold.  The approach is an evolutionary dead end.

### 2. Tagged pointer — high bits

On x86-64, user-space virtual addresses fit in 48 bits; bits 48–63 are always
zero.  Use two or three of those bits as a type field, leaving the remaining
bits as the raw pointer.

**Pros**: no alignment dependency; more bits available.

**Cons**: still pointer manipulation.  Relies on the 48-bit virtual address
space being a stable property of the platform and kernel ABI, which is not
guaranteed (AMDs 5-level paging extends addresses to 57 bits).  Same
evolutionary problem as option 1 — `drain_cqes()` must still be updated for
each new op type.

### 3. Scheduler-side registry

Keep a `std::unordered_set<void*>` (or `unordered_map`) in the scheduler
listing all live multishot-state pointers.  On each CQE, check membership to
decide how to cast.

**Pros**: no pointer arithmetic; type-safe; `drain_cqes()` dispatch is clean.

**Cons**: one hash lookup per CQE, even for the common one-shot case.  The set
must be kept in sync with SQE lifetime, adding a registration/deregistration
API to the scheduler.  Separate sets are still needed per op type unless a
tagged union is stored in the map, which reintroduces the dispatch problem.

### 4. Virtual dispatch via `cqe_handler` base class (chosen)

Define an abstract base:

```cpp
struct cqe_handler {
    virtual void complete(io_uring* ring, int res, uint32_t flags) noexcept = 0;
    virtual ~cqe_handler() = default;
};
```

Both `io_awaitable` and every multishot-state type inherit from it.
`drain_cqes()` stores `static_cast<cqe_handler*>(ptr)` in `user_data` and
dispatches with a single virtual call:

```cpp
reinterpret_cast<detail::cqe_handler*>(tag)->complete(&ring_, res, flags);
```

`io_awaitable::complete()` ignores `ring` and `flags` and calls
`promise.set_value(res)` — identical to the old behaviour.  `accept_state::complete()`
delivers the fd, rearms the SQE when `IORING_CQE_F_MORE` is absent, and sets
`error` on failure.  Future `recv_state` and `poll_state` types follow the
same pattern without touching `drain_cqes()` at all.

**Pros**:

- `drain_cqes()` is closed for modification once and for all.  Adding a new
  multi-CQE op type is purely additive: define a new `cqe_handler` subclass
  and implement `complete()`.
- Type-safe: no integer casts at the dispatch site.
- `flags` is passed to every handler, so future handlers that inspect
  `IORING_CQE_F_MORE`, `IORING_CQE_F_SOCK_NONEMPTY`, or other flag bits
  receive them without any interface change.
- Overhead is one indirect call per CQE — negligible compared to the kernel
  round-trip that just completed.

**Cons**:

- Adds a vtable pointer (8 bytes) to every `io_awaitable`.  Each `io_awaitable`
  is stack-allocated for the duration of exactly one async op; the extra 8
  bytes are inconsequential.
- Required touching `io_awaitable` and both `set_data` call sites in
  `submit_and_wait` / `submit_and_wait_with_timeout`.

## Decision

Virtual dispatch (option 4).  The overriding factor is that multishot recv and
multishot poll are planned.  With options 1–3, each addition modifies
`drain_cqes()` and the bit-stealing or registry machinery; with option 4,
`drain_cqes()` is written once and never revisited.

## Implementation

- `include/fiberexec/detail/cqe_handler.hpp` — abstract base; forward-declares
  `struct io_uring` to avoid pulling in `<liburing.h>` from the header.
- `src/fiber_context.cpp` — `io_awaitable : cqe_handler`; `drain_cqes()`
  reduced to a single virtual dispatch in the non-sentinel branch; both
  `submit_and_wait` call sites upcast to `cqe_handler*` before storing in
  `user_data`.
- `include/fiberexec/multishot_acceptor.hpp` — `multishot_acceptor : private
  detail::cqe_handler`; all accept state (pending fd queue, waiter, error) held
  as direct members; `complete()` declared private.
- `src/multishot_acceptor.cpp` — `complete()` implementation; rearms the SQE
  when `IORING_CQE_F_MORE` is absent and `res >= 0`; stores
  `static_cast<detail::cqe_handler*>(this)` in `user_data` — no intermediate
  state type, no pointer arithmetic.
- Since `multishot_acceptor` is non-copyable and non-movable its address is
  stable for its entire lifetime, allowing `this` to be used direcly as the
  sqe user data

## Consequences

- Adding `IORING_RECV_MULTISHOT` or `IORING_POLL_ADD_MULTI` requires only a
  new `cqe_handler` subclass and its `complete()` implementation.  No changes
  to `drain_cqes()`, the sentinel scheme, or the dispatch machinery.
- `complete()` receives `flags` unconditionally.  One-shot handlers ignore it;
  multi-CQE handlers use it for `IORING_CQE_F_MORE` checks and rearm logic.
- The vtable pointer on `io_awaitable` costs 8 bytes per in-flight op.  Each
  op suspends its fiber, so there is at most one `io_awaitable` per fiber per
  yield; at typical fiber counts the total footprint increase is immaterial.
- The `reinterpret_cast<detail::cqe_handler*>(tag)` in `drain_cqes()` is safe
  because the value was stored via `static_cast<cqe_handler*>(ptr)` through
  `io_uring_sqe_set_data` (which stores `void*` as `uint64_t`).  The
  round-trip `T* → cqe_handler* → void* → uint64_t → cqe_handler*` is
  well-defined for derived-to-base upcasts in single-inheritance hierarchies.
