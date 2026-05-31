# ADR-0005: Thread-local fixed-buffer pool and registered-fd table

**Status**: Implemented

## Context

ADR-0004 introduced `fixed_buffer_pool` and `async_send_zc` for zero-copy
sends using `IORING_REGISTER_BUFFERS` / `IORING_OP_SEND_ZC`.  A companion
feature, `fixed_fd_table`, was added to exploit `IORING_REGISTER_FILES` /
`IOSQE_FIXED_FILE`, which lets the kernel skip the per-operation fd table
lookup for pre-registered file descriptors.

Both features are backed by kernel state that is **per io_uring ring**, and
every ring is **per OS thread** in fiberexec's 1:1 thread-to-ring design.
The initial implementations were fiber-scoped: a fiber would construct a
`fixed_buffer_pool` or `fixed_fd_table` as a local variable, use it, and
destroy it when the fiber returned.

## Problem

The fiber-scoped design has two fatal flaws:

1. **Only one table per ring is permitted.**  Constructing a second
   `fixed_buffer_pool` or `fixed_fd_table` on the same thread while the first
   is alive causes `io_uring_register_buffers` / `io_uring_register_files` to
   return `-EBUSY`.  In a pool with N fibers per thread, at most one can ever
   hold a table at a time — all others would throw on construction.

2. **Registration overhead amortizes poorly over short lifetimes.**  The
   `io_uring_register_*` / `io_uring_unregister_*` pair is not free; its
   benefit (skipping per-op pin/unpin or fd-table lookup) only pays off when a
   stable set of resources is registered once and used for many operations.
   Registering and unregistering per fiber, per connection, or per file
   rotation erases the gain.

The concrete motivating use case that exposed this: a long-running
data-processing application with hundreds of output channels, each served by a
dedicated fiber.  Each fiber writes a stream of records to a file and
periodically rotates to a new file when the current one reaches a size limit.
There is no central coordinator where all fds are created; fds are opened and
closed per-fiber.  Neither the fiber-scoped design (EBUSY) nor a centralised
acceptor pattern (no single place that knows about all fds) applies.

## Decision: thread-local singletons configured at context construction

The registered buffer pool and the registered-fd table are thread-level
resources — their scope matches the ring, not the fiber.  Both are therefore
initialised once per worker thread by the pool infrastructure itself, not by
user code, and all fibers on that thread share them.

### Configuration via `context_options`

Because the table capacity and buffer geometry must be fixed at registration
time, they are specified when the `context` is constructed:

```cpp
fiberexec::context ctx{fiberexec::context_options{
    .thread_count          = 4,
    .fixed_buffer_size     = 4096,  // per-buffer size in bytes
    .fixed_buffer_count    = 64,    // slots per thread (0 = disabled)
    .registered_fd_capacity = 256,  // fd slots per thread (0 = disabled)
}};
```

`context_options` replaces the old two-argument `context(thread_count,
stack_size)` constructor.  The old constructor is retained as a backward-
compatible overload that delegates to the new one with the resource fields
zeroed (features disabled).

### Initialisation and destruction in `fiber_pool::worker()`

Each worker thread constructs the per-thread resources as `std::optional<T>`
**stack locals** in `worker()`, immediately after
`boost::fibers::use_scheduling_algorithm<io_uring_scheduler>()` returns:

```cpp
void fiber_pool::worker() {
    use_scheduling_algorithm<io_uring_scheduler>(this);
    // tl_ring is now set — ring is live.

    std::optional<fixed_buffer_pool> fbp;
    std::optional<fixed_fd_table>    fdt;

    if (opts_.fixed_buffer_size > 0 && opts_.fixed_buffer_count > 0)
        fbp.emplace(opts_.fixed_buffer_size, opts_.fixed_buffer_count);
    if (opts_.registered_fd_capacity > 0)
        fdt.emplace(opts_.registered_fd_capacity);

    // ... wait for shutdown and all fibers to drain ...

    // Stack unwind destroys fdt, then fbp here — before thread-local
    // cleanup, so io_uring_unregister_* always sees a live ring.
}
```

Thread-local pointers `tl_fixed_buffer_pool` and `tl_fixed_fd_table` are set
after construction and nulled immediately before the optionals are destroyed,
preventing any post-shutdown access.

The ordering guarantee is critical: C++ stack locals are destroyed when the
function returns, which happens before thread-local destructors run.  The
`io_uring_scheduler` (which clears `tl_ring` and calls `io_uring_queue_exit`)
is a thread-local, so the ring is still live when `io_uring_unregister_*` is
called from the pool and table destructors.

### Fiber-callable free functions

Fibers do not construct the pools directly.  Instead they call thread-local
accessor functions:

- `borrow_fixed_buffer()` — draws a slot from the calling thread's
  `fixed_buffer_pool`.  Suspends the fiber if all slots are currently in
  flight.  Throws `std::runtime_error` if the feature was not configured.
- `acquire_fd_slot(int fd)` — borrows a slot from the calling thread's
  `fixed_fd_table`, installs `fd` via `io_uring_register_files_update`, and
  returns an `fd_slot` RAII handle.  Suspends the fiber if all slots are
  occupied.  Throws `std::runtime_error` if the feature was not configured.

Both follow the same slot-allocation pattern as the existing
`fixed_buffer_pool::borrow()` (a pre-populated `channel<T>` free list that
provides fiber-aware blocking with no additional mechanism).

### `fd_slot` — RAII handle with rotation support

The `fd_slot` type models the file-rotation use case directly:

```cpp
// Fiber opens a file, acquires a slot, writes until size limit,
// then rotates without releasing the slot.
auto slot = fiberexec::acquire_fd_slot(fd);
while (!done) {
    fiberexec::async_write(slot, buf);  // implicit fixed_fd conversion
    if (file_too_large()) {
        fiberexec::async_close(fd);
        fd = fiberexec::async_openat(AT_FDCWD, next_path, O_WRONLY|O_CREAT, 0644);
        slot.update(fd);                // one register_files_update, no slot churn
    }
}
// slot destroyed → clears slot in kernel (-1), returns index to free list
```

`fd_slot` implicitly converts to `fixed_fd` so it can be passed directly to
every `async_recv` / `async_send` / `async_read` / `async_write` /
`async_connect` overload without a conversion call at the call site.

On destruction, `fd_slot` clears the kernel slot (`update(-1)`) before
returning the index to the free list, ensuring no stale fd reference lingers
in the registered table.

## Alternatives considered

### Fiber-scoped construction (original design)

Rejected: EBUSY for any second fiber on the same thread; registration overhead
per fiber erases the performance benefit.

### Per-fiber tables with an exclusion lock

Each fiber acquires a per-thread mutex before constructing a table and releases
it on destruction.  Only one fiber at a time could hold the table, serialising
all registered-resource I/O on a thread.

Rejected: serialisation is worse than the baseline (regular fd lookups run
concurrently); it also means a fiber that blocks waiting for I/O holds the
lock and prevents other fibers from even starting their I/O.

### Dynamic resizing

Allow `io_uring_register_files_update` to grow the registered table lazily.

Rejected: the kernel requires the table size to be declared up front
(`io_uring_register_files`); growing it requires `io_uring_unregister_files`
followed by `io_uring_register_files` with a larger array, which is not atomic
and races with in-flight ops that reference the old table.

### Thread-local with no free-list (index assignment at connect/open time)

A central acceptor assigns a fixed slot index to each connection at accept
time, as in Seastar's shard model.

Rejected: inapplicable to the file-rotation use case where fds are created
per-fiber with no central coordinator.  A free-list generalises to both
patterns without requiring a central point.

## Consequences

### One pool and one table per thread

`io_uring_register_buffers` and `io_uring_register_files` each allow one
registration per ring.  With thread-local singletons this constraint is
naturally satisfied: `worker()` never calls either twice.  The constraint is
still reflected in the documentation of both classes.

### Capacity is fixed at context construction

`fixed_buffer_count` and `registered_fd_capacity` cannot be changed after the
context starts.  The correct value depends on the application's concurrency
model: a reasonable starting point is the expected number of concurrent fibers
per thread (for the fd table) or the expected number of in-flight sends (for
the buffer pool).  Requests for a slot beyond the configured capacity block the
calling fiber until one is returned; they do not fail.

### `fixed_buffer_pool` and `fixed_fd_table` remain constructible

The class constructors are not hidden.  A power user can still construct an
instance directly inside a fiber for a scoped, single-fiber use case, as long
as no thread-local instance is active on the same thread.  The EBUSY constraint
is unchanged; the documentation now flags it explicitly.

### `context_options` replaces the two-argument constructor

All call sites that pass only `thread_count` continue to work via the
backward-compatible `context(uint32_t thread_count, size_t stack_size)` overload.
Call sites that need fixed buffers or registered fds switch to
`context(context_options{...})`.
