# ADR-0006: `fd_ref` — unified fd parameter for async ops

**Status**: Implemented

## Context

ADR-0005 introduced `fixed_fd_table` and the `fd_slot` RAII handle.  Passing a
pre-registered fd to an async op requires `IOSQE_FIXED_FILE` on the SQE; passing
a regular fd does not.  The only thing that differs between the two call paths is
that flag.

The initial implementation handled this by adding a second overload of every
async op that accepted `fixed_fd` in place of `int`:

```cpp
ssize_t async_read(int fd, std::span<std::byte> buf, ...);
ssize_t async_read(fixed_fd fd, std::span<std::byte> buf, ...);

ssize_t async_write(int fd, std::span<std::byte const> buf, ...);
ssize_t async_write(fixed_fd fd, std::span<std::byte const> buf, ...);

// … repeated for async_recv, async_send, async_connect, …
```

Five functions, each with two overloads: ten function bodies that are nearly
identical except for one `sqe->flags |= IOSQE_FIXED_FILE` line.  Adding the
new linked-pair ops (`async_send_recv`, `async_write_fsync`) would require two
more overloads each.

## Problem

Overload explosion: N async ops × 2 fd kinds = 2N function bodies.  Every new
async op, every new signature change (e.g. adding a `timeout` parameter), and
every bug fix must be applied twice.  The duplication also makes the public
header larger and the documentation harder to read.

## Decision: implicit-conversion wrapper `fd_ref`

Introduce a lightweight POD struct `fd_ref` in `include/fiberexec/fd_ref.hpp`
that carries both the fd index and a `bool fixed` flag:

```cpp
struct fd_ref {
    int  index;
    bool fixed;

    fd_ref(int fd) noexcept
        : index(fd), fixed(false) {}      // NOLINT(google-explicit-constructor)
    fd_ref(fixed_fd fd) noexcept
        : index(fd.index), fixed(true) {} // NOLINT(google-explicit-constructor)
};
```

Every async op that previously had two overloads now has one, with `fd_ref fd`
as the parameter:

```cpp
ssize_t async_read(fd_ref fd, std::span<std::byte> buf, ...);
```

Inside the implementation, `IOSQE_FIXED_FILE` is set conditionally:

```cpp
io_uring_prep_read(sqe, fd.index, buf.data(), buf.size(), 0);
if (fd.fixed) {
    sqe->flags |= IOSQE_FIXED_FILE;
}
```

Call sites are unchanged — implicit conversion from `int` or `fixed_fd`
continues to work without any modification at existing call sites:

```cpp
fiberexec::async_read(raw_fd, buf);   // int → fd_ref, fixed = false
fiberexec::async_read(slot, buf);     // fd_slot → fd_ref, fixed = true
```

`fixed_fd` is moved from `fixed_fd_table.hpp` into `fd_ref.hpp` so that
`fd_ref.hpp` has no dependency on the fixed-fd-table machinery.

## Alternatives considered

### Template parameter `template <typename Fd>`

Each async op becomes a function template instantiated separately for `int` and
`fixed_fd`:

```cpp
template <typename Fd>
ssize_t async_read(Fd fd, std::span<std::byte> buf, ...);
```

Rejected for three reasons:

1. **Implementations move to headers.**  Templates cannot be split across a
   header and a `.cpp` file without explicit instantiations, which reintroduces
   the duplication at the bottom of each `.cpp`.  Moving everything inline
   exposes internal liburing calls in the public header.
2. **Error messages are worse.**  A call with an incompatible fd type produces a
   template substitution error rather than a clean "no matching overload" or
   implicit-conversion failure.
3. **No runtime benefit.**  The `if (fd.fixed)` branch in the `fd_ref` path is
   trivially predicted and involves no virtual dispatch; a template would not
   generate meaningfully faster code.

### Keep the overloads, add a shared `impl` helper

Factor the body into a private `async_read_impl(int fd_index, bool fixed, ...)`,
call it from both the `int` and `fixed_fd` overloads.

Rejected: the public API still has N × 2 declarations in the header and
documentation.  The duplication shifts from the implementation to the interface,
which is where it does the most damage to readability.

## Two-implicit-conversion problem with `fd_slot`

`fd_slot` (the `fixed_fd_table` RAII handle) previously converted to `fixed_fd`:

```
fd_slot → fixed_fd → fd_ref      (two user-defined conversions — not allowed)
```

C++ permits at most one user-defined conversion in an implicit conversion
sequence.  The chain above would require two, so `fd_slot` passed to an
`fd_ref` parameter would fail to compile.

Fix: `fd_slot` gains a direct `operator fd_ref() const noexcept` conversion that
bypasses the intermediate `fixed_fd` step:

```cpp
[[nodiscard]] operator fd_ref() const noexcept {
    return fd_ref{fixed_fd{static_cast<int>(slot_)}};
} // NOLINT(google-explicit-constructor)
```

The `fixed_fd` → `fd_ref` conversion is still used at construction, but it is
an explicit expression inside `operator fd_ref()`, not a second implicit
conversion in the chain.

## Consequences

### Single overload per async op

`async_read`, `async_write`, `async_recv`, `async_send`, and `async_connect`
each have one declaration, one implementation, and one documentation block.
New async ops (`async_send_recv`, `async_write_fsync`) are written once with no
fixed-fd variant needed.

### `fixed_fd_table.hpp` shrinks

The five `fixed_fd`-specific async op declarations and their implementations in
`fixed_fd_table.cpp` are removed entirely.  `fixed_fd_table.hpp` now contains
only the table, the slot type, and the `acquire_fd_slot` accessor.

### `async_accept` and `async_openat` remain `int fd`

These operations return an fd rather than operating on a pre-registered one, so
`fd_ref` does not apply to their parameters.  `async_close` is also excluded:
it accepts a plain `int` because `fd_slot` destruction closes via the fixed-fd
table machinery, not through `async_close`.

Note that this is a **fiberexec design limitation**, not an io_uring limitation.
io_uring supports `IORING_OP_OPENAT_DIRECT` and `IORING_OP_ACCEPT_DIRECT`, which
write the resulting fd directly into a registered-file-table slot (specified by
`sqe->file_index`, or `IORING_FILE_INDEX_ALLOC` to let the kernel pick a free
slot automatically) rather than returning a normal fd through the CQE result.
These would let a fiber open or accept a connection and have it land directly in
`fixed_fd_table` with no `acquire_fd_slot` / `update` step.  The current table
design — capacity fixed at context construction, slots managed by a free-list
`channel` — would need a complementary "kernel-allocated slot" path to expose
this cleanly, and that work is deferred.

### No API breakage at existing call sites

Passing an `int` or an `fd_slot` to any async op continues to compile and
behave identically.  The change is purely at the overload-resolution and
implementation layer.
