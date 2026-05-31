#pragma once

#include <fiberexec/channel.hpp>
#include <fiberexec/fd_ref.hpp>

#include <cstddef>

struct io_uring;

namespace fiberexec {

class fd_slot; // forward declaration — defined below

/// RAII wrapper around io_uring registered file descriptors
/// (IORING_REGISTER_FILES / io_uring_register_files_update).
///
/// Registered fds are looked up once at registration time; subsequent I/O ops
/// reference them by slot index with IOSQE_FIXED_FILE, skipping the per-op fd
/// table lookup.
///
/// The normal usage pattern is to configure a per-thread table via
/// `context_options::registered_fd_capacity` and then call `acquire_fd_slot()`
/// from within a fiber.  Direct construction is available as a power-user
/// escape hatch.
///
/// Only one fixed_fd_table may be active per worker thread at a time — the
/// kernel allows only one registered file table per ring.
///
/// Must be constructed and destroyed from a fiberexec worker thread (i.e. from
/// within `worker()` or from a fiber running on that thread).
class fixed_fd_table {
public:
    /// Reserve @p capacity empty slots.  All slots are initialised to -1 and
    /// added to the internal free list; call `acquire_fd_slot()` to check out
    /// a slot and install a real fd.
    ///
    /// @throws std::system_error on io_uring registration failure.
    /// @throws std::runtime_error if called outside a fiberexec worker thread.
    explicit fixed_fd_table(std::size_t capacity);

    ~fixed_fd_table();
    fixed_fd_table(fixed_fd_table const&) = delete;
    fixed_fd_table& operator=(fixed_fd_table const&) = delete;
    fixed_fd_table(fixed_fd_table&&) = delete;
    fixed_fd_table& operator=(fixed_fd_table&&) = delete;

    /// Replace the fd in @p slot with @p fd.  Pass -1 to clear a slot.
    ///
    /// @throws std::system_error on io_uring update failure.
    void update(unsigned slot, int fd);

    /// Return a fixed_fd handle for @p slot (direct index access).
    [[nodiscard]] fixed_fd operator[](unsigned slot) const noexcept;

    [[nodiscard]] std::size_t capacity() const noexcept { return capacity_; }

private:
    friend class fd_slot;
    friend fd_slot acquire_fd_slot(int fd);

    [[nodiscard]] unsigned borrow_slot();
    void release_slot(unsigned slot) noexcept;

    io_uring* ring_;
    std::size_t capacity_;
    channel<unsigned> free_list_;
};

/// RAII handle for one slot borrowed from the thread-local fixed_fd_table.
///
/// Obtain via `acquire_fd_slot(fd)`.  Implicitly converts to `fixed_fd` so it
/// can be passed directly to the fixed-fd async overloads.  On destruction the
/// slot is cleared in the kernel and returned to the free list.
///
/// Call `update(new_fd)` to swap the registered fd (e.g. on file rotation)
/// without releasing the slot.
class fd_slot {
public:
    /// Install @p fd in this slot (e.g. after file rotation).
    ///
    /// @throws std::system_error on io_uring update failure.
    void update(int fd);

    /// Implicit conversions so `fd_slot` can be passed directly to any async op.
    [[nodiscard]] operator fixed_fd() const noexcept {
        return fixed_fd{static_cast<int>(slot_)};
    } // NOLINT(google-explicit-constructor)
    [[nodiscard]] operator fd_ref() const noexcept {
        return fd_ref{fixed_fd{static_cast<int>(slot_)}};
    } // NOLINT(google-explicit-constructor)

    ~fd_slot();
    fd_slot(fd_slot&& other) noexcept;
    fd_slot& operator=(fd_slot&& other) noexcept;
    fd_slot(fd_slot const&) = delete;
    fd_slot& operator=(fd_slot const&) = delete;

private:
    friend fd_slot acquire_fd_slot(int fd);
    fd_slot(fixed_fd_table* table, unsigned slot) noexcept;

    fixed_fd_table* table_{nullptr};
    unsigned slot_{0};
};

/// Borrow a registered-fd slot from the calling thread's per-thread table and
/// install @p fd in it.
///
/// The table must have been enabled via `context_options::registered_fd_capacity`
/// before the `context` was constructed.  Suspends the calling fiber if all
/// slots are currently in use.
///
/// On destruction the returned `fd_slot` clears the slot (installs -1) and
/// returns it to the free list.  Call `fd_slot::update(new_fd)` to swap the
/// registered fd without releasing the slot (e.g. on file rotation).
///
/// @throws std::runtime_error if no registered-fd table was configured for this thread.
[[nodiscard]] fd_slot acquire_fd_slot(int fd);

} // namespace fiberexec
