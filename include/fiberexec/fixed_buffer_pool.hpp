#pragma once

#include <fiberexec/channel.hpp>
#include <fiberexec/detail/cqe_handler.hpp>

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

struct io_uring;

namespace fiberexec {

/// RAII pool of fixed buffers pre-registered with the io_uring ring via
/// IORING_REGISTER_BUFFERS.  Registered buffers are pinned in the kernel once
/// at construction; subsequent fixed I/O operations reference them by index,
/// avoiding the per-op memory pin/unpin that ordinary I/O incurs.
///
/// Only one fixed_buffer_pool may be active per worker thread at a time — the
/// kernel allows only one registered buffer table per ring.  Constructing a
/// second pool on the same thread while the first is alive will throw
/// std::system_error(EBUSY).
///
/// Must be constructed and destroyed from a fiber running on a fiberexec worker.
class fixed_buffer_pool {
public:
    /// RAII handle for one buffer slot borrowed from the pool.
    ///
    /// Fill data() with the bytes to send before passing to async_send_zc.
    /// The buffer is returned to the pool on destruction.  Must be destroyed
    /// on the same worker thread it was created on.
    class fixed_buffer {
    public:
        /// Writable span over the full buffer slot (buf_size bytes).
        [[nodiscard]] std::span<std::byte> data() noexcept;
        [[nodiscard]] std::span<std::byte const> data() const noexcept;

        /// Index of this buffer in the ring's registered buffer table.
        [[nodiscard]] uint16_t buf_index() const noexcept { return idx_; }

        ~fixed_buffer();
        fixed_buffer(fixed_buffer&& other) noexcept;
        fixed_buffer& operator=(fixed_buffer&& other) noexcept;
        fixed_buffer(fixed_buffer const&) = delete;
        fixed_buffer& operator=(fixed_buffer const&) = delete;

    private:
        friend class fixed_buffer_pool;
        fixed_buffer(fixed_buffer_pool* pool, uint16_t idx) noexcept;

        fixed_buffer_pool* pool_{nullptr};
        uint16_t idx_{0};
    };

    /// Allocate buf_count buffers of buf_size bytes each and register them
    /// with the current thread's io_uring ring.  buf_count is rounded up to
    /// the next power of two (minimum 2); the internal free-list channel is
    /// allocated at twice that size to accommodate all indices without blocking.
    fixed_buffer_pool(std::size_t buf_size, std::size_t buf_count);
    ~fixed_buffer_pool();
    fixed_buffer_pool(fixed_buffer_pool const&) = delete;
    fixed_buffer_pool& operator=(fixed_buffer_pool const&) = delete;
    fixed_buffer_pool(fixed_buffer_pool&&) = delete;
    fixed_buffer_pool& operator=(fixed_buffer_pool&&) = delete;

    /// Borrow one buffer from the pool.  Suspends the calling fiber if all
    /// buffers are currently in use.
    fixed_buffer borrow();

private:
    void return_buffer(uint16_t idx) noexcept;

    io_uring* ring_;
    std::size_t buf_size_;
    std::size_t buf_count_;
    std::vector<std::byte> storage_;
    channel<uint16_t> free_list_;
};

/// Borrow one fixed buffer from the calling thread's per-thread pool.
///
/// The pool must have been enabled via `context_options::fixed_buffer_size` /
/// `fixed_buffer_count` before the `context` was constructed.  Suspends the
/// calling fiber if all buffers are currently in use.
///
/// @throws std::runtime_error if no fixed-buffer pool was configured for this thread.
[[nodiscard]] fixed_buffer_pool::fixed_buffer borrow_fixed_buffer();

/// Zero-copy send using a pre-registered fixed buffer.
///
/// Submits IORING_OP_SEND_ZC with IORING_RECVSEND_FIXED_BUF, referencing buf
/// by its registered index.  Suspends the calling fiber until both the send
/// CQE and the kernel's buffer-release notification CQE arrive.  The kernel
/// reads directly from the registered buffer with no intermediate copy.
///
/// buf must have been borrowed from the thread-local fixed_buffer_pool (via
/// `borrow_fixed_buffer()`).  Returns the number of bytes sent.
ssize_t async_send_zc(int fd, fixed_buffer_pool::fixed_buffer const& buf, std::size_t nbytes, int flags = 0);

} // namespace fiberexec
