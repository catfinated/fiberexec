#pragma once

#include <fiberexec/detail/cqe_handler.hpp>

#include <boost/fiber/future/promise.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <queue>
#include <span>
#include <vector>

struct io_uring_buf_ring;

namespace fiberexec {

/// RAII wrapper around IORING_RECV_MULTISHOT with a kernel buffer ring.
///
/// One SQE stays armed for the lifetime of the object; the scheduler rearms
/// it automatically whenever the kernel consumes it without error.  The kernel
/// selects a buffer from the ring for each arriving message, avoiding any
/// per-recv allocation.  next() suspends the calling fiber and returns a
/// received_buffer whose data() span is valid until the object is destroyed.
///
/// If next() has not returned nullopt before this object is destroyed, the
/// destructor cancels the in-flight SQE and drains any remaining data.
///
/// Must be constructed and used from a fiber running on a fiberexec worker.
class multishot_recv : private detail::cqe_handler {
public:
    /// RAII handle for one kernel-selected buffer.
    ///
    /// Returns the buffer to the ring on destruction.  Do not use the span
    /// from data() after the received_buffer is destroyed.  Must be destroyed
    /// on the same worker thread it was created on.
    class received_buffer {
    public:
        [[nodiscard]] std::span<std::byte const> data() const noexcept;

        ~received_buffer();
        received_buffer(received_buffer&& other) noexcept;
        received_buffer& operator=(received_buffer&& other) noexcept;
        received_buffer(received_buffer const&) = delete;
        received_buffer& operator=(received_buffer const&) = delete;

    private:
        friend class multishot_recv;
        received_buffer(multishot_recv* owner, uint16_t buf_id, std::size_t size) noexcept;

        multishot_recv* owner_{nullptr};
        uint16_t buf_id_{0};
        std::size_t size_{0};
    };

    /// Construct, allocate a buffer ring of @p buf_count buffers of
    /// @p buf_size bytes each, and arm the multishot recv SQE on @p fd.
    ///
    /// @p buf_count is rounded up to the next power of two.
    multishot_recv(int fd, std::size_t buf_size, std::size_t buf_count);
    ~multishot_recv() override;
    multishot_recv(multishot_recv const&) = delete;
    multishot_recv& operator=(multishot_recv const&) = delete;
    multishot_recv(multishot_recv&&) = delete;
    multishot_recv& operator=(multishot_recv&&) = delete;

    /// Return the next received buffer, or nullopt when the stream ends.
    ///
    /// Drains any buffers already received from prior CQEs first, then
    /// suspends the fiber until data arrives or the SQE terminates.
    /// Returns nullopt on EOF, ECANCELED, or EINVAL.  Throws
    /// std::system_error for any other I/O error (e.g. ENOBUFS if all
    /// buffers are in use when a message arrives).
    std::optional<received_buffer> next();

private:
    void complete(io_uring* ring, int res, uint32_t flags) noexcept override;
    void arm(io_uring* ring) noexcept;
    void return_buffer(uint16_t buf_id) noexcept;

    int fd_;
    uint16_t bgid_;
    std::size_t buf_size_;
    std::size_t buf_count_;
    std::vector<std::byte> buf_storage_;
    io_uring_buf_ring* buf_ring_{nullptr};

    struct pending_item {
        uint16_t buf_id;
        std::size_t size;
    };
    std::queue<pending_item> pending_;
    std::optional<boost::fibers::promise<void>> waiter_;
    // 0 = SQE live; negative errno = error; -ENODATA = EOF
    int error_{0};
};

} // namespace fiberexec
