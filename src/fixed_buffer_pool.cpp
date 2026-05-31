#include <fiberexec/fixed_buffer_pool.hpp>

#include <fiberexec/detail/cqe_handler.hpp>
#include <fiberexec/detail/fiber_ops.hpp>

#include <boost/fiber/future/promise.hpp>

#include <liburing.h>
#include <sys/uio.h>

#include <bit>
#include <cassert>
#include <cerrno>
#include <stdexcept>
#include <system_error>

namespace fiberexec {

namespace {

// Two-phase CQE handler for IORING_OP_SEND_ZC.
//
// The kernel delivers two CQEs per SEND_ZC operation:
//   1. Send CQE (res = bytes sent or error; IORING_CQE_F_MORE set when a
//      notification will follow).
//   2. Notification CQE (IORING_CQE_F_NOTIF set; res = 0): the kernel has
//      released its reference to the buffer.
//
// The fiber is suspended until the second CQE arrives (or until the first if
// no notification is expected, e.g. on error).
class send_zc_handler : public detail::cqe_handler {
public:
    send_zc_handler() = default;

    void complete(io_uring* /*ring*/, int res, uint32_t flags) noexcept override {
        if ((flags & IORING_CQE_F_NOTIF) != 0) {
            // Buffer released by kernel — wake the fiber.
            promise_.set_value();
        } else {
            result_ = res;
            if ((flags & IORING_CQE_F_MORE) == 0) {
                // No notification coming (error, or inline completion).
                promise_.set_value();
            }
        }
    }

    int wait() {
        promise_.get_future().get();
        return result_;
    }

private:
    boost::fibers::promise<void> promise_;
    int result_{0};
};

} // namespace

// ---------------------------------------------------------------------------
// fixed_buffer
// ---------------------------------------------------------------------------

fixed_buffer_pool::fixed_buffer::fixed_buffer(fixed_buffer_pool* pool, uint16_t idx) noexcept
    : pool_{pool}
    , idx_{idx} {}

fixed_buffer_pool::fixed_buffer::fixed_buffer(fixed_buffer&& other) noexcept
    : pool_{other.pool_}
    , idx_{other.idx_} {
    other.pool_ = nullptr;
}

fixed_buffer_pool::fixed_buffer& fixed_buffer_pool::fixed_buffer::operator=(fixed_buffer&& other) noexcept {
    if (this != &other) {
        if (pool_ != nullptr) {
            pool_->return_buffer(idx_);
        }
        pool_ = other.pool_;
        idx_ = other.idx_;
        other.pool_ = nullptr;
    }
    return *this;
}

fixed_buffer_pool::fixed_buffer::~fixed_buffer() {
    if (pool_ != nullptr) {
        pool_->return_buffer(idx_);
    }
}

std::span<std::byte> fixed_buffer_pool::fixed_buffer::data() noexcept {
    return std::span<std::byte>{pool_->storage_.data(), pool_->storage_.size()}.subspan(idx_ * pool_->buf_size_,
                                                                                        pool_->buf_size_);
}

std::span<std::byte const> fixed_buffer_pool::fixed_buffer::data() const noexcept {
    return std::span<std::byte const>{pool_->storage_.data(), pool_->storage_.size()}.subspan(idx_ * pool_->buf_size_,
                                                                                              pool_->buf_size_);
}

// ---------------------------------------------------------------------------
// fixed_buffer_pool
// ---------------------------------------------------------------------------

fixed_buffer_pool::fixed_buffer_pool(std::size_t buf_size, std::size_t buf_count)
    : ring_{detail::current_ring()}
    , buf_size_{buf_size}
    , buf_count_{std::max(std::bit_ceil(buf_count), std::size_t{2})}
    , storage_(buf_size_ * buf_count_)
    , free_list_{buf_count_} {
    if (buf_size_ == 0 || buf_count_ == 0) {
        throw std::invalid_argument("fixed_buffer_pool: buf_size and buf_count must be non-zero");
    }
    if (ring_ == nullptr) {
        throw std::runtime_error("fixed_buffer_pool constructed outside of a fiberexec worker thread");
    }

    std::vector<iovec> iovecs(buf_count_);
    auto const buf_span = std::span<std::byte>{storage_.data(), storage_.size()};
    for (std::size_t i = 0; i < buf_count_; ++i) {
        auto slot = buf_span.subspan(i * buf_size_, buf_size_);
        iovecs.at(i) = {.iov_base = slot.data(), .iov_len = slot.size()};
    }

    if (int r = io_uring_register_buffers(ring_, iovecs.data(), static_cast<unsigned int>(buf_count_)); r < 0) {
        throw std::system_error(-r, std::system_category(), "io_uring_register_buffers");
    }

    for (uint16_t i = 0; i < static_cast<uint16_t>(buf_count_); ++i) {
        static_cast<void>(free_list_.push(i));
    }
}

fixed_buffer_pool::~fixed_buffer_pool() {
    free_list_.close();
    if (ring_ != nullptr) {
        io_uring_unregister_buffers(ring_);
    }
}

fixed_buffer_pool::fixed_buffer fixed_buffer_pool::borrow() {
    uint16_t idx{};
    auto status = free_list_.pop(idx);
    if (status != channel_op_status::success) {
        throw std::runtime_error("fixed_buffer_pool: pool closed");
    }
    return fixed_buffer{this, idx};
}

void fixed_buffer_pool::return_buffer(uint16_t idx) noexcept { static_cast<void>(free_list_.push(idx)); }

// ---------------------------------------------------------------------------
// async_send_zc
// ---------------------------------------------------------------------------

ssize_t async_send_zc(int fd, fixed_buffer_pool::fixed_buffer const& buf, std::size_t nbytes, int flags) {
    io_uring* ring = detail::current_ring();
    if (ring == nullptr) {
        throw std::runtime_error("async_send_zc called outside of a fiberexec fiber");
    }

    io_uring_sqe* sqe = io_uring_get_sqe(ring);
    if (sqe == nullptr) {
        throw std::runtime_error("async_send_zc: io_uring ring full");
    }

    send_zc_handler handler;
    io_uring_prep_send_zc_fixed(sqe, fd, buf.data().data(), nbytes, flags, 0, buf.buf_index());
    io_uring_sqe_set_data(sqe, static_cast<detail::cqe_handler*>(&handler));

    if (int r = io_uring_submit(ring); r < 0) {
        throw std::system_error(-r, std::system_category(), "io_uring_submit (async_send_zc)");
    }

    int const result = handler.wait();
    if (result < 0) {
        throw std::system_error(-result, std::system_category(), "async_send_zc");
    }
    return static_cast<ssize_t>(result);
}

fixed_buffer_pool::fixed_buffer borrow_fixed_buffer() {
    fixed_buffer_pool* pool = detail::current_fixed_buffer_pool();
    if (pool == nullptr) {
        throw std::runtime_error(
            "borrow_fixed_buffer called without fixed_buffer_size/fixed_buffer_count configured in context_options");
    }
    return pool->borrow();
}

} // namespace fiberexec
