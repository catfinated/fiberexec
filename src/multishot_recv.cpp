#include <fiberexec/multishot_recv.hpp>

#include <fiberexec/detail/fiber_ops.hpp>

#include <liburing.h>

#include <bit>
#include <cassert>
#include <cerrno>
#include <stdexcept>
#include <system_error>

namespace fiberexec {

namespace {
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
thread_local uint16_t tl_next_bgid = 0;
} // namespace

// ---------------------------------------------------------------------------
// received_buffer
// ---------------------------------------------------------------------------

multishot_recv::received_buffer::received_buffer(multishot_recv* owner, uint16_t buf_id, std::size_t size) noexcept
    : owner_{owner}
    , buf_id_{buf_id}
    , size_{size} {}

multishot_recv::received_buffer::received_buffer(received_buffer&& other) noexcept
    : owner_{other.owner_}
    , buf_id_{other.buf_id_}
    , size_{other.size_} {
    other.owner_ = nullptr;
}

multishot_recv::received_buffer& multishot_recv::received_buffer::operator=(received_buffer&& other) noexcept {
    if (this != &other) {
        if (owner_ != nullptr) {
            owner_->return_buffer(buf_id_);
        }
        owner_ = other.owner_;
        buf_id_ = other.buf_id_;
        size_ = other.size_;
        other.owner_ = nullptr;
    }
    return *this;
}

multishot_recv::received_buffer::~received_buffer() {
    if (owner_ != nullptr) {
        owner_->return_buffer(buf_id_);
    }
}

std::span<std::byte const> multishot_recv::received_buffer::data() const noexcept {
    return std::span<std::byte const>{owner_->buf_storage_.data(), owner_->buf_storage_.size()}.subspan(
        buf_id_ * owner_->buf_size_, size_);
}

// ---------------------------------------------------------------------------
// multishot_recv
// ---------------------------------------------------------------------------

multishot_recv::multishot_recv(int fd, std::size_t buf_size, std::size_t buf_count)
    : fd_{fd}
    , bgid_{tl_next_bgid++}
    , buf_size_{buf_size}
    , buf_count_{std::bit_ceil(buf_count)} {
    if (buf_size_ == 0 || buf_count == 0) {
        throw std::invalid_argument("multishot_recv: buf_size and buf_count must be non-zero");
    }

    io_uring* ring = detail::current_ring();
    if (ring == nullptr) {
        throw std::runtime_error("multishot_recv constructed outside of a fiberexec fiber");
    }

    buf_storage_.resize(buf_size_ * buf_count_);

    int ret = 0;
    buf_ring_ = io_uring_setup_buf_ring(ring, static_cast<unsigned int>(buf_count_), bgid_, 0, &ret);
    if (buf_ring_ == nullptr) {
        throw std::system_error(-ret, std::system_category(), "io_uring_setup_buf_ring");
    }

    int const mask = io_uring_buf_ring_mask(static_cast<unsigned int>(buf_count_));
    auto const buf_span = std::span<std::byte>{buf_storage_.data(), buf_storage_.size()};
    for (std::size_t i = 0; i < buf_count_; ++i) {
        io_uring_buf_ring_add(buf_ring_, buf_span.subspan(i * buf_size_, buf_size_).data(),
                              static_cast<unsigned int>(buf_size_), static_cast<uint16_t>(i), mask,
                              static_cast<int>(i));
    }
    io_uring_buf_ring_advance(buf_ring_, static_cast<int>(buf_count_));

    io_uring_sqe* sqe = io_uring_get_sqe(ring);
    if (sqe == nullptr) {
        io_uring_free_buf_ring(ring, buf_ring_, static_cast<unsigned int>(buf_count_), bgid_);
        buf_ring_ = nullptr;
        throw std::runtime_error("multishot_recv: io_uring ring full");
    }
    io_uring_prep_recv_multishot(sqe, fd_, nullptr, 0, 0);
    sqe->flags |= IOSQE_BUFFER_SELECT;
    sqe->buf_group = bgid_; // NOLINT(cppcoreguidelines-pro-type-union-access)
    io_uring_sqe_set_data(sqe, static_cast<detail::cqe_handler*>(this));
    if (int r = io_uring_submit(ring); r < 0) {
        io_uring_free_buf_ring(ring, buf_ring_, static_cast<unsigned int>(buf_count_), bgid_);
        buf_ring_ = nullptr;
        throw std::system_error(-r, std::system_category(), "io_uring_submit (multishot_recv)");
    }
}

multishot_recv::~multishot_recv() {
    try {
        // Return any buffers already received but not consumed by the caller.
        while (!pending_.empty()) {
            return_buffer(pending_.front().buf_id);
            pending_.pop();
        }

        if (error_ == 0) {
            // SQE still in flight; cancel it and drain remaining data.
            detail::submit_cancel(static_cast<detail::cqe_handler*>(this));
            while (next()) {
            }
        }
    } catch (...) {
    }

    if (buf_ring_ != nullptr) {
        if (io_uring* ring = detail::current_ring(); ring != nullptr) {
            io_uring_free_buf_ring(ring, buf_ring_, static_cast<unsigned int>(buf_count_), bgid_);
        }
        buf_ring_ = nullptr;
    }
}

void multishot_recv::arm(io_uring* ring) noexcept {
    io_uring_sqe* sqe = io_uring_get_sqe(ring);
    if (sqe == nullptr) {
        return;
    }
    io_uring_prep_recv_multishot(sqe, fd_, nullptr, 0, 0);
    sqe->flags |= IOSQE_BUFFER_SELECT;
    sqe->buf_group = bgid_; // NOLINT(cppcoreguidelines-pro-type-union-access)
    io_uring_sqe_set_data(sqe, static_cast<detail::cqe_handler*>(this));
    io_uring_submit(ring);
}

void multishot_recv::return_buffer(uint16_t buf_id) noexcept {
    auto const buf_span = std::span<std::byte>{buf_storage_.data(), buf_storage_.size()};
    io_uring_buf_ring_add(buf_ring_, buf_span.subspan(buf_id * buf_size_, buf_size_).data(),
                          static_cast<unsigned int>(buf_size_), buf_id,
                          io_uring_buf_ring_mask(static_cast<unsigned int>(buf_count_)), 0);
    io_uring_buf_ring_advance(buf_ring_, 1);
}

void multishot_recv::complete(io_uring* ring, int res, uint32_t flags) noexcept {
    bool const has_buf = (flags & IORING_CQE_F_BUFFER) != 0;
    auto const buf_id = static_cast<uint16_t>(flags >> IORING_CQE_BUFFER_SHIFT);

    if (res > 0) {
        pending_.push({.buf_id = buf_id, .size = static_cast<std::size_t>(res)});
    } else {
        // EOF (res == 0) or error; return any buffer the kernel selected unused.
        if (has_buf) {
            return_buffer(buf_id);
        }
        error_ = (res == 0) ? -ENODATA : res;
    }

    if (waiter_) {
        waiter_->set_value();
        waiter_.reset();
    }

    // Rearm if the kernel consumed the SQE without a terminal error.
    if ((flags & IORING_CQE_F_MORE) == 0 && res > 0) {
        arm(ring);
    }
}

std::optional<multishot_recv::received_buffer> multishot_recv::next() {
    if (!pending_.empty()) {
        auto item = pending_.front();
        pending_.pop();
        return received_buffer{this, item.buf_id, item.size};
    }

    if (error_ != 0) {
        if (error_ == -ENODATA || error_ == -ECANCELED || error_ == -EINVAL) {
            return std::nullopt;
        }
        throw std::system_error(-error_, std::system_category(), "multishot_recv::next");
    }

    auto st = detail::current_fiber_stop_token();
    if (st.stop_requested()) {
        error_ = -ECANCELED;
        return std::nullopt;
    }

    boost::fibers::promise<void> promise;
    auto future = promise.get_future();
    waiter_ = std::move(promise);
    future.get();

    if (!pending_.empty()) {
        auto item = pending_.front();
        pending_.pop();
        return received_buffer{this, item.buf_id, item.size};
    }

    if (error_ == -ENODATA || error_ == -ECANCELED || error_ == -EINVAL) {
        return std::nullopt;
    }
    throw std::system_error(-error_, std::system_category(), "multishot_recv::next");
}

} // namespace fiberexec
