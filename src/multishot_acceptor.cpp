#include <fiberexec/multishot_acceptor.hpp>

#include <fiberexec/async_io.hpp>
#include <fiberexec/detail/fiber_ops.hpp>

#include <liburing.h>

#include <cassert>
#include <cerrno>
#include <stdexcept>
#include <system_error>

namespace fiberexec {

multishot_acceptor::multishot_acceptor(int server_fd, sockaddr* addr, socklen_t* addrlen)
    : server_fd_{server_fd}
    , addr_{addr}
    , addrlen_{addrlen} {
    io_uring* ring = detail::current_ring();
    if (ring == nullptr) {
        throw std::runtime_error("multishot_acceptor constructed outside of a fiberexec fiber");
    }
    io_uring_sqe* sqe = io_uring_get_sqe(ring);
    if (sqe == nullptr) {
        throw std::runtime_error("multishot_acceptor: io_uring ring full");
    }
    io_uring_prep_multishot_accept(sqe, server_fd_, addr_, addrlen_, 0);
    io_uring_sqe_set_data(sqe, static_cast<detail::cqe_handler*>(this));
    if (int ret = io_uring_submit(ring); ret < 0) {
        throw std::system_error(-ret, std::system_category(), "io_uring_submit (multishot_accept)");
    }
}

multishot_acceptor::~multishot_acceptor() {
    try {
        if (error_ != 0) {
            // SQE already terminated; close any fds buffered before the error CQE.
            while (!pending_fds_.empty()) {
                async_close(pending_fds_.front());
                pending_fds_.pop();
            }
            return;
        }
        // SQE still in flight; terminate it and drain remaining connections.
        ::shutdown(server_fd_, SHUT_RDWR);
        while (auto fd = next()) {
            async_close(*fd);
        }
    } catch (...) {
    }
}

void multishot_acceptor::complete(io_uring* ring, int res, uint32_t flags) noexcept {
    if (res < 0) {
        error_ = res;
        if (waiter_) {
            waiter_->set_value(res);
            waiter_.reset();
        }
    } else {
        if (waiter_) {
            waiter_->set_value(res);
            waiter_.reset();
        } else {
            pending_fds_.push(res);
        }
    }
    // Rearm if the kernel consumed the SQE without error; a negative res means
    // the SQE is terminated and no further completions will arrive.
    if ((flags & IORING_CQE_F_MORE) == 0 && res >= 0) {
        io_uring_sqe* sqe = io_uring_get_sqe(ring);
        if (sqe != nullptr) {
            io_uring_prep_multishot_accept(sqe, server_fd_, addr_, addrlen_, 0);
            io_uring_sqe_set_data(sqe, static_cast<detail::cqe_handler*>(this));
            io_uring_submit(ring);
        }
    }
}

std::optional<int> multishot_acceptor::next() {
    if (!pending_fds_.empty()) {
        int fd = pending_fds_.front();
        pending_fds_.pop();
        return fd;
    }

    if (error_ != 0) {
        if (error_ == -ECANCELED || error_ == -EINVAL) {
            return std::nullopt;
        }
        throw std::system_error(-error_, std::system_category(), "multishot_acceptor::next");
    }

    auto st = detail::current_fiber_stop_token();
    if (st.stop_requested()) {
        error_ = -ECANCELED;
        return std::nullopt;
    }

    boost::fibers::promise<int> promise;
    auto future = promise.get_future();
    waiter_ = std::move(promise);

    int const res = future.get();
    if (res == -ECANCELED || res == -EINVAL) {
        // ECANCELED: explicit io_uring cancel or stop token.
        // EINVAL: kernel terminated the multishot SQE because the socket was
        // shut down (::shutdown(fd, SHUT_RDWR)).
        return std::nullopt;
    }
    if (res < 0) {
        throw std::system_error(-res, std::system_category(), "multishot_acceptor::next");
    }
    return res;
}

} // namespace fiberexec
