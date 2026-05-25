#include <fiberexec/async_io.hpp>
#include <fiberexec/detail/fiber_ops.hpp>

#include <liburing.h>

#include <cerrno>
#include <chrono>
#include <stdexcept>
#include <stop_token>
#include <system_error>

namespace fiberexec {

namespace {

// Validate fiber context, resolve the stop token, and check for
// pre-cancellation. Returns the current thread's ring so the caller can
// acquire an SQE at the right point (e.g. after any early-return guards).
io_uring* begin_async_op(std::stop_token& st, char const* caller) {
    io_uring* ring = detail::current_ring();
    if (ring == nullptr) {
        throw std::runtime_error(std::string(caller) + " called outside of a fiberexec fiber");
    }
    if (!st.stop_possible()) {
        st = detail::current_fiber_stop_token();
    }
    if (st.stop_requested()) {
        throw std::system_error(ECANCELED, std::system_category(), caller);
    }
    return ring;
}

} // namespace

ssize_t async_read(int fd, void* buf, std::size_t len, std::stop_token st) {
    io_uring_sqe* sqe = io_uring_get_sqe(begin_async_op(st, "async_read"));
    io_uring_prep_read(sqe, fd, buf, static_cast<unsigned>(len), 0);
    int const res = detail::submit_and_wait(sqe, std::move(st));
    if (res < 0) {
        throw std::system_error(-res, std::system_category(), "async_read");
    }
    return static_cast<ssize_t>(res);
}

ssize_t async_write(int fd, void const* buf, std::size_t len, std::stop_token st) {
    io_uring_sqe* sqe = io_uring_get_sqe(begin_async_op(st, "async_write"));
    io_uring_prep_write(sqe, fd, buf, static_cast<unsigned>(len), 0);
    int const res = detail::submit_and_wait(sqe, std::move(st));
    if (res < 0) {
        throw std::system_error(-res, std::system_category(), "async_write");
    }
    return static_cast<ssize_t>(res);
}

int async_accept(int fd, sockaddr* addr, socklen_t* addrlen, std::stop_token st) {
    io_uring_sqe* sqe = io_uring_get_sqe(begin_async_op(st, "async_accept"));
    io_uring_prep_accept(sqe, fd, addr, addrlen, 0);
    int const res = detail::submit_and_wait(sqe, std::move(st));
    if (res < 0) {
        throw std::system_error(-res, std::system_category(), "async_accept");
    }
    return res;
}

void async_connect(int fd, sockaddr const* addr, socklen_t addrlen, std::stop_token st) {
    io_uring_sqe* sqe = io_uring_get_sqe(begin_async_op(st, "async_connect"));
    // io_uring_prep_connect takes a non-const addr; the kernel does not modify it.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
    io_uring_prep_connect(sqe, fd, const_cast<sockaddr*>(addr), addrlen);
    int const res = detail::submit_and_wait(sqe, std::move(st));
    if (res < 0) {
        throw std::system_error(-res, std::system_category(), "async_connect");
    }
}

ssize_t async_recv(int fd, void* buf, std::size_t len, int flags, std::stop_token st) {
    io_uring_sqe* sqe = io_uring_get_sqe(begin_async_op(st, "async_recv"));
    io_uring_prep_recv(sqe, fd, buf, len, flags);
    int const res = detail::submit_and_wait(sqe, std::move(st));
    if (res < 0) {
        throw std::system_error(-res, std::system_category(), "async_recv");
    }
    return static_cast<ssize_t>(res);
}

ssize_t async_send(int fd, void const* buf, std::size_t len, int flags, std::stop_token st) {
    io_uring_sqe* sqe = io_uring_get_sqe(begin_async_op(st, "async_send"));
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
    io_uring_prep_send(sqe, fd, const_cast<void*>(buf), len, flags);
    int const res = detail::submit_and_wait(sqe, std::move(st));
    if (res < 0) {
        throw std::system_error(-res, std::system_category(), "async_send");
    }
    return static_cast<ssize_t>(res);
}

void async_sleep_for(std::chrono::nanoseconds duration, std::stop_token st) {
    io_uring* ring = begin_async_op(st, "async_sleep_for");
    if (duration.count() <= 0) {
        return;
    }
    // ts lives on the fiber's stack and remains valid while the fiber is
    // suspended: Boost.Fiber preserves the stack until the fiber is resumed.
    __kernel_timespec ts{};
    ts.tv_sec = duration.count() / 1'000'000'000LL;
    ts.tv_nsec = duration.count() % 1'000'000'000LL;
    io_uring_sqe* sqe = io_uring_get_sqe(ring);
    io_uring_prep_timeout(sqe, &ts, 0, 0);
    int const res = detail::submit_and_wait(sqe, std::move(st));
    // -ETIME means the timeout fired normally; anything else is an error.
    if (res != -ETIME) {
        throw std::system_error(-res, std::system_category(), "async_sleep_for");
    }
}

} // namespace fiberexec
