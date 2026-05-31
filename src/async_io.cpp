#include <fiberexec/async_io.hpp>
#include <fiberexec/detail/async_helpers.hpp>

#include <liburing.h>

#include <chrono>
#include <optional>
#include <span>
#include <system_error>

namespace fiberexec {

ssize_t async_read(int fd, std::span<std::byte> buf, std::optional<std::chrono::nanoseconds> timeout) {
    io_uring_sqe* sqe = io_uring_get_sqe(detail::begin_async_op("async_read"));
    io_uring_prep_read(sqe, fd, buf.data(), static_cast<unsigned>(buf.size()), 0);
    int const res = detail::dispatch(sqe, timeout);
    if (res < 0) {
        throw std::system_error(-res, std::system_category(), "async_read");
    }
    return static_cast<ssize_t>(res);
}

ssize_t async_write(int fd, std::span<std::byte const> buf, std::optional<std::chrono::nanoseconds> timeout) {
    io_uring_sqe* sqe = io_uring_get_sqe(detail::begin_async_op("async_write"));
    io_uring_prep_write(sqe, fd, buf.data(), static_cast<unsigned>(buf.size()), 0);
    int const res = detail::dispatch(sqe, timeout);
    if (res < 0) {
        throw std::system_error(-res, std::system_category(), "async_write");
    }
    return static_cast<ssize_t>(res);
}

int async_accept(int fd, sockaddr* addr, socklen_t* addrlen, std::optional<std::chrono::nanoseconds> timeout) {
    io_uring_sqe* sqe = io_uring_get_sqe(detail::begin_async_op("async_accept"));
    io_uring_prep_accept(sqe, fd, addr, addrlen, 0);
    int const res = detail::dispatch(sqe, timeout);
    if (res < 0) {
        throw std::system_error(-res, std::system_category(), "async_accept");
    }
    return res;
}

int async_openat(int dirfd, char const* path, int flags, mode_t mode, std::optional<std::chrono::nanoseconds> timeout) {
    io_uring_sqe* sqe = io_uring_get_sqe(detail::begin_async_op("async_openat"));
    io_uring_prep_openat(sqe, dirfd, path, flags, mode);
    int const res = detail::dispatch(sqe, timeout);
    if (res < 0) {
        throw std::system_error(-res, std::system_category(), "async_openat");
    }
    return res;
}

void async_close(int fd) {
    io_uring* ring = detail::current_ring();
    if (ring == nullptr) {
        throw std::runtime_error("async_close called outside of a fiberexec fiber");
    }
    io_uring_sqe* sqe = io_uring_get_sqe(ring);
    io_uring_prep_close(sqe, fd);
    int const res = detail::submit_and_wait(sqe);
    if (res < 0) {
        throw std::system_error(-res, std::system_category(), "async_close");
    }
}

void async_connect(int fd, sockaddr const* addr, socklen_t addrlen, std::optional<std::chrono::nanoseconds> timeout) {
    io_uring_sqe* sqe = io_uring_get_sqe(detail::begin_async_op("async_connect"));
    // io_uring_prep_connect takes a non-const addr; the kernel does not modify it.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
    io_uring_prep_connect(sqe, fd, const_cast<sockaddr*>(addr), addrlen);
    int const res = detail::dispatch(sqe, timeout);
    if (res < 0) {
        throw std::system_error(-res, std::system_category(), "async_connect");
    }
}

ssize_t async_recv(int fd, std::span<std::byte> buf, int flags, std::optional<std::chrono::nanoseconds> timeout) {
    io_uring_sqe* sqe = io_uring_get_sqe(detail::begin_async_op("async_recv"));
    io_uring_prep_recv(sqe, fd, buf.data(), buf.size(), flags);
    int const res = detail::dispatch(sqe, timeout);
    if (res < 0) {
        throw std::system_error(-res, std::system_category(), "async_recv");
    }
    return static_cast<ssize_t>(res);
}

ssize_t async_send(int fd, std::span<std::byte const> buf, int flags, std::optional<std::chrono::nanoseconds> timeout) {
    io_uring_sqe* sqe = io_uring_get_sqe(detail::begin_async_op("async_send"));
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
    io_uring_prep_send(sqe, fd, const_cast<void*>(static_cast<void const*>(buf.data())), buf.size(), flags);
    int const res = detail::dispatch(sqe, timeout);
    if (res < 0) {
        throw std::system_error(-res, std::system_category(), "async_send");
    }
    return static_cast<ssize_t>(res);
}

void async_sleep_for(std::chrono::nanoseconds duration) {
    io_uring* ring = detail::begin_async_op("async_sleep_for");
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
    int const res = detail::submit_and_wait(sqe);
    // -ETIME means the timeout fired normally; anything else is an error.
    if (res != -ETIME) {
        throw std::system_error(-res, std::system_category(), "async_sleep_for");
    }
}

} // namespace fiberexec
