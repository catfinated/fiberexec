#include <fiberexec/async_io.hpp>
#include <fiberexec/detail/async_helpers.hpp>

#include <liburing.h>

#include <array>
#include <chrono>
#include <optional>
#include <span>
#include <system_error>

namespace fiberexec {

ssize_t async_read(fd_ref fd, std::span<std::byte> buf, std::optional<std::chrono::nanoseconds> timeout) {
    io_uring_sqe* sqe = io_uring_get_sqe(detail::begin_async_op("async_read"));
    io_uring_prep_read(sqe, fd.index, buf.data(), static_cast<unsigned>(buf.size()), 0);
    if (fd.fixed) {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    int const res = detail::dispatch(sqe, timeout);
    if (res < 0) {
        throw std::system_error(-res, std::system_category(), "async_read");
    }
    return static_cast<ssize_t>(res);
}

ssize_t async_write(fd_ref fd, std::span<std::byte const> buf, std::optional<std::chrono::nanoseconds> timeout) {
    io_uring_sqe* sqe = io_uring_get_sqe(detail::begin_async_op("async_write"));
    io_uring_prep_write(sqe, fd.index, buf.data(), static_cast<unsigned>(buf.size()), 0);
    if (fd.fixed) {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
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

void async_connect(fd_ref fd,
                   sockaddr const* addr,
                   socklen_t addrlen,
                   std::optional<std::chrono::nanoseconds> timeout) {
    io_uring_sqe* sqe = io_uring_get_sqe(detail::begin_async_op("async_connect"));
    // io_uring_prep_connect takes a non-const addr; the kernel does not modify it.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
    io_uring_prep_connect(sqe, fd.index, const_cast<sockaddr*>(addr), addrlen);
    if (fd.fixed) {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    int const res = detail::dispatch(sqe, timeout);
    if (res < 0) {
        throw std::system_error(-res, std::system_category(), "async_connect");
    }
}

ssize_t async_recv(fd_ref fd, std::span<std::byte> buf, int flags, std::optional<std::chrono::nanoseconds> timeout) {
    io_uring_sqe* sqe = io_uring_get_sqe(detail::begin_async_op("async_recv"));
    io_uring_prep_recv(sqe, fd.index, buf.data(), buf.size(), flags);
    if (fd.fixed) {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    int const res = detail::dispatch(sqe, timeout);
    if (res < 0) {
        throw std::system_error(-res, std::system_category(), "async_recv");
    }
    return static_cast<ssize_t>(res);
}

ssize_t
async_send(fd_ref fd, std::span<std::byte const> buf, int flags, std::optional<std::chrono::nanoseconds> timeout) {
    io_uring_sqe* sqe = io_uring_get_sqe(detail::begin_async_op("async_send"));
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
    io_uring_prep_send(sqe, fd.index, const_cast<void*>(static_cast<void const*>(buf.data())), buf.size(), flags);
    if (fd.fixed) {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    int const res = detail::dispatch(sqe, timeout);
    if (res < 0) {
        throw std::system_error(-res, std::system_category(), "async_send");
    }
    return static_cast<ssize_t>(res);
}

std::pair<ssize_t, ssize_t> async_send_recv(
    fd_ref fd, std::span<std::byte const> send_buf, std::span<std::byte> recv_buf, int send_flags, int recv_flags) {
    io_uring* ring = detail::begin_async_op("async_send_recv");
    io_uring_sqe* sqe0 = io_uring_get_sqe(ring);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
    io_uring_prep_send(sqe0, fd.index, const_cast<void*>(static_cast<void const*>(send_buf.data())), send_buf.size(),
                       send_flags);
    if (fd.fixed) {
        sqe0->flags |= IOSQE_FIXED_FILE;
    }
    io_uring_sqe* sqe1 = io_uring_get_sqe(ring);
    io_uring_prep_recv(sqe1, fd.index, recv_buf.data(), recv_buf.size(), recv_flags);
    if (fd.fixed) {
        sqe1->flags |= IOSQE_FIXED_FILE;
    }

    std::array<io_uring_sqe*, 2> sqes = {sqe0, sqe1};
    std::array<int, 2> res = {};
    detail::submit_linked_and_wait(sqes, res);

    if (res.front() < 0) {
        throw std::system_error(-res.front(), std::system_category(), "async_send_recv (send)");
    }
    if (res.back() < 0) {
        throw std::system_error(-res.back(), std::system_category(), "async_send_recv (recv)");
    }
    return {static_cast<ssize_t>(res.front()), static_cast<ssize_t>(res.back())};
}

ssize_t async_write_fsync(fd_ref fd, std::span<std::byte const> buf) {
    io_uring* ring = detail::begin_async_op("async_write_fsync");
    io_uring_sqe* sqe0 = io_uring_get_sqe(ring);
    io_uring_prep_write(sqe0, fd.index, buf.data(), static_cast<unsigned>(buf.size()), 0);
    if (fd.fixed) {
        sqe0->flags |= IOSQE_FIXED_FILE;
    }
    io_uring_sqe* sqe1 = io_uring_get_sqe(ring);
    io_uring_prep_fsync(sqe1, fd.index, 0);
    if (fd.fixed) {
        sqe1->flags |= IOSQE_FIXED_FILE;
    }

    std::array<io_uring_sqe*, 2> sqes = {sqe0, sqe1};
    std::array<int, 2> res = {};
    detail::submit_linked_and_wait(sqes, res);

    if (res.front() < 0) {
        throw std::system_error(-res.front(), std::system_category(), "async_write_fsync (write)");
    }
    if (res.back() < 0) {
        throw std::system_error(-res.back(), std::system_category(), "async_write_fsync (fsync)");
    }
    return static_cast<ssize_t>(res.front());
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
