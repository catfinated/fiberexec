#include <fiberexec/async_io.hpp>
#include <fiberexec/fiber_context.hpp>

#include <liburing.h>

#include <stdexcept>
#include <system_error>

namespace fiberexec {

ssize_t async_read(int fd, void* buf, std::size_t len) {
    io_uring* ring = detail::current_ring();
    if (ring == nullptr) {
        throw std::runtime_error("async_read called outside of a fiberexec fiber");
    }
    io_uring_sqe* sqe = io_uring_get_sqe(ring);
    io_uring_prep_read(sqe, fd, buf, static_cast<unsigned>(len), 0);
    int const res = detail::submit_and_wait(sqe);
    if (res < 0) {
        throw std::system_error(-res, std::system_category(), "async_read");
    }
    return static_cast<ssize_t>(res);
}

ssize_t async_write(int fd, void const* buf, std::size_t len) {
    io_uring* ring = detail::current_ring();
    if (ring == nullptr) {
        throw std::runtime_error("async_write called outside of a fiberexec fiber");
    }
    io_uring_sqe* sqe = io_uring_get_sqe(ring);
    io_uring_prep_write(sqe, fd, buf, static_cast<unsigned>(len), 0);
    int const res = detail::submit_and_wait(sqe);
    if (res < 0) {
        throw std::system_error(-res, std::system_category(), "async_write");
    }
    return static_cast<ssize_t>(res);
}

} // namespace fiberexec
