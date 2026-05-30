#pragma once

#include <fiberexec/detail/cqe_handler.hpp>

#include <boost/fiber/future/promise.hpp>

#include <cstdint>
#include <optional>
#include <queue>
#include <sys/socket.h>

namespace fiberexec {

/// RAII wrapper around IORING_ACCEPT_MULTISHOT.
///
/// One SQE stays armed in the ring for the lifetime of the object; the
/// scheduler resubmits it automatically whenever the kernel consumes it
/// without error (e.g. under resource pressure).  next() suspends the
/// calling fiber — the OS thread is never blocked — and returns the next
/// accepted file descriptor.
///
/// If the multishot SQE has not been terminated when the object is destroyed
/// (i.e. next() has not yet returned nullopt), the destructor calls
/// ::shutdown(server_fd, SHUT_RDWR) and drains remaining connections until
/// the SQE terminates.
///
/// Must be constructed and used from a fiber running on a fiberexec worker.
class multishot_acceptor : private detail::cqe_handler {
public:
    /// Construct and immediately arm the multishot accept SQE on @p server_fd.
    ///
    /// @p addr and @p addrlen are passed to each accept call; nullptr is fine
    /// if the peer address is not needed.
    multishot_acceptor(int server_fd, sockaddr* addr, socklen_t* addrlen);

    ~multishot_acceptor() override;

    multishot_acceptor(multishot_acceptor const&) = delete;
    multishot_acceptor& operator=(multishot_acceptor const&) = delete;
    multishot_acceptor(multishot_acceptor&&) = delete;
    multishot_acceptor& operator=(multishot_acceptor&&) = delete;

    /// Return the next accepted file descriptor, or nullopt when the acceptor
    /// has terminated cleanly.
    ///
    /// Drains any fds buffered from prior CQEs first, then suspends the fiber
    /// until a new connection arrives or the multishot SQE terminates.
    /// Returns nullopt on ECANCELED (explicit cancel or stop token) or EINVAL
    /// (::shutdown on the server fd).  Throws std::system_error for any other
    /// I/O error.
    /// The returned fd is owned by the caller.
    std::optional<int> next();

private:
    void complete(io_uring* ring, int res, uint32_t flags) noexcept override;

    int server_fd_;
    sockaddr* addr_;
    socklen_t* addrlen_;
    std::queue<int> pending_fds_;
    std::optional<boost::fibers::promise<int>> waiter_;
    int error_{0};
};

} // namespace fiberexec
