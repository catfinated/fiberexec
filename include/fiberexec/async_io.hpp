#pragma once

#include <fiberexec/fd_ref.hpp>

#include <chrono>
#include <cstddef>
#include <optional>
#include <span>
#include <sys/socket.h>
#include <sys/types.h>

namespace fiberexec {

/// Asynchronously read bytes from @p fd into @p buf.
///
/// Submits an io_uring read request and suspends the calling fiber until the
/// completion event arrives — the OS thread is never blocked.  Must be called
/// from a fiber running on a fiberexec worker thread.
///
/// Cancellation is automatic: the fiber-local stop token (installed by
/// `fiberexec::run` or `stdexec::schedule`) and the pool-wide stop token are
/// both observed without any explicit threading.  If @p timeout is set and the
/// read does not complete in time, the operation is cancelled and throws
/// std::system_error(ECANCELED).
///
/// @returns Number of bytes read.
/// @throws std::system_error on I/O failure, timeout, or cancellation.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
ssize_t async_read(fd_ref fd, std::span<std::byte> buf, std::optional<std::chrono::nanoseconds> timeout = std::nullopt);

/// Asynchronously write @p buf to @p fd.
///
/// Submits an io_uring write request and suspends the calling fiber until the
/// completion event arrives — the OS thread is never blocked.  Must be called
/// from a fiber running on a fiberexec worker thread.
///
/// If @p timeout is set and the write does not complete in time, throws
/// std::system_error(ECANCELED).
///
/// @returns Number of bytes written.
/// @throws std::system_error on I/O failure, timeout, or cancellation.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
ssize_t
async_write(fd_ref fd, std::span<std::byte const> buf, std::optional<std::chrono::nanoseconds> timeout = std::nullopt);

/// Accept an incoming connection on @p fd.
///
/// Submits an io_uring accept request and suspends the calling fiber until a
/// client connects — the OS thread is never blocked.  Must be called from a
/// fiber running on a fiberexec worker thread.
///
/// @p addr and @p addrlen may be nullptr if the peer address is not needed.
/// If @p timeout is set and no connection arrives in time, throws
/// std::system_error(ECANCELED).
///
/// @returns The accepted file descriptor (caller must close it).
/// @throws std::system_error on I/O failure, timeout, or cancellation.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
int async_accept(int fd,
                 sockaddr* addr,
                 socklen_t* addrlen,
                 std::optional<std::chrono::nanoseconds> timeout = std::nullopt);

/// Asynchronously open a file relative to @p dirfd.
///
/// Submits an io_uring openat request and suspends the calling fiber until a
/// file descriptor is ready — the OS thread is never blocked.  Must be called
/// from a fiber running on a fiberexec worker thread.
///
/// @p dirfd may be AT_FDCWD to open relative to the current directory.
/// @p flags and @p mode are passed directly to the underlying openat(2).
/// If @p timeout is set and the open does not complete in time, throws
/// std::system_error(ECANCELED).
///
/// @returns The opened file descriptor (caller is responsible for closing it).
/// @throws std::system_error on I/O failure, timeout, or cancellation.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
int async_openat(int dirfd,
                 char const* path,
                 int flags,
                 mode_t mode = 0,
                 std::optional<std::chrono::nanoseconds> timeout = std::nullopt);

/// Asynchronously close @p fd.
///
/// Submits an io_uring close request and suspends the calling fiber until the
/// close completes — the OS thread is never blocked.  The file descriptor is
/// consumed by this call; do not use @p fd after calling async_close.  Must be
/// called from a fiber running on a fiberexec worker thread.
///
/// @throws std::system_error on I/O failure.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
void async_close(int fd);

/// Initiate a connection from @p fd to the address described by @p addr.
///
/// Submits an io_uring connect request and suspends the calling fiber until
/// the connection is established — the OS thread is never blocked.  Must be
/// called from a fiber running on a fiberexec worker thread.
///
/// If @p timeout is set and the connection is not established in time, throws
/// std::system_error(ECANCELED).
///
/// @throws std::system_error on I/O failure, timeout, or cancellation.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
void async_connect(fd_ref fd,
                   sockaddr const* addr,
                   socklen_t addrlen,
                   std::optional<std::chrono::nanoseconds> timeout = std::nullopt);

/// Receive bytes from socket @p fd into @p buf.
///
/// Submits an io_uring recv request and suspends the calling fiber until data
/// arrives — the OS thread is never blocked.  Must be called from a fiber
/// running on a fiberexec worker thread.
///
/// @p flags is passed directly to the underlying recv (e.g. MSG_WAITALL).
/// If @p timeout is set and no data arrives in time, throws
/// std::system_error(ECANCELED).
///
/// @returns Number of bytes received.
/// @throws std::system_error on I/O failure, timeout, or cancellation.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
ssize_t async_recv(fd_ref fd,
                   std::span<std::byte> buf,
                   int flags = 0,
                   std::optional<std::chrono::nanoseconds> timeout = std::nullopt);

/// Send @p buf to socket @p fd.
///
/// Submits an io_uring send request and suspends the calling fiber until the
/// kernel has accepted the data — the OS thread is never blocked.  Must be
/// called from a fiber running on a fiberexec worker thread.
///
/// @p flags is passed directly to the underlying send (e.g. MSG_NOSIGNAL).
/// If @p timeout is set and the send does not complete in time, throws
/// std::system_error(ECANCELED).
///
/// @returns Number of bytes sent.
/// @throws std::system_error on I/O failure, timeout, or cancellation.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
ssize_t async_send(fd_ref fd,
                   std::span<std::byte const> buf,
                   int flags = 0,
                   std::optional<std::chrono::nanoseconds> timeout = std::nullopt);

/// Submit send + recv on @p fd as a linked pair in a single io_uring_submit
/// call.  The kernel starts the recv immediately after the send completes
/// without waiting for userspace to re-enter.  The calling fiber suspends
/// once and resumes when both CQEs have arrived.
///
/// @p send_flags and @p recv_flags are passed through to the underlying ops.
///
/// @returns {bytes_sent, bytes_received}.
/// @throws std::system_error if either op fails (send error checked first).
/// @throws std::runtime_error if called outside of a fiberexec fiber.
std::pair<ssize_t, ssize_t> async_send_recv(fd_ref fd,
                                            std::span<std::byte const> send_buf,
                                            std::span<std::byte> recv_buf,
                                            int send_flags = 0,
                                            int recv_flags = 0);

/// Submit write + fsync on @p fd as a linked pair in a single io_uring_submit
/// call.  The kernel starts the fsync immediately after the write completes
/// without waiting for userspace to re-enter.  The calling fiber suspends
/// once and resumes when both CQEs have arrived.
///
/// @returns Number of bytes written.
/// @throws std::system_error if either op fails (write error checked first).
/// @throws std::runtime_error if called outside of a fiberexec fiber.
ssize_t async_write_fsync(fd_ref fd, std::span<std::byte const> buf);

/// Suspend the calling fiber for at least @p duration.
///
/// Submits an io_uring timeout and suspends the calling fiber until it fires —
/// the OS thread is never blocked.  Must be called from a fiber running on a
/// fiberexec worker thread.
///
/// Cancellation is automatic via the fiber-local stop token.
///
/// @throws std::system_error on unexpected io_uring error or cancellation.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
void async_sleep_for(std::chrono::nanoseconds duration);

} // namespace fiberexec
