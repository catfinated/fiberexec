#pragma once

#include <chrono>
#include <cstddef>
#include <optional>
#include <span>
#include <stop_token>
#include <sys/socket.h>
#include <sys/types.h>

namespace fiberexec {

/// Asynchronously read bytes from @p fd into @p buf.
///
/// Submits an io_uring read request and suspends the calling fiber until the
/// completion event arrives — the OS thread is never blocked.  Must be called
/// from a fiber running on a fiberexec worker thread.
///
/// If @p timeout is set and the read does not complete in time, the operation
/// is cancelled and throws std::system_error(ECANCELED).  If @p st is
/// cancellable and stop is requested while suspended, also throws ECANCELED.
///
/// @returns Number of bytes read.
/// @throws std::system_error on I/O failure, timeout, or cancellation.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
ssize_t async_read(int fd,
                   std::span<std::byte> buf,
                   std::optional<std::chrono::nanoseconds> timeout = std::nullopt,
                   std::stop_token st = {});

/// Asynchronously write @p buf to @p fd.
///
/// Submits an io_uring write request and suspends the calling fiber until the
/// completion event arrives — the OS thread is never blocked.  Must be called
/// from a fiber running on a fiberexec worker thread.
///
/// If @p timeout is set and the write does not complete in time, throws
/// std::system_error(ECANCELED).  If @p st is cancellable and stop is
/// requested while suspended, also throws ECANCELED.
///
/// @returns Number of bytes written.
/// @throws std::system_error on I/O failure, timeout, or cancellation.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
ssize_t async_write(int fd,
                    std::span<std::byte const> buf,
                    std::optional<std::chrono::nanoseconds> timeout = std::nullopt,
                    std::stop_token st = {});

/// Accept an incoming connection on @p fd.
///
/// Submits an io_uring accept request and suspends the calling fiber until a
/// client connects — the OS thread is never blocked.  Must be called from a
/// fiber running on a fiberexec worker thread.
///
/// @p addr and @p addrlen may be nullptr if the peer address is not needed.
/// If @p timeout is set and no connection arrives in time, throws
/// std::system_error(ECANCELED).  If @p st is cancellable and stop is
/// requested, also throws ECANCELED.
///
/// @returns The accepted file descriptor (caller must close it).
/// @throws std::system_error on I/O failure, timeout, or cancellation.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
int async_accept(int fd,
                 sockaddr* addr,
                 socklen_t* addrlen,
                 std::optional<std::chrono::nanoseconds> timeout = std::nullopt,
                 std::stop_token st = {});

/// Asynchronously open a file relative to @p dirfd.
///
/// Submits an io_uring openat request and suspends the calling fiber until a
/// file descriptor is ready — the OS thread is never blocked.  Must be called
/// from a fiber running on a fiberexec worker thread.
///
/// @p dirfd may be AT_FDCWD to open relative to the current directory.
/// @p flags and @p mode are passed directly to the underlying openat(2).
/// If @p timeout is set and the open does not complete in time, throws
/// std::system_error(ECANCELED).  If @p st is cancellable and stop is
/// requested, also throws ECANCELED.
///
/// @returns The opened file descriptor (caller is responsible for closing it).
/// @throws std::system_error on I/O failure, timeout, or cancellation.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
int async_openat(int dirfd,
                 char const* path,
                 int flags,
                 mode_t mode = 0,
                 std::optional<std::chrono::nanoseconds> timeout = std::nullopt,
                 std::stop_token st = {});

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
/// std::system_error(ECANCELED).  If @p st is cancellable and stop is
/// requested, also throws ECANCELED.
///
/// @throws std::system_error on I/O failure, timeout, or cancellation.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
void async_connect(int fd,
                   sockaddr const* addr,
                   socklen_t addrlen,
                   std::optional<std::chrono::nanoseconds> timeout = std::nullopt,
                   std::stop_token st = {});

/// Receive bytes from socket @p fd into @p buf.
///
/// Submits an io_uring recv request and suspends the calling fiber until data
/// arrives — the OS thread is never blocked.  Must be called from a fiber
/// running on a fiberexec worker thread.
///
/// @p flags is passed directly to the underlying recv (e.g. MSG_WAITALL).
/// If @p timeout is set and no data arrives in time, throws
/// std::system_error(ECANCELED).  If @p st is cancellable and stop is
/// requested, also throws ECANCELED.
///
/// @returns Number of bytes received.
/// @throws std::system_error on I/O failure, timeout, or cancellation.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
ssize_t async_recv(int fd,
                   std::span<std::byte> buf,
                   int flags = 0,
                   std::optional<std::chrono::nanoseconds> timeout = std::nullopt,
                   std::stop_token st = {});

/// Send @p buf to socket @p fd.
///
/// Submits an io_uring send request and suspends the calling fiber until the
/// kernel has accepted the data — the OS thread is never blocked.  Must be
/// called from a fiber running on a fiberexec worker thread.
///
/// @p flags is passed directly to the underlying send (e.g. MSG_NOSIGNAL).
/// If @p timeout is set and the send does not complete in time, throws
/// std::system_error(ECANCELED).  If @p st is cancellable and stop is
/// requested, also throws ECANCELED.
///
/// @returns Number of bytes sent.
/// @throws std::system_error on I/O failure, timeout, or cancellation.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
ssize_t async_send(int fd,
                   std::span<std::byte const> buf,
                   int flags = 0,
                   std::optional<std::chrono::nanoseconds> timeout = std::nullopt,
                   std::stop_token st = {});

/// Suspend the calling fiber for at least @p duration.
///
/// Submits an io_uring timeout and suspends the calling fiber until it fires —
/// the OS thread is never blocked.  Must be called from a fiber running on a
/// fiberexec worker thread.
///
/// If @p st is cancellable and stop is requested while the fiber is suspended,
/// the sleep is cancelled and throws std::system_error(ECANCELED).
///
/// @throws std::system_error on unexpected io_uring error or cancellation.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
void async_sleep_for(std::chrono::nanoseconds duration, std::stop_token st = {});

} // namespace fiberexec
