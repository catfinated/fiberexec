#pragma once

#include <chrono>
#include <cstddef>
#include <stop_token>
#include <sys/socket.h>
#include <sys/types.h>

namespace fiberexec {

/// Asynchronously read up to @p len bytes from @p fd into @p buf.
///
/// Submits an io_uring read request and suspends the calling fiber until the
/// completion event arrives — the OS thread is never blocked.  Must be called
/// from a fiber running on a fiberexec worker thread.
///
/// If @p st is cancellable and stop is requested while the fiber is suspended,
/// the operation is cancelled and throws std::system_error(ECANCELED).
///
/// @returns Number of bytes read.
/// @throws std::system_error on I/O failure or cancellation.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
ssize_t async_read(int fd, void* buf, std::size_t len, std::stop_token st = {});

/// Asynchronously write @p len bytes from @p buf to @p fd.
///
/// Submits an io_uring write request and suspends the calling fiber until the
/// completion event arrives — the OS thread is never blocked.  Must be called
/// from a fiber running on a fiberexec worker thread.
///
/// If @p st is cancellable and stop is requested while the fiber is suspended,
/// the operation is cancelled and throws std::system_error(ECANCELED).
///
/// @returns Number of bytes written.
/// @throws std::system_error on I/O failure or cancellation.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
ssize_t async_write(int fd, void const* buf, std::size_t len, std::stop_token st = {});

/// Accept an incoming connection on @p fd.
///
/// Submits an io_uring accept request and suspends the calling fiber until a
/// client connects — the OS thread is never blocked.  Must be called from a
/// fiber running on a fiberexec worker thread.
///
/// @p addr and @p addrlen may be nullptr if the peer address is not needed.
/// If @p st is cancellable and stop is requested, throws std::system_error(ECANCELED).
///
/// @returns The accepted file descriptor (caller must close it).
/// @throws std::system_error on I/O failure or cancellation.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
int async_accept(int fd, sockaddr* addr, socklen_t* addrlen, std::stop_token st = {});

/// Initiate a connection from @p fd to the address described by @p addr.
///
/// Submits an io_uring connect request and suspends the calling fiber until
/// the connection is established — the OS thread is never blocked.  Must be
/// called from a fiber running on a fiberexec worker thread.
///
/// If @p st is cancellable and stop is requested, throws std::system_error(ECANCELED).
///
/// @throws std::system_error on I/O failure or cancellation.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
void async_connect(int fd, sockaddr const* addr, socklen_t addrlen, std::stop_token st = {});

/// Receive up to @p len bytes from socket @p fd into @p buf.
///
/// Submits an io_uring recv request and suspends the calling fiber until data
/// arrives — the OS thread is never blocked.  Must be called from a fiber
/// running on a fiberexec worker thread.
///
/// If @p st is cancellable and stop is requested, throws std::system_error(ECANCELED).
///
/// @returns Number of bytes received.
/// @throws std::system_error on I/O failure or cancellation.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
ssize_t async_recv(int fd, void* buf, std::size_t len, std::stop_token st = {});

/// Send @p len bytes from @p buf to socket @p fd.
///
/// Submits an io_uring send request and suspends the calling fiber until the
/// kernel has accepted the data — the OS thread is never blocked.  Must be
/// called from a fiber running on a fiberexec worker thread.
///
/// If @p st is cancellable and stop is requested, throws std::system_error(ECANCELED).
///
/// @returns Number of bytes sent.
/// @throws std::system_error on I/O failure or cancellation.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
ssize_t async_send(int fd, void const* buf, std::size_t len, std::stop_token st = {});

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
