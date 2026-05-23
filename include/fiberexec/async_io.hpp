#pragma once

#include <chrono>
#include <cstddef>
#include <sys/types.h>

namespace fiberexec {

/// Asynchronously read up to @p len bytes from @p fd into @p buf.
///
/// Submits an io_uring read request and suspends the calling fiber until the
/// completion event arrives — the OS thread is never blocked.  Must be called
/// from a fiber running on a fiberexec worker thread.
///
/// @returns Number of bytes read.
/// @throws std::system_error on I/O failure.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
ssize_t async_read(int fd, void* buf, std::size_t len);

/// Asynchronously write @p len bytes from @p buf to @p fd.
///
/// Submits an io_uring write request and suspends the calling fiber until the
/// completion event arrives — the OS thread is never blocked.  Must be called
/// from a fiber running on a fiberexec worker thread.
///
/// @returns Number of bytes written.
/// @throws std::system_error on I/O failure.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
ssize_t async_write(int fd, void const* buf, std::size_t len);

/// Suspend the calling fiber for at least @p duration.
///
/// Submits an io_uring timeout and suspends the calling fiber until it fires —
/// the OS thread is never blocked.  Must be called from a fiber running on a
/// fiberexec worker thread.
///
/// @throws std::system_error on unexpected io_uring error.
/// @throws std::runtime_error if called outside of a fiberexec fiber.
void async_sleep_for(std::chrono::nanoseconds duration);

} // namespace fiberexec
