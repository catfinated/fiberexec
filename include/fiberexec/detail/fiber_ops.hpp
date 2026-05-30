#pragma once

#include <fiberexec/task.hpp>

#include <chrono>
#include <stop_token>

// Forward-declare liburing types so includers don't need to pull in <liburing.h>.
struct io_uring;     // NOLINT(bugprone-reserved-identifier)
struct io_uring_sqe; // NOLINT(bugprone-reserved-identifier)

namespace fiberexec {

class fiber_pool;

namespace detail {

/// Route a callable onto the fiber pool without exposing Boost headers.
///
/// Internal bridge used by `schedule_sender::operation::start()`. Declared
/// here (rather than in a .cpp) because `start()` is an inline template.
/// Nothing outside of fiberexec should call this directly.
void schedule_task(fiber_pool& pool, task work) noexcept;

/// Return the io_uring ring owned by the current worker thread, or nullptr if
/// the calling thread is not a fiberexec worker.
[[nodiscard]] io_uring* current_ring() noexcept;

/// Submit @p sqe to the current thread's ring and suspend the calling fiber
/// until the completion event arrives.  Returns the CQE result (negative errno
/// on I/O failure).  Must be called from a fiber running on a fiberexec worker.
///
/// Cancellation is automatic: the fiber-local stop token (installed by
/// schedule_sender or fiberexec::run) and the pool-wide stop token are both
/// observed.  If either fires while the fiber is suspended, an
/// IORING_OP_ASYNC_CANCEL is submitted and the return value will be -ECANCELED.
int submit_and_wait(io_uring_sqe* sqe);

/// Like submit_and_wait, but attaches a linked timeout to @p sqe via
/// IORING_OP_LINK_TIMEOUT.  If @p timeout elapses before the op completes,
/// the kernel cancels the op and returns -ECANCELED.  Both SQEs are submitted
/// in a single io_uring_submit call.
int submit_and_wait_with_timeout(io_uring_sqe* sqe, std::chrono::nanoseconds timeout);

/// Install @p tok as the stop token for the currently running fiber.
/// Called by operation::start() before invoking set_value on the receiver.
void install_fiber_stop_token(std::stop_token tok);

/// Return the stop token installed for the current fiber, or an empty
/// (non-stoppable) token if none was installed.
[[nodiscard]] std::stop_token current_fiber_stop_token();

/// Submit an IORING_OP_ASYNC_CANCEL for the SQE whose user_data matches
/// @p handler, tagged so drain_cqes() silently discards the cancel CQE.
/// No-op if the calling thread has no ring (e.g. called from outside a worker).
void submit_cancel(void* handler) noexcept;

} // namespace detail

} // namespace fiberexec
