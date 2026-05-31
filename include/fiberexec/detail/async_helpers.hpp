#pragma once

#include <fiberexec/detail/fiber_ops.hpp>

#include <liburing.h>

#include <cerrno>
#include <chrono>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>

struct io_uring_sqe; // NOLINT(bugprone-reserved-identifier)

namespace fiberexec::detail {

// Validate fiber context and check for pre-cancellation via the fiber-local
// stop token.  Returns the current thread's ring so the caller can acquire an
// SQE at the right point.
inline io_uring* begin_async_op(char const* caller) {
    io_uring* ring = current_ring();
    if (ring == nullptr) {
        throw std::runtime_error(std::string(caller) + " called outside of a fiberexec fiber");
    }
    auto st = current_fiber_stop_token();
    if (st.stop_requested()) {
        throw std::system_error(ECANCELED, std::system_category(), caller);
    }
    return ring;
}

// Dispatch to submit_and_wait or submit_and_wait_with_timeout depending on
// whether a timeout was provided.
inline int dispatch(io_uring_sqe* sqe, std::optional<std::chrono::nanoseconds> const& timeout) {
    if (timeout) {
        return submit_and_wait_with_timeout(sqe, *timeout);
    }
    return submit_and_wait(sqe);
}

} // namespace fiberexec::detail
