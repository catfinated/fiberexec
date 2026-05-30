#pragma once

#include <cstdint>

// Forward-declare liburing type so this header stays lightweight.
struct io_uring; // NOLINT(bugprone-reserved-identifier)

namespace fiberexec::detail {

// Abstract base for any object that can receive an io_uring CQE.
// A pointer to this type is stored in SQE user_data. drain_cqes() casts
// back and calls complete(), which handles both one-shot ops (io_awaitable)
// and persistent multi-CQE ops (accept_state, and future recv/poll variants).
struct cqe_handler {
    cqe_handler() = default;
    cqe_handler(cqe_handler const&) = delete;
    cqe_handler& operator=(cqe_handler const&) = delete;
    cqe_handler(cqe_handler&&) = delete;
    cqe_handler& operator=(cqe_handler&&) = delete;
    virtual ~cqe_handler() = default;
    virtual void complete(io_uring* ring, int res, uint32_t flags) noexcept = 0;
};

} // namespace fiberexec::detail
