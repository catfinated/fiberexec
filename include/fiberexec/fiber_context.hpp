#pragma once

#include <stdexec/execution.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <thread>

namespace fiberexec {

class fiber_pool;

namespace detail {

/// Route a callable onto the fiber pool without exposing Boost headers.
///
/// This is an internal bridge used by
/// `scheduler::schedule_sender::operation::start()`. It is declared in the
/// public header only because `start()` must be inline (it is a template);
/// nothing outside of fiberexec should call it directly.
///
/// @param pool The pool to post work onto.
/// @param work The callable to run inside a new fiber.
void schedule_task(fiber_pool& pool, std::function<void()> work) noexcept;

} // namespace detail

/// Execution context that owns a Boost.Fiber work-stealing thread pool backed
/// by io_uring.
///
/// Each OS thread in the pool runs its own Boost.Fiber scheduler and an
/// independent `io_uring` instance. When a fiber issues I/O it suspends
/// cooperatively; the per-thread event loop reaps completions and resumes
/// the fiber — no OS thread ever blocks.
///
/// **Lifetime:** create exactly one `fiber_context` and keep it alive for as
/// long as work is being scheduled through it.
///
/// - Non-copyable and non-movable — it owns OS threads and kernel resources.
/// - Obtain a lightweight scheduler handle via `get_scheduler()`.
///
/// @see fiber_scheduler
class fiber_context {
public:
    /// Construct the context and start @p thread_count worker threads.
    ///
    /// Defaults to `std::thread::hardware_concurrency()` when omitted.
    ///
    /// @param thread_count Number of OS threads to spin up.
    explicit fiber_context(std::uint32_t thread_count = std::thread::hardware_concurrency());

    /// Drain all in-flight fibers and join every worker thread.
    ~fiber_context();

    fiber_context(fiber_context const&) = delete;
    fiber_context& operator=(fiber_context const&) = delete;
    fiber_context(fiber_context&&) = delete;
    fiber_context& operator=(fiber_context&&) = delete;

    /// Access the underlying pool (used internally by `schedule_task()`).
    fiber_pool& pool() noexcept { return *pool_; }

    // =========================================================================
    /// @name stdexec scheduler interface
    // =========================================================================
    /// @{

    /// Lightweight, copyable handle that satisfies `stdexec::scheduler`.
    ///
    /// Obtain one from `fiber_context::get_scheduler()`. Calling `schedule()`
    /// on this handle returns a sender that, when started, transitions
    /// execution onto the fiber pool and calls `set_value()` on the connected
    /// receiver from inside a new fiber.
    ///
    /// @note Two `scheduler` values compare equal iff they refer to the same
    ///       `fiber_context`.
    /// @see fiber_context::get_scheduler()
    class scheduler {
    public:
        using scheduler_concept = stdexec::scheduler_t; ///<  Opt-in tag required by stdexec.

        /// Environment attached to senders produced by this scheduler.
        ///
        /// Answers `get_completion_scheduler` queries so that stdexec
        /// algorithms can inspect which context a sender will complete on.
        struct env {
            fiber_context* ctx; ///<  Back-pointer to the owning context.

            /// Return the completion scheduler for any completion tag @p Tag.
            template <class Tag> auto query(stdexec::get_completion_scheduler_t<Tag> /*tag*/) const noexcept -> scheduler {
                return scheduler{ctx};
            }
        };

        /// Sender returned by `scheduler::schedule()`.
        ///
        /// Completes with `set_value()` (no arguments) once the associated
        /// fiber has been posted to the pool and begins executing.
        struct schedule_sender {
            using sender_concept = stdexec::sender_t;
            using completion_signatures = stdexec::completion_signatures<stdexec::set_value_t()>;

            fiber_context* ctx; ///<  Non-owning pointer to the execution context.

            /// Return the environment for this sender.
            [[nodiscard]] env get_env() const noexcept { return {ctx}; }

            /// Operation state that drives a single `schedule()` send.
            template <class Receiver> struct operation {
                Receiver rcvr;      ///<  Connected receiver.
                fiber_context* ctx; ///<  Execution context to post onto.

                /// Post a fiber that calls `set_value` on the receiver.
                void start() noexcept {
                    detail::schedule_task(ctx->pool(),
                                          [r = std::move(rcvr)]() mutable { stdexec::set_value(std::move(r)); });
                }
            };

            /// Connect this sender to @p rcvr and return the operation state.
            template <stdexec::receiver Receiver> auto connect(Receiver rcvr) const noexcept -> operation<Receiver> {
                return {std::move(rcvr), ctx};
            }
        };

        /// Return a sender that transitions onto the fiber pool when started.
        [[nodiscard]] schedule_sender schedule() const noexcept { return {ctx_}; }

        /// Two schedulers are equal iff they refer to the same `fiber_context`.
        bool operator==(scheduler const&) const noexcept = default;

    private:
        friend class fiber_context;
        explicit scheduler(fiber_context* ctx) noexcept
            : ctx_(ctx) {}

        fiber_context* ctx_; ///<  Non-owning pointer to the parent context.
    };

    /// Return a scheduler handle bound to this context.
    [[nodiscard]] scheduler get_scheduler() noexcept { return scheduler{this}; }

    /// @}

private:
    std::unique_ptr<fiber_pool> pool_; ///<  Owned thread pool and io_uring instances.
};

/// Convenience alias — `fiber_scheduler` is the scheduler type most callers
/// interact with.
using fiber_scheduler = fiber_context::scheduler;

static_assert(stdexec::scheduler<fiber_scheduler>);

} // namespace fiberexec
