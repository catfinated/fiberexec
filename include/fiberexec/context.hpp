#pragma once

#include <fiberexec/detail/fiber_bulk.hpp>
#include <fiberexec/detail/fiber_ops.hpp>
#include <stdexec/execution.hpp>

#include <cstdint>
#include <memory>
#include <optional>
#include <stop_token>
#include <thread>

namespace fiberexec {

/// Execution context that owns a thread pool of Boost.Fiber workers backed
/// by io_uring.
///
/// Each OS thread in the pool runs a custom `io_uring`-aware Boost.Fiber
/// scheduling algorithm. When a fiber issues I/O it suspends cooperatively;
/// the per-thread event loop reaps completions and resumes the fiber — no OS
/// thread ever blocks.
///
/// Multiple independent `context` instances may coexist in the same
/// process. Each context owns its OS threads and io_uring rings and is
/// entirely self-contained.
///
/// - Non-copyable and non-movable — it owns OS threads and kernel resources.
/// - Obtain a lightweight scheduler handle via `get_scheduler()`.
///
/// @see scheduler
class context {
public:
    /// Default fiber stack size — matches `boost::context::stack_traits::default_size()` (128 KiB).
    static constexpr std::size_t default_stack_size = 128UL * 1024UL;

    /// Construct the context and start @p thread_count worker threads.
    ///
    /// Defaults to `std::thread::hardware_concurrency()` when omitted.
    ///
    /// @param thread_count Number of OS threads to spin up.
    /// @param stack_size   Stack size in bytes for each spawned fiber.
    ///                     Defaults to `default_stack_size` (128 KiB).
    explicit context(std::uint32_t thread_count = std::thread::hardware_concurrency(),
                     std::size_t stack_size = default_stack_size);

    /// Drain all in-flight fibers and join every worker thread.
    ~context();

    context(context const&) = delete;
    context& operator=(context const&) = delete;
    context(context&&) = delete;
    context& operator=(context&&) = delete;

    /// Access the underlying pool (used internally by `schedule_task()`).
    fiber_pool& pool() noexcept { return *pool_; }

    // =========================================================================
    /// @name stdexec scheduler interface
    // =========================================================================
    /// @{

    /// Lightweight, copyable handle that satisfies `stdexec::scheduler`.
    ///
    /// Obtain one from `context::get_scheduler()`. Calling `schedule()`
    /// on this handle returns a sender that, when started, transitions
    /// execution onto the fiber pool and calls `set_value()` on the connected
    /// receiver from inside a new fiber.
    ///
    /// @note Two `scheduler` values compare equal iff they refer to the same
    ///       `context`.
    /// @see context::get_scheduler()
    class scheduler {
    public:
        using scheduler_concept = stdexec::scheduler_tag; ///<  Opt-in tag required by stdexec.

        /// Environment attached to senders produced by this scheduler.
        ///
        /// Answers `get_completion_scheduler` queries so that stdexec
        /// algorithms can inspect which context a sender will complete on.
        struct env {
            context* ctx; ///<  Back-pointer to the owning context.

            /// Return the completion scheduler for any completion tag @p Tag.
            template <class Tag>
            [[nodiscard]] auto query(stdexec::get_completion_scheduler_t<Tag> /*tag*/) const noexcept -> scheduler {
                return scheduler{ctx};
            }
        };

        /// Sender returned by `scheduler::schedule()`.
        ///
        /// Completes with `set_value()` (no arguments) once the associated
        /// fiber has been posted to the pool and begins executing.
        struct schedule_sender {
            using sender_concept = stdexec::sender_tag;
            using completion_signatures = stdexec::completion_signatures<stdexec::set_value_t()>;

            context* ctx; ///<  Non-owning pointer to the execution context.

            /// Return the environment for this sender.
            [[nodiscard]] env get_env() const noexcept { return {ctx}; }

            /// Operation state that drives a single `schedule()` send.
            ///
            /// Bridges the receiver environment's stop token into a
            /// `std::stop_token` stored in fiber-local storage so that
            /// `async_read` / `async_write` / `async_sleep_for` are
            /// automatically cancellable without explicit token threading.
            template <class Receiver> struct operation {
                using operation_state_concept = stdexec::operation_state_tag;
                using stop_token_t = stdexec::stop_token_of_t<stdexec::env_of_t<Receiver>>;

                /// Forwards a stop request from the receiver's token to the
                /// fiber-local stop source so async ops can observe it.
                struct stop_forwarder {
                    std::stop_source* src{nullptr};
                    void operator()() const noexcept { src->request_stop(); }
                };
                using stop_cb_t = stdexec::stop_callback_for_t<stop_token_t, stop_forwarder>;

                Receiver rcvr;         ///<  Connected receiver.
                context* ctx{nullptr}; ///<  Execution context to post onto.
                /// Per-operation stop source. Starts with no shared state to
                /// avoid a heap allocation when cancellation is not in use;
                /// start() allocates a real shared state only if the receiver's
                /// token is stoppable.
                std::stop_source fiber_stop_{std::nostopstate};
                std::optional<stop_cb_t> stop_cb_; ///<  Bridges receiver token → fiber_stop_.

                /// Post a fiber that installs the bridged stop token and then
                /// calls set_value on the receiver.
                void start() noexcept {
                    auto env_tok = stdexec::get_stop_token(stdexec::get_env(rcvr));
                    if (env_tok.stop_possible()) {
                        fiber_stop_ = std::stop_source{};
                        stop_cb_.emplace(env_tok, stop_forwarder{&fiber_stop_});
                    }
                    detail::schedule_task(ctx->pool(), [this, tok = fiber_stop_.get_token()]() mutable {
                        detail::install_fiber_stop_token(std::move(tok));
                        stdexec::set_value(std::move(this->rcvr));
                    });
                }
            };

            /// Connect this sender to @p rcvr and return the operation state.
            template <stdexec::receiver Receiver>
            [[nodiscard]] auto connect(Receiver rcvr) const -> operation<Receiver> {
                return {std::move(rcvr), ctx, std::stop_source{std::nostopstate}, std::nullopt};
            }
        };

        /// Return a sender that transitions onto the fiber pool when started.
        [[nodiscard]] schedule_sender schedule() const noexcept { return {ctx_}; }

        /// Two schedulers are equal iff they refer to the same `context`.
        bool operator==(scheduler const&) const noexcept = default;

        /// Access the owning context (used by `fiberexec::run`).
        [[nodiscard]] context& get_context() const noexcept { return *ctx_; }

        /// Expose the fiber domain at the scheduler level so stdexec algorithms
        /// (e.g. sync_wait's domain consistency check) find it here.
        [[nodiscard]] static auto query(stdexec::get_completion_domain_t<stdexec::set_value_t> /*tag*/) noexcept
            -> detail::fiber_domain {
            return {};
        }

    private:
        friend class context;
        explicit scheduler(context* ctx) noexcept
            : ctx_(ctx) {}

        context* ctx_; ///<  Non-owning pointer to the parent context.
    };

    /// Return a scheduler handle bound to this context.
    [[nodiscard]] scheduler get_scheduler() noexcept { return scheduler{this}; }

    /// @}

private:
    std::unique_ptr<fiber_pool> pool_; ///<  Owned thread pool and io_uring instances.
};

/// Convenience alias — `scheduler` is the scheduler type most callers
/// interact with.
using scheduler = context::scheduler;

static_assert(stdexec::scheduler<scheduler>);

} // namespace fiberexec
