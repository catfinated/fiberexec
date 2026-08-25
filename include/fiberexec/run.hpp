#pragma once

#include <fiberexec/context.hpp>
#include <fiberexec/detail/fiber_ops.hpp>

#include <boost/context/detail/exception.hpp>
#include <stdexec/execution.hpp>

#include <cerrno>
#include <exception>
#include <optional>
#include <stop_token>
#include <system_error>
#include <type_traits>
#include <utility>

namespace fiberexec {

namespace detail {

// std::conditional_t instantiates both branches, so set_value_t(void) would
// be formed even when result_t is non-void. Use a specializable trait instead.
template <class ResultT> struct run_completion_signatures {
    using type = stdexec::completion_signatures<stdexec::set_value_t(ResultT),
                                                stdexec::set_error_t(std::exception_ptr),
                                                stdexec::set_stopped_t()>;
};

template <> struct run_completion_signatures<void> {
    using type = stdexec::completion_signatures<stdexec::set_value_t(),
                                                stdexec::set_error_t(std::exception_ptr),
                                                stdexec::set_stopped_t()>;
};

template <class Fn> struct run_sender {
    using sender_concept = stdexec::sender_tag;
    using result_t = std::invoke_result_t<Fn>;

    // set_stopped_t() is always advertised: ECANCELED thrown by async ops is
    // mapped to set_stopped rather than set_error (see ADR-0001 step 4).
    using completion_signatures = run_completion_signatures<result_t>::type;

    struct env {
        context* ctx;

        template <class Tag>
        [[nodiscard]] auto query(stdexec::get_completion_scheduler_t<Tag> /*tag*/) const noexcept -> scheduler {
            return ctx->get_scheduler();
        }
    };

    [[nodiscard]] env get_env() const noexcept { return {ctx_}; }

    template <stdexec::receiver Receiver> struct operation {
        using operation_state_concept = stdexec::operation_state_tag;
        using stop_token_t = stdexec::stop_token_of_t<stdexec::env_of_t<Receiver>>;

        struct stop_forwarder {
            std::stop_source* src{nullptr};
            void operator()() const noexcept { src->request_stop(); }
        };
        using stop_cb_t = stdexec::stop_callback_for_t<stop_token_t, stop_forwarder>;

        Receiver rcvr;
        context* ctx{nullptr};
        Fn fn;
        std::stop_source fiber_stop_{std::nostopstate};
        std::optional<stop_cb_t> stop_cb_;

        void start() noexcept {
            auto env_tok = stdexec::get_stop_token(stdexec::get_env(rcvr));
            if (env_tok.stop_possible()) {
                fiber_stop_ = std::stop_source{};
                stop_cb_.emplace(env_tok, stop_forwarder{&fiber_stop_});
            }
            schedule_task(ctx->pool(), [this, tok = fiber_stop_.get_token()]() mutable {
                install_fiber_stop_token(std::move(tok));
                try {
                    if constexpr (std::is_void_v<result_t>) {
                        std::move(fn)();
                        stdexec::set_value(std::move(rcvr));
                    } else {
                        // Evaluate fn() before moving rcvr so that a throw
                        // from fn() leaves rcvr valid for the catch blocks.
                        auto val = std::move(fn)();
                        stdexec::set_value(std::move(rcvr), std::move(val));
                    }
                } catch (boost::context::detail::forced_unwind const&) {
                    stdexec::set_stopped(std::move(rcvr));
                    throw; // Boost.Fiber stack-unwinding; must propagate after completing
                } catch (std::system_error const& e) {
                    if (e.code().value() == ECANCELED) {
                        stdexec::set_stopped(std::move(rcvr));
                    } else {
                        stdexec::set_error(std::move(rcvr), std::current_exception());
                    }
                } catch (...) {
                    stdexec::set_error(std::move(rcvr), std::current_exception());
                }
            });
        }
    };

    template <stdexec::receiver Receiver> [[nodiscard]] auto connect(Receiver rcvr) && -> operation<Receiver> {
        return {std::move(rcvr), ctx_, std::move(fn_), std::stop_source{std::nostopstate}, std::nullopt};
    }

    template <stdexec::receiver Receiver> [[nodiscard]] auto connect(Receiver rcvr) const& -> operation<Receiver> {
        return {std::move(rcvr), ctx_, fn_, std::stop_source{std::nostopstate}, std::nullopt};
    }

    context* ctx_;
    Fn fn_;
};

// ---------------------------------------------------------------------------
// Pipe form: schedule(sched) | run(fn)
// ---------------------------------------------------------------------------

// run_closure is the sender adaptor closure returned by the one-arg run(fn).
// Piping a schedule_sender into it rebuilds the two-arg run_sender from the
// upstream sender's context, so the pipe form *is* the direct form: one fiber,
// one stop-token bridge, and a single definition of the completion mapping.
//
// Only schedule_sender is accepted.  fn must run on a fiber for async_read,
// async_write, etc. to work, which is only true downstream of a fiberexec
// schedule; piping run(fn) after an arbitrary sender would silently run fn
// off-fiber, so it is a compile error instead.
template <class Fn> struct run_closure : stdexec::sender_adaptor_closure<run_closure<Fn>> {
    [[nodiscard]] auto operator()(scheduler::schedule_sender sndr) && -> run_sender<Fn> {
        return {sndr.ctx, std::move(fn_)};
    }

    [[nodiscard]] auto operator()(scheduler::schedule_sender sndr) const& -> run_sender<Fn> { return {sndr.ctx, fn_}; }

    Fn fn_;
};

} // namespace detail

/// Schedule @p fn on the fiber pool and run it on a new fiber, mapping a
/// cancelled I/O operation (ECANCELED) to `set_stopped` rather than
/// `set_error`.
///
/// This is the canonical fiber entry point (ADR-0001 Option B). It combines:
///
///   1. **Schedule** — transitions onto the fiber pool, identical to
///      `stdexec::schedule(sched)`.
///   2. **Stop-token bridge** — installs the receiver's stop token as the
///      fiber-local stop token so `async_read`, `async_write`, etc. are
///      automatically cancellable without explicit token threading.
///   3. **Cancellation mapping** — if @p fn propagates a
///      `std::system_error(ECANCELED)`, `run` calls `set_stopped` on the
///      receiver instead of `set_error`, completing the stdexec cancellation
///      signal loop.
///
/// @tparam Fn  Callable with signature `R()`. May return void.
/// @param sched  Scheduler whose pool the fiber should run on.
/// @param fn     Callable to invoke on the fiber.
///
/// @returns A sender that completes with:
///   - `set_value(fn())` — @p fn returned normally.
///   - `set_stopped()` — @p fn propagated `std::system_error(ECANCELED)`.
///   - `set_error(exception_ptr)` — @p fn propagated any other exception.
template <class Fn> [[nodiscard]] auto run(scheduler sched, Fn&& fn) {
    return detail::run_sender<std::decay_t<Fn>>{&sched.get_context(), std::forward<Fn>(fn)};
}

/// Returns a sender adaptor closure (SAC) for the pipe form of `run`.
///
/// Usage: `stdexec::schedule(sched) | fiberexec::run(fn)`
///
/// This is identical to `fiberexec::run(sched, fn)` by construction: the
/// closure takes the context out of the upstream `schedule_sender` and returns
/// the very same sender the two-arg form returns. The upstream must be a
/// `fiberexec` schedule sender; piping after any other sender is a compile
/// error, since @p fn would not be running on a fiber.
///
/// @tparam Fn  Callable with signature `R()`. May return void.
/// @param fn   Callable to invoke on the fiber.
///
/// @returns A closure that, when piped after `stdexec::schedule(sched)`,
///   produces exactly `fiberexec::run(sched, fn)`.
template <class Fn> [[nodiscard]] auto run(Fn&& fn) {
    return detail::run_closure<std::decay_t<Fn>>{{}, std::forward<Fn>(fn)};
}

} // namespace fiberexec
