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

// run_pipe_sender wraps an upstream sender. When the upstream sends
// set_value, it installs the fiber-local stop token and runs Fn, mapping
// ECANCELED -> set_stopped. set_error and set_stopped from upstream are
// forwarded unchanged.
template <class Upstream, class Fn> struct run_pipe_sender {
    using sender_concept = stdexec::sender_tag;
    using result_t = std::invoke_result_t<Fn>;
    using completion_signatures = run_completion_signatures<result_t>::type;

    template <stdexec::receiver Receiver> struct operation {
        using operation_state_concept = stdexec::operation_state_tag;
        using stop_token_t = stdexec::stop_token_of_t<stdexec::env_of_t<Receiver>>;

        struct stop_forwarder {
            std::stop_source* src{nullptr};
            void operator()() const noexcept { src->request_stop(); }
        };
        using stop_cb_t = stdexec::stop_callback_for_t<stop_token_t, stop_forwarder>;

        // inner_receiver is what the upstream (schedule) delivers set_value to.
        // It holds a pointer back to the parent operation, which is safe because
        // the operation is pinned after connect() returns (mandatory copy elision).
        struct inner_receiver {
            using receiver_concept = stdexec::receiver_tag;
            operation* op{nullptr};

            [[nodiscard]] stdexec::env_of_t<Receiver> get_env() const noexcept { return stdexec::get_env(op->rcvr); }

            // Accept (and discard) any upstream value args — schedule sends none,
            // but this lets run(fn) compose after other senders too.
            template <class... Args> void set_value(Args&&... /*args*/) noexcept {
                install_fiber_stop_token(op->fiber_stop_.get_token());
                try {
                    if constexpr (std::is_void_v<result_t>) {
                        std::move(op->fn)();
                        stdexec::set_value(std::move(op->rcvr));
                    } else {
                        auto val = std::move(op->fn)();
                        stdexec::set_value(std::move(op->rcvr), std::move(val));
                    }
                } catch (boost::context::detail::forced_unwind const&) {
                    stdexec::set_stopped(std::move(op->rcvr));
                    throw; // Boost.Fiber stack-unwinding; must propagate after completing
                } catch (std::system_error const& e) {
                    if (e.code().value() == ECANCELED) {
                        stdexec::set_stopped(std::move(op->rcvr));
                    } else {
                        stdexec::set_error(std::move(op->rcvr), std::current_exception());
                    }
                } catch (...) {
                    stdexec::set_error(std::move(op->rcvr), std::current_exception());
                }
            }

            template <class E> void set_error(E&& e) noexcept {
                stdexec::set_error(std::move(op->rcvr), std::forward<E>(e));
            }

            void set_stopped() noexcept { stdexec::set_stopped(std::move(op->rcvr)); }
        };

        // upstream_op_ must be declared last: its constructor calls
        // stdexec::connect(upstream, inner_receiver{this}), so rcvr, fn,
        // fiber_stop_, and stop_cb_ must already be initialized at that point.
        Receiver rcvr;
        Fn fn;
        std::stop_source fiber_stop_{std::nostopstate};
        std::optional<stop_cb_t> stop_cb_;
        stdexec::connect_result_t<Upstream, inner_receiver> upstream_op_;

        explicit operation(Upstream upstream, Receiver rcvr_, Fn fn_)
            : rcvr(std::move(rcvr_))
            , fn(std::move(fn_))
            , upstream_op_(stdexec::connect(std::move(upstream), inner_receiver{this})) {}

        void start() noexcept {
            auto env_tok = stdexec::get_stop_token(stdexec::get_env(rcvr));
            if (env_tok.stop_possible()) {
                fiber_stop_ = std::stop_source{};
                stop_cb_.emplace(env_tok, stop_forwarder{&fiber_stop_});
            }
            stdexec::start(upstream_op_);
        }
    };

    template <stdexec::receiver Receiver> [[nodiscard]] auto connect(Receiver rcvr) && -> operation<Receiver> {
        return operation<Receiver>{std::move(upstream_), std::move(rcvr), std::move(fn_)};
    }

    template <stdexec::receiver Receiver> [[nodiscard]] auto connect(Receiver rcvr) const& -> operation<Receiver> {
        return operation<Receiver>{upstream_, std::move(rcvr), fn_};
    }

    Upstream upstream_;
    Fn fn_;
};

// run_closure is the SAC returned by the one-arg run(fn). operator| connects
// it to an upstream sender to produce a run_pipe_sender.
template <class Fn> struct run_closure {
    template <stdexec::sender Upstream>
    friend auto operator|(Upstream&& upstream, run_closure&& closure) -> run_pipe_sender<std::decay_t<Upstream>, Fn> {
        return {std::forward<Upstream>(upstream), std::move(closure).fn_};
    }

    template <stdexec::sender Upstream>
    friend auto operator|(Upstream&& upstream, run_closure const& closure)
        -> run_pipe_sender<std::decay_t<Upstream>, Fn> {
        return {std::forward<Upstream>(upstream), closure.fn_};
    }

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
/// This has identical semantics to `fiberexec::run(sched, fn)`: the stop-token
/// bridge is installed before @p fn runs, and ECANCELED is mapped to
/// `set_stopped`. The scheduler is provided by the upstream sender rather than
/// as a direct argument.
///
/// @tparam Fn  Callable with signature `R()`. May return void.
/// @param fn   Callable to invoke on the fiber.
///
/// @returns A closure that, when piped after a sender, produces a sender with
///   the same completion signatures as `fiberexec::run(sched, fn)`.
template <class Fn> [[nodiscard]] auto run(Fn&& fn) {
    return detail::run_closure<std::decay_t<Fn>>{std::forward<Fn>(fn)};
}

} // namespace fiberexec
