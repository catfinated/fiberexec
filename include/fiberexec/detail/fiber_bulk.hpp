#pragma once

#include <fiberexec/detail/fiber_ops.hpp>
#include <stdexec/__detail/__sender_introspection.hpp>
#include <stdexec/execution.hpp>

#include <atomic>
#include <exception>

namespace fiberexec::detail {

// ---------------------------------------------------------------------------
// Parallel bulk sender for the fiber pool
//
// When stdexec::bulk(stdexec::schedule(fiber_sched), stdexec::par, N, fn) is
// called, stdexec lowers bulk → bulk_chunked and then asks the domain to
// transform the sender.  fiber_domain::transform_sender intercepts that and
// returns a fiber_bulk_sender whose operation state fans out N fibers via
// schedule_task — one per index — so work is actually distributed across pool
// threads rather than run sequentially.
// ---------------------------------------------------------------------------

// Shared state between N fibers running in parallel.  Lives inside
// fiber_bulk_opstate; fibers hold raw pointers to it.  Safe because:
//   - The opstate is alive until set_value/set_error is called downstream.
//   - set_value/set_error is called by the *last* fiber to finish via the
//     atomic counter, so all fibers are done before the opstate is destroyed.
template <class Shape, class Fun, class Receiver> struct fiber_bulk_shared {
    Fun fun;
    Receiver rcvr;
    std::atomic<Shape> remaining;
    std::exception_ptr error{};
    std::atomic<bool> has_error{false};

    fiber_bulk_shared(Shape n, Fun f, Receiver r)
        : fun(std::move(f))
        , rcvr(std::move(r))
        , remaining(n) {}
};

// Receiver connected to the predecessor sender (schedule).  When the
// predecessor calls set_value(), we post N fibers to the pool.
template <class Shape, class Fun, class Receiver> struct fiber_bulk_receiver {
    using receiver_concept = stdexec::receiver_tag;
    using shared_t = fiber_bulk_shared<Shape, Fun, Receiver>;

    fiber_pool* pool;
    shared_t* shared;
    Shape shape;

    void set_value() && noexcept {
        shared_t* s = shared;
        Shape n = shape;

        if (n == 0) {
            stdexec::set_value(std::move(s->rcvr));
            return;
        }

        for (Shape i = 0; i < n; ++i) {
            schedule_task(*pool, [s, i] {
                try {
                    s->fun(i, static_cast<Shape>(i + 1));
                } catch (...) {
                    bool expected = false;
                    if (s->has_error.compare_exchange_strong(expected, true, std::memory_order_relaxed)) {
                        s->error = std::current_exception();
                    }
                }
                if (--s->remaining == 0) {
                    if (s->has_error.load(std::memory_order_acquire)) {
                        stdexec::set_error(std::move(s->rcvr), std::move(s->error));
                    } else {
                        stdexec::set_value(std::move(s->rcvr));
                    }
                }
            });
        }
    }

    template <class E> void set_error(E&& e) && noexcept {
        stdexec::set_error(std::move(shared->rcvr), std::forward<E>(e));
    }

    void set_stopped() && noexcept { stdexec::set_stopped(std::move(shared->rcvr)); }

    [[nodiscard]] auto get_env() const noexcept { return stdexec::get_env(shared->rcvr); }
};

// Operation state.  shared_ must be declared before inner_op_ so that
// &shared_ is valid when inner_op_ is constructed via connect().
template <class Shape, class Fun, class PredSender, class Receiver> struct fiber_bulk_opstate {
    using operation_state_concept = stdexec::operation_state_tag;

    using shared_t = fiber_bulk_shared<Shape, Fun, Receiver>;
    using bulk_rcvr = fiber_bulk_receiver<Shape, Fun, Receiver>;
    using inner_op_t = stdexec::connect_result_t<PredSender, bulk_rcvr>;

    shared_t shared_;
    inner_op_t inner_op_;

    fiber_bulk_opstate(fiber_pool* pool, PredSender pred, Shape shape, Fun fun, Receiver rcvr)
        : shared_(shape, std::move(fun), std::move(rcvr))
        , inner_op_(stdexec::connect(std::move(pred), bulk_rcvr{pool, &shared_, shape})) {}

    void start() & noexcept { stdexec::start(inner_op_); }
};

// The sender returned by fiber_domain::transform_sender.
template <class Shape, class Fun, class PredSender> struct fiber_bulk_sender {
    using sender_concept = stdexec::sender_tag;
    using completion_signatures =
        stdexec::completion_signatures<stdexec::set_value_t(), stdexec::set_error_t(std::exception_ptr)>;

    fiber_pool* pool;
    PredSender pred;
    Shape shape;
    Fun fun;

    template <stdexec::receiver Receiver> auto connect(Receiver rcvr) && {
        return fiber_bulk_opstate<Shape, Fun, PredSender, Receiver>{pool, std::move(pred), shape, std::move(fun),
                                                                    std::move(rcvr)};
    }
};

// Domain exposed via schedule_sender::env so stdexec::bulk picks it up.
// Inherits default_domain to pass all other algorithms through unchanged.
struct fiber_domain : stdexec::default_domain {
    template <class Sender, class Env>
        requires stdexec::__sender_for<Sender, stdexec::bulk_chunked_t>
    auto transform_sender(stdexec::set_value_t /*tag*/,
                          Sender&& sndr, // NOLINT(cppcoreguidelines-missing-std-forward)
                          Env const& /*env*/) const noexcept {
        auto& [tag, data, child] = sndr;
        auto [pol, shape, fun] = std::move(data);
        auto sched = stdexec::get_completion_scheduler<stdexec::set_value_t>(stdexec::get_env(child));
        using child_t = std::decay_t<decltype(child)>;
        using shape_t = std::decay_t<decltype(shape)>;
        using fun_t = std::decay_t<decltype(fun)>;
        return fiber_bulk_sender<shape_t, fun_t, child_t>{&sched.get_context().pool(), std::move(child), shape,
                                                          std::move(fun)};
    }
};

} // namespace fiberexec::detail
