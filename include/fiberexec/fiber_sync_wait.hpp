#pragma once

#include <fiberexec/detail/fiber_ops.hpp>

#include <stdexec/execution.hpp>

#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/mutex.hpp>

#include <exception>
#include <memory>
#include <optional>
#include <stdexcept>
#include <system_error>
#include <tuple>
#include <type_traits>

namespace fiberexec {

namespace detail {

// Heap-allocated synchronization state shared between fiber_sync_wait (the
// waiter) and sync_wait_receiver (the completer). Using shared_ptr instead of
// a stack-allocated promise/future avoids a use-after-free: when the receiver's
// set_value wakes the outer fiber on another OS thread, that fiber can destroy
// the operation state (and the receiver inside it) while we are still executing
// inside notify_all(). The local shared_ptr copy in each completion method
// keeps the state alive through the notify_all() call.
template <class ValueTuple> struct fiber_sync_state {
    boost::fibers::mutex mtx;
    boost::fibers::condition_variable cv;
    std::optional<ValueTuple> value; // engaged on set_value, empty on set_stopped
    std::exception_ptr error;        // set on set_error
    bool done{false};
};

template <class ValueTuple> struct sync_wait_receiver {
    using receiver_concept = stdexec::receiver_tag;

    std::shared_ptr<fiber_sync_state<ValueTuple>> state_;

    template <class... Args> void set_value(Args&&... args) noexcept {
        auto s = state_; // local copy keeps state alive past notify_all
        {
            std::unique_lock<boost::fibers::mutex> lk{s->mtx};
            try {
                s->value = std::make_optional<ValueTuple>(std::forward<Args>(args)...);
            } catch (...) {
                s->error = std::current_exception();
            }
            s->done = true;
        }
        s->cv.notify_all();
    }

    template <class Error> void set_error(Error&& e) noexcept {
        auto s = state_;
        {
            std::unique_lock<boost::fibers::mutex> lk{s->mtx};
            if constexpr (std::same_as<std::decay_t<Error>, std::exception_ptr>) {
                s->error = std::forward<Error>(e);
            } else if constexpr (std::same_as<std::decay_t<Error>, std::error_code>) {
                s->error = std::make_exception_ptr(std::system_error(std::forward<Error>(e)));
            } else {
                s->error = std::make_exception_ptr(std::forward<Error>(e));
            }
            s->done = true;
        }
        s->cv.notify_all();
    }

    void set_stopped() noexcept {
        auto s = state_;
        {
            std::unique_lock<boost::fibers::mutex> lk{s->mtx};
            s->done = true; // value stays nullopt
        }
        s->cv.notify_all();
    }

    [[nodiscard]] stdexec::env<> get_env() const noexcept { return {}; }
};

} // namespace detail

/// Await @p sender from inside a fiber, suspending the fiber (not the OS
/// thread) until completion.
///
/// This is the fiber-pool equivalent of `stdexec::sync_wait`. Calling
/// `stdexec::sync_wait` from inside a fiberexec fiber would block the OS
/// thread, starving every other fiber sharing that thread. `fiber_sync_wait`
/// suspends only the calling fiber and yields the thread back to the scheduler
/// so other fibers can continue running.
///
/// The sender may complete on any thread; the calling fiber is woken via
/// Boost.Fiber's cross-thread mutex/condition_variable mechanism.
///
/// @returns `std::optional<std::tuple<value_types...>>`:
///   - An engaged tuple on `set_value`.
///   - `std::nullopt` on `set_stopped`.
///   - Rethrows into the calling fiber on `set_error`.
///
/// @throws std::runtime_error if called outside a fiberexec fiber.
/// @throws Whatever the sender delivers via `set_error`.
template <stdexec::sender Sender> auto fiber_sync_wait(Sender&& sender) {
    if (detail::current_ring() == nullptr) {
        throw std::runtime_error("fiber_sync_wait called outside of a fiberexec fiber");
    }

    using value_tuple_t = stdexec::value_types_of_t<Sender, stdexec::env<>, std::tuple, std::type_identity_t>;
    using state_t = detail::fiber_sync_state<value_tuple_t>;

    auto state = std::make_shared<state_t>();

    // op lives on the fiber's stack, which Boost.Fiber preserves while this
    // fiber is suspended in cv.wait below.
    auto op = stdexec::connect(std::forward<Sender>(sender), detail::sync_wait_receiver<value_tuple_t>{state});
    stdexec::start(op);

    {
        std::unique_lock<boost::fibers::mutex> lk{state->mtx};
        state->cv.wait(lk, [&] { return state->done; });
    } // mutex released here; op and state may now be safely destroyed

    if (state->error) {
        std::rethrow_exception(std::move(state->error));
    }
    return std::move(state->value);
}

} // namespace fiberexec
