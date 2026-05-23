#pragma once

#include <fiberexec/detail/fiber_ops.hpp>

#include <stdexec/execution.hpp>

#include <boost/fiber/future.hpp>

#include <exception>
#include <optional>
#include <stdexcept>
#include <system_error>
#include <tuple>
#include <type_traits>

namespace fiberexec {

namespace detail {

// Receiver that fulfills a boost::fibers::promise on any completion signal.
// The fiber waiting on the corresponding future is suspended (not blocked)
// until the promise is set — whether from the same thread or another.
template <class ValueTuple> struct sync_wait_receiver {
    using receiver_concept = stdexec::receiver_tag;

    boost::fibers::promise<std::optional<ValueTuple>>* promise_; // non-owning

    template <class... Args> void set_value(Args&&... args) noexcept {
        try {
            promise_->set_value(std::make_optional<ValueTuple>(std::forward<Args>(args)...));
        } catch (...) {
            promise_->set_exception(std::current_exception());
        }
    }

    template <class Error> void set_error(Error&& e) noexcept {
        if constexpr (std::same_as<std::decay_t<Error>, std::exception_ptr>) {
            promise_->set_exception(std::forward<Error>(e));
        } else if constexpr (std::same_as<std::decay_t<Error>, std::error_code>) {
            promise_->set_exception(std::make_exception_ptr(std::system_error(std::forward<Error>(e))));
        } else {
            promise_->set_exception(std::make_exception_ptr(std::forward<Error>(e)));
        }
    }

    void set_stopped() noexcept { promise_->set_value(std::nullopt); }

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
/// Boost.Fiber's cross-thread promise/future mechanism.
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

    boost::fibers::promise<std::optional<value_tuple_t>> promise;
    auto future = promise.get_future();

    // op and the receiver inside it hold a pointer to promise. All three live
    // on this fiber's stack; the stack is preserved while the fiber is
    // suspended, so the pointer remains valid until future.get() returns.
    auto op = stdexec::connect(std::forward<Sender>(sender), detail::sync_wait_receiver<value_tuple_t>{&promise});
    stdexec::start(op);

    return future.get(); // suspends this fiber; OS thread runs other fibers
}

} // namespace fiberexec
