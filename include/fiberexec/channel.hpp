#pragma once

#include <boost/fiber/buffered_channel.hpp>

#include <cstddef>
#include <cstdint>
#include <utility>

namespace fiberexec {

/// Status returned by channel push and pop operations.
enum class channel_op_status : std::uint8_t {
    success, ///< Operation completed successfully.
    empty,   ///< try_pop found the channel empty (non-blocking only).
    full,    ///< try_push found the channel full (non-blocking only).
    closed,  ///< Channel is closed; no further items will be produced.
    timeout, ///< Timed operation expired before completing (timed overloads only).
};

/// Bounded MPMC channel for fibers.
///
/// A thin wrapper around boost::fibers::buffered_channel<T> that keeps
/// Boost.Fiber out of the fiberexec public API surface.  Blocking push/pop
/// operations suspend the calling fiber (not the OS thread), so the pool
/// thread remains free to run other fibers while a producer or consumer waits.
///
/// Both ends may be called from fibers running on different OS threads.
/// Calling push or pop from outside a fiber context is legal but will block
/// the calling OS thread rather than yield — the same caveat that applies to
/// all Boost.Fiber synchronization primitives.
///
/// @tparam T  Value type.  Must be movable.
template <class T> class channel {
public:
    /// Construct a channel with the given bounded @p capacity (must be > 0).
    explicit channel(std::size_t capacity)
        : ch_(capacity) {}

    ~channel() = default;
    channel(channel const&) = delete;
    channel& operator=(channel const&) = delete;
    channel(channel&&) = delete;
    channel& operator=(channel&&) = delete;

    /// Blocking push.  Suspends the fiber if the channel is full.
    /// @returns success, or closed if the channel was closed.
    [[nodiscard]] channel_op_status push(T const& v) { return cvt(ch_.push(v)); }
    [[nodiscard]] channel_op_status push(T&& v) { return cvt(ch_.push(std::move(v))); }

    /// Non-blocking push.
    /// @returns success, full (channel is at capacity), or closed.
    [[nodiscard]] channel_op_status try_push(T const& v) { return cvt(ch_.try_push(v)); }
    [[nodiscard]] channel_op_status try_push(T&& v) { return cvt(ch_.try_push(std::move(v))); }

    /// Blocking pop.  Suspends the fiber if the channel is empty.
    /// @returns success (value written to @p v), or closed (channel empty and closed).
    [[nodiscard]] channel_op_status pop(T& v) { return cvt(ch_.pop(v)); }

    /// Blocking pop that returns the value directly.
    /// @throws std::system_error if the channel is closed and empty.
    [[nodiscard]] T value_pop() { return ch_.value_pop(); }

    /// Non-blocking pop.
    /// @returns success (value written to @p v), empty, or closed.
    [[nodiscard]] channel_op_status try_pop(T& v) { return cvt(ch_.try_pop(v)); }

    /// Close the channel.  Subsequent pushes return closed; pops drain any
    /// remaining items then return closed.
    void close() noexcept { ch_.close(); }

    [[nodiscard]] bool is_closed() noexcept { return ch_.is_closed(); }

private:
    static channel_op_status cvt(boost::fibers::channel_op_status s) noexcept {
        switch (s) {
        case boost::fibers::channel_op_status::success:
            return channel_op_status::success;
        case boost::fibers::channel_op_status::empty:
            return channel_op_status::empty;
        case boost::fibers::channel_op_status::full:
            return channel_op_status::full;
        case boost::fibers::channel_op_status::closed:
            return channel_op_status::closed;
        case boost::fibers::channel_op_status::timeout:
            return channel_op_status::timeout;
        }
        return channel_op_status::closed; // unreachable
    }

    boost::fibers::buffered_channel<T> ch_;
};

} // namespace fiberexec
