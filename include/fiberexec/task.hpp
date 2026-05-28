#pragma once

#include <array>
#include <cstddef>
#include <new>
#include <type_traits>
#include <utility>

namespace fiberexec {

// Move-only type-erased callable for void() tasks.
//
// Closures up to 64 bytes that are nothrow-move-constructible are stored
// inline (SBO path) with no heap allocation.  Larger or potentially-throwing
// closures fall back to a heap allocation.  All practical fiber task lambdas
// (capturing a few pointers or references) use the SBO path.
//
// Preferred over std::function<void()> for two reasons: std::function requires
// copyability (forcing heap allocation for move-only captures regardless of
// size) and heap-allocates any closure larger than ~16 bytes.  bench_task
// measures 3-4x faster construct and move for a 24-byte closure, and identical
// cost for small (8-byte) ones.
class task {
public:
    task() = default;

    template <typename F>
        requires(!std::is_same_v<std::decay_t<F>, task> && std::is_invocable_r_v<void, std::decay_t<F>>)
    task(F&& f) { // NOLINT(hicpp-explicit-conversions)
        using Fn = std::decay_t<F>;
        if constexpr (sizeof(Fn) <= kBufSize && alignof(Fn) <= kBufAlign && std::is_nothrow_move_constructible_v<Fn>) {
            ::new (buf_.data()) Fn(std::forward<F>(f));
            vtable_ = &sbo_vtable_<Fn>;
        } else {
            ::new (buf_.data()) Fn*(new Fn(std::forward<F>(f))); // NOLINT(cppcoreguidelines-owning-memory)
            vtable_ = &heap_vtable_<Fn>;
        }
    }

    task(task&& other) noexcept
        : vtable_{other.vtable_} {
        if (vtable_ != nullptr) {
            vtable_->move(other.buf_.data(), buf_.data());
            other.vtable_ = nullptr;
        }
    }

    task& operator=(task&& other) noexcept {
        if (this != &other) {
            destroy();
            vtable_ = other.vtable_;
            if (vtable_ != nullptr) {
                vtable_->move(other.buf_.data(), buf_.data());
                other.vtable_ = nullptr;
            }
        }
        return *this;
    }

    task(task const&) = delete;
    task& operator=(task const&) = delete;

    ~task() { destroy(); }

    void operator()() { vtable_->invoke(buf_.data()); }

    explicit operator bool() const noexcept { return vtable_ != nullptr; }

private:
    static constexpr std::size_t kBufSize = 64;
    static constexpr std::size_t kBufAlign = alignof(std::max_align_t);

    struct vtable_t {
        void (*invoke)(void*);
        void (*move)(void* src, void* dst) noexcept;
        void (*destroy)(void* p) noexcept;
    };

    // SBO vtable: Fn lives directly in buf_.
    template <typename Fn>
    static constexpr vtable_t sbo_vtable_{
        .invoke = [](void* p) { (*std::launder(reinterpret_cast<Fn*>(p)))(); },
        .move =
            [](void* src, void* dst) noexcept {
                auto* fn = std::launder(reinterpret_cast<Fn*>(src));
                ::new (dst) Fn(std::move(*fn));
                fn->~Fn();
            },
        .destroy = [](void* p) noexcept { std::launder(reinterpret_cast<Fn*>(p))->~Fn(); },
    };

    // Heap vtable: buf_ holds a Fn* pointing to the heap allocation.
    template <typename Fn>
    static constexpr vtable_t heap_vtable_{
        .invoke = [](void* p) { (*(*std::launder(reinterpret_cast<Fn**>(p))))(); },
        .move = [](void* src, void* dst) noexcept { ::new (dst) Fn*(*std::launder(reinterpret_cast<Fn**>(src))); },
        .destroy =
            [](void* p) noexcept {
                delete *std::launder(reinterpret_cast<Fn**>(p)); // NOLINT(cppcoreguidelines-owning-memory)
            },
    };

    alignas(kBufAlign) std::array<std::byte, kBufSize> buf_{};
    vtable_t const* vtable_{nullptr};

    void destroy() noexcept {
        if (vtable_ != nullptr) {
            vtable_->destroy(buf_.data());
            vtable_ = nullptr;
        }
    }
};

} // namespace fiberexec
