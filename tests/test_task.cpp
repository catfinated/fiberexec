#include <fiberexec/task.hpp>

#include <catch2/catch_test_macros.hpp>

#include <array>

TEST_CASE("default-constructed task is empty", "[task]") {
    fiberexec::task t;
    REQUIRE(!t);
}

TEST_CASE("small callable uses SBO path and is invocable", "[task]") {
    bool ran = false;
    fiberexec::task t{[&ran] { ran = true; }};
    REQUIRE(t);
    t();
    REQUIRE(ran);
}

TEST_CASE("large callable uses heap path and is invocable", "[task]") {
    // Array by value exceeds the 64-byte SBO buffer, forcing heap allocation.
    std::array<char, 80> data{};
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    data[0] = 'x';
    bool ran = false;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    fiberexec::task t{[data, &ran] { ran = (data[0] == 'x'); }};
    REQUIRE(t);
    t();
    REQUIRE(ran);
}

TEST_CASE("move constructor transfers ownership and empties source", "[task]") {
    bool ran = false;
    fiberexec::task a{[&ran] { ran = true; }};
    fiberexec::task b{std::move(a)};
    REQUIRE(!a); // NOLINT(bugprone-use-after-move)
    REQUIRE(b);
    b();
    REQUIRE(ran);
}

TEST_CASE("move assignment transfers ownership and empties source", "[task]") {
    bool ran = false;
    fiberexec::task a{[&ran] { ran = true; }};
    fiberexec::task b;
    b = std::move(a);
    REQUIRE(!a); // NOLINT(bugprone-use-after-move)
    REQUIRE(b);
    b();
    REQUIRE(ran);
}

TEST_CASE("move of heap-allocated task transfers ownership", "[task]") {
    std::array<char, 80> data{};
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    data[0] = 'y';
    bool ran = false;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    fiberexec::task a{[data, &ran] { ran = (data[0] == 'y'); }};
    fiberexec::task b{std::move(a)};
    REQUIRE(!a); // NOLINT(bugprone-use-after-move)
    b();
    REQUIRE(ran);
}

TEST_CASE("callable destructor runs exactly once on task destruction", "[task]") {
    int destroy_count = 0;
    struct Tracker {
        int* count;
        explicit Tracker(int* c)
            : count{c} {}
        Tracker(Tracker const&) = delete;
        Tracker(Tracker&& o) noexcept
            : count{o.count} {}
        Tracker& operator=(Tracker const&) = delete;
        Tracker& operator=(Tracker&&) = delete;
        ~Tracker() { ++(*count); }
        void operator()() const {}
    };

    {
        fiberexec::task t{Tracker{&destroy_count}};
        // The temporary Tracker is moved into task; the temporary is then
        // destroyed (count = 1).  The live Tracker is inside the task.
        REQUIRE(destroy_count == 1);
    } // task destroyed here — the live Tracker is destroyed (count = 2)
    REQUIRE(destroy_count == 2);
}

TEST_CASE("move does not double-destroy the callable", "[task]") {
    // Track net live instances: +1 on any construction, -1 on destruction.
    // At any point count must be non-negative; zero at the end means no leaks
    // and no double-destructions.
    int count = 0;
    struct Counter {
        int* n;
        explicit Counter(int* n)
            : n{n} {
            ++(*n);
        }
        Counter(Counter&& o) noexcept
            : n{o.n} {
            ++(*n);
        }
        Counter(Counter const&) = delete;
        Counter& operator=(Counter const&) = delete;
        Counter& operator=(Counter&&) = delete;
        ~Counter() { --(*n); }
        void operator()() const {}
    };

    {
        fiberexec::task a{Counter{&count}};
        // Temp Counter created (+1), moved into task (+1), temp destroyed (-1) → 1 live.
        REQUIRE(count == 1);
        {
            fiberexec::task b{std::move(a)};
            // Counter moved into b (+1), moved-from Counter in a's buf destroyed (-1) → 1 live.
            REQUIRE(count == 1);
        } // b destroyed → Counter in b's buf destroyed (-1) → 0 live.
        REQUIRE(count == 0);
    } // a is empty — no change.
    REQUIRE(count == 0);
}
