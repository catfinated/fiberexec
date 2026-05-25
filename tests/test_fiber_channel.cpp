#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <numeric>
#include <vector>

TEST_CASE("push and pop exchange a value", "[fiber_channel]") {
    fiberexec::fiber_context ctx{2};
    auto sched = ctx.get_scheduler();
    fiberexec::fiber_channel<int> ch{4};

    stdexec::sync_wait(
        stdexec::when_all(fiberexec::run(sched, [&] { REQUIRE(ch.push(42) == fiberexec::channel_op_status::success); }),
                          fiberexec::run(sched, [&] {
                              int v{};
                              REQUIRE(ch.pop(v) == fiberexec::channel_op_status::success);
                              REQUIRE(v == 42);
                          })));
}

TEST_CASE("value_pop returns the value directly", "[fiber_channel]") {
    fiberexec::fiber_context ctx{2};
    auto sched = ctx.get_scheduler();
    fiberexec::fiber_channel<int> ch{2}; // capacity 2 → stores 1 item (ring buffer uses one slot as sentinel)

    stdexec::sync_wait(
        stdexec::when_all(fiberexec::run(sched, [&] { REQUIRE(ch.push(7) == fiberexec::channel_op_status::success); }),
                          fiberexec::run(sched, [&] { REQUIRE(ch.value_pop() == 7); })));
}

TEST_CASE("try_push returns full when channel is at capacity", "[fiber_channel]") {
    fiberexec::fiber_context ctx{2};
    auto sched = ctx.get_scheduler();
    fiberexec::fiber_channel<int> ch{4}; // capacity 4 → stores 3 items

    stdexec::sync_wait(fiberexec::run(sched, [&] {
        REQUIRE(ch.try_push(1) == fiberexec::channel_op_status::success);
        REQUIRE(ch.try_push(2) == fiberexec::channel_op_status::success);
        REQUIRE(ch.try_push(3) == fiberexec::channel_op_status::success);
        REQUIRE(ch.try_push(4) == fiberexec::channel_op_status::full);
    }));
}

TEST_CASE("try_pop returns empty on an empty channel", "[fiber_channel]") {
    fiberexec::fiber_context ctx{2};
    auto sched = ctx.get_scheduler();
    fiberexec::fiber_channel<int> ch{4};

    stdexec::sync_wait(fiberexec::run(sched, [&] {
        int v{};
        REQUIRE(ch.try_pop(v) == fiberexec::channel_op_status::empty);
    }));
}

TEST_CASE("push returns closed after channel is closed", "[fiber_channel]") {
    fiberexec::fiber_context ctx{2};
    auto sched = ctx.get_scheduler();
    fiberexec::fiber_channel<int> ch{4};

    stdexec::sync_wait(fiberexec::run(sched, [&] {
        ch.close();
        REQUIRE(ch.push(1) == fiberexec::channel_op_status::closed);
        REQUIRE(ch.try_push(1) == fiberexec::channel_op_status::closed);
    }));
}

// NOLINTBEGIN(readability-function-cognitive-complexity)
TEST_CASE("pop drains remaining items then returns closed", "[fiber_channel]") {
    fiberexec::fiber_context ctx{2};
    auto sched = ctx.get_scheduler();
    fiberexec::fiber_channel<int> ch{4};

    stdexec::sync_wait(fiberexec::run(sched, [&] {
        REQUIRE(ch.push(10) == fiberexec::channel_op_status::success);
        REQUIRE(ch.push(20) == fiberexec::channel_op_status::success);
        ch.close();

        int v{};
        REQUIRE(ch.pop(v) == fiberexec::channel_op_status::success);
        REQUIRE(v == 10);
        REQUIRE(ch.pop(v) == fiberexec::channel_op_status::success);
        REQUIRE(v == 20);
        REQUIRE(ch.pop(v) == fiberexec::channel_op_status::closed);
    }));
}

TEST_CASE("multiple producers and consumers exchange all values", "[fiber_channel]") {
    // 4 producers each push N items; 4 consumers drain the channel.
    // Verifies MPMC correctness: every pushed value is received exactly once.
    fiberexec::fiber_context ctx{4};
    auto sched = ctx.get_scheduler();

    constexpr int kProducers = 4;
    constexpr int kItemsEach = 64;
    constexpr int kTotal = kProducers * kItemsEach;

    fiberexec::fiber_channel<int> ch{16};
    std::atomic<int> received{0};
    std::atomic<int> sum{0};
    std::atomic<int> producers_done{0};

    // Expected sum: each producer i pushes values [i*kItemsEach, (i+1)*kItemsEach).
    int expected_sum = 0;
    for (int i = 0; i < kTotal; ++i) {
        expected_sum += i;
    }

    stdexec::sync_wait(stdexec::when_all(
        // Producers
        fiberexec::run(sched,
                       [&] {
                           for (int i = 0; i < kItemsEach; ++i) {
                               REQUIRE(ch.push((0 * kItemsEach) + i) == fiberexec::channel_op_status::success);
                           }
                           if (producers_done.fetch_add(1, std::memory_order_acq_rel) == kProducers - 1) {
                               ch.close();
                           }
                       }),
        fiberexec::run(sched,
                       [&] {
                           for (int i = 0; i < kItemsEach; ++i) {
                               REQUIRE(ch.push((1 * kItemsEach) + i) == fiberexec::channel_op_status::success);
                           }
                           if (producers_done.fetch_add(1, std::memory_order_acq_rel) == kProducers - 1) {
                               ch.close();
                           }
                       }),
        fiberexec::run(sched,
                       [&] {
                           for (int i = 0; i < kItemsEach; ++i) {
                               REQUIRE(ch.push((2 * kItemsEach) + i) == fiberexec::channel_op_status::success);
                           }
                           if (producers_done.fetch_add(1, std::memory_order_acq_rel) == kProducers - 1) {
                               ch.close();
                           }
                       }),
        fiberexec::run(sched,
                       [&] {
                           for (int i = 0; i < kItemsEach; ++i) {
                               REQUIRE(ch.push((3 * kItemsEach) + i) == fiberexec::channel_op_status::success);
                           }
                           if (producers_done.fetch_add(1, std::memory_order_acq_rel) == kProducers - 1) {
                               ch.close();
                           }
                       }),
        // Consumers
        fiberexec::run(sched,
                       [&] {
                           int v{};
                           while (ch.pop(v) == fiberexec::channel_op_status::success) {
                               sum.fetch_add(v, std::memory_order_relaxed);
                               received.fetch_add(1, std::memory_order_relaxed);
                           }
                       }),
        fiberexec::run(sched,
                       [&] {
                           int v{};
                           while (ch.pop(v) == fiberexec::channel_op_status::success) {
                               sum.fetch_add(v, std::memory_order_relaxed);
                               received.fetch_add(1, std::memory_order_relaxed);
                           }
                       }),
        fiberexec::run(sched,
                       [&] {
                           int v{};
                           while (ch.pop(v) == fiberexec::channel_op_status::success) {
                               sum.fetch_add(v, std::memory_order_relaxed);
                               received.fetch_add(1, std::memory_order_relaxed);
                           }
                       }),
        fiberexec::run(sched, [&] {
            int v{};
            while (ch.pop(v) == fiberexec::channel_op_status::success) {
                sum.fetch_add(v, std::memory_order_relaxed);
                received.fetch_add(1, std::memory_order_relaxed);
            }
        })));

    REQUIRE(received.load() == kTotal);
    REQUIRE(sum.load() == expected_sum);
}
// NOLINTEND(readability-function-cognitive-complexity)
