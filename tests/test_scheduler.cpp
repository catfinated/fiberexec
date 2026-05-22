#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <thread>

TEST_CASE("fiber_scheduler satisfies stdexec::scheduler concept", "[scheduler]") {
    STATIC_REQUIRE(stdexec::scheduler<fiberexec::fiber_scheduler>);
}

TEST_CASE("scheduled work executes", "[scheduler]") {
    fiberexec::fiber_context ctx{2};
    auto sched = ctx.get_scheduler();

    std::atomic<bool> ran{false};
    stdexec::sync_wait(stdexec::schedule(sched) | stdexec::then([&ran] { ran.store(true); }));

    REQUIRE(ran.load());
}

TEST_CASE("work executes on a worker thread, not the caller", "[scheduler]") {
    fiberexec::fiber_context ctx{2};
    auto sched = ctx.get_scheduler();

    std::thread::id work_thread{};
    stdexec::sync_wait(stdexec::schedule(sched) |
                       stdexec::then([&work_thread] { work_thread = std::this_thread::get_id(); }));

    REQUIRE(work_thread != std::thread::id{});
    REQUIRE(work_thread != std::this_thread::get_id());
}

TEST_CASE("multiple sequential dispatches all complete", "[scheduler]") {
    fiberexec::fiber_context ctx{2};
    auto sched = ctx.get_scheduler();

    std::atomic<int> count{0};
    for (int i = 0; i < 8; ++i) {
        stdexec::sync_wait(stdexec::schedule(sched) | stdexec::then([&count] { count.fetch_add(1); }));
    }

    REQUIRE(count.load() == 8);
}

TEST_CASE("concurrent dispatch via when_all completes all tasks", "[scheduler]") {
    fiberexec::fiber_context ctx{2};
    auto sched = ctx.get_scheduler();

    std::atomic<int> count{0};
    auto inc = [&count] { count.fetch_add(1); };

    stdexec::sync_wait(stdexec::when_all(stdexec::schedule(sched) | stdexec::then(inc),
                                         stdexec::schedule(sched) | stdexec::then(inc),
                                         stdexec::schedule(sched) | stdexec::then(inc)));

    REQUIRE(count.load() == 3);
}
