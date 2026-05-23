#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

#include <catch2/catch_test_macros.hpp>

#include <unistd.h>

#include <array>
#include <atomic>
#include <string_view>
#include <thread>

// All tests share one pool so the Boost.Fiber work-stealing scheduler's
// global thread ID counter stays within the pool's registered slot range.
namespace {
// NOLINTNEXTLINE(cert-err58-cpp,cppcoreguidelines-avoid-non-const-global-variables)
fiberexec::fiber_context g_ctx{2};
} // namespace

TEST_CASE("fiber_scheduler satisfies stdexec::scheduler concept", "[scheduler]") {
    STATIC_REQUIRE(stdexec::scheduler<fiberexec::fiber_scheduler>);
}

TEST_CASE("scheduled work executes", "[scheduler]") {
    auto sched = g_ctx.get_scheduler();

    std::atomic<bool> ran{false};
    stdexec::sync_wait(stdexec::schedule(sched) | stdexec::then([&ran] { ran.store(true); }));

    REQUIRE(ran.load());
}

TEST_CASE("work executes on a worker thread, not the caller", "[scheduler]") {
    auto sched = g_ctx.get_scheduler();

    std::thread::id work_thread{};
    stdexec::sync_wait(stdexec::schedule(sched) |
                       stdexec::then([&work_thread] { work_thread = std::this_thread::get_id(); }));

    REQUIRE(work_thread != std::thread::id{});
    REQUIRE(work_thread != std::this_thread::get_id());
}

TEST_CASE("multiple sequential dispatches all complete", "[scheduler]") {
    auto sched = g_ctx.get_scheduler();

    std::atomic<int> count{0};
    for (int i = 0; i < 8; ++i) {
        stdexec::sync_wait(stdexec::schedule(sched) | stdexec::then([&count] { count.fetch_add(1); }));
    }

    REQUIRE(count.load() == 8);
}

TEST_CASE("concurrent dispatch via when_all completes all tasks", "[scheduler]") {
    auto sched = g_ctx.get_scheduler();

    std::atomic<int> count{0};
    auto inc = [&count] { count.fetch_add(1); };

    stdexec::sync_wait(stdexec::when_all(stdexec::schedule(sched) | stdexec::then(inc),
                                         stdexec::schedule(sched) | stdexec::then(inc),
                                         stdexec::schedule(sched) | stdexec::then(inc)));

    REQUIRE(count.load() == 3);
}

TEST_CASE("async_read and async_write suspend and resume fibers via io_uring", "[io]") {
    std::array<int, 2> pipefd{};
    REQUIRE(::pipe(pipefd.data()) == 0);
    auto [read_fd, write_fd] = pipefd;

    auto sched = g_ctx.get_scheduler();

    constexpr std::string_view kMsg = "ping";
    std::array<char, 4> buf{};

    // Dispatch writer and reader concurrently. The reader suspends until the
    // writer's data reaches the pipe buffer, exercising the CQE routing path.
    stdexec::sync_wait(stdexec::when_all(
        stdexec::schedule(sched) | stdexec::then([&] { fiberexec::async_write(write_fd, kMsg.data(), kMsg.size()); }),
        stdexec::schedule(sched) | stdexec::then([&] { fiberexec::async_read(read_fd, buf.data(), buf.size()); })));

    ::close(read_fd);
    ::close(write_fd);

    REQUIRE(std::string_view(buf.data(), buf.size()) == kMsg);
}
