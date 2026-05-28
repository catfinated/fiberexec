#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

#include <catch2/catch_test_macros.hpp>

#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <span>
#include <stdexcept>
#include <string_view>
#include <system_error>

TEST_CASE("scheduler satisfies stdexec::scheduler concept", "[scheduler]") {
    STATIC_REQUIRE(stdexec::scheduler<fiberexec::scheduler>);
}

TEST_CASE("scheduled work executes", "[scheduler]") {
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    std::atomic<bool> ran{false};
    stdexec::sync_wait(stdexec::schedule(sched) | stdexec::then([&ran] { ran.store(true); }));

    REQUIRE(ran.load());
}

TEST_CASE("work executes on a worker thread, not the caller", "[scheduler]") {
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    std::thread::id work_thread{};
    stdexec::sync_wait(stdexec::schedule(sched) |
                       stdexec::then([&work_thread] { work_thread = std::this_thread::get_id(); }));

    REQUIRE(work_thread != std::thread::id{});
    REQUIRE(work_thread != std::this_thread::get_id());
}

TEST_CASE("multiple sequential dispatches all complete", "[scheduler]") {
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    std::atomic<int> count{0};
    for (int i = 0; i < 8; ++i) {
        stdexec::sync_wait(stdexec::schedule(sched) | stdexec::then([&count] { count.fetch_add(1); }));
    }

    REQUIRE(count.load() == 8);
}

TEST_CASE("concurrent dispatch via when_all completes all tasks", "[scheduler]") {
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    std::atomic<int> count{0};
    auto inc = [&count] { count.fetch_add(1); };

    stdexec::sync_wait(stdexec::when_all(stdexec::schedule(sched) | stdexec::then(inc),
                                         stdexec::schedule(sched) | stdexec::then(inc),
                                         stdexec::schedule(sched) | stdexec::then(inc)));

    REQUIRE(count.load() == 3);
}

TEST_CASE("async_sleep_for suspends the fiber for at least the requested duration", "[timer]") {
    using namespace std::chrono_literals;
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    auto const before = std::chrono::steady_clock::now();
    stdexec::sync_wait(stdexec::schedule(sched) | stdexec::then([] { fiberexec::async_sleep_for(50ms); }));
    auto const elapsed = std::chrono::steady_clock::now() - before;

    REQUIRE(elapsed >= 50ms);
}

TEST_CASE("async_read with pre-cancelled fiber stop token throws immediately", "[cancellation]") {
    // Verify the pre-cancellation fast path in begin_async_op: if the fiber's
    // stop token is already requested, async_read throws ECANCELED without
    // submitting an SQE to io_uring.  We arrange for the stop to be requested
    // BEFORE async_read is called by letting the trigger branch throw first (a
    // 60 s sleep in the reader ensures the trigger always wins the race).
    using namespace std::chrono_literals;
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();
    bool threw = false;

    std::array<int, 2> pipefd{};
    REQUIRE(::pipe(pipefd.data()) == 0);
    auto [read_fd, write_fd] = pipefd;

    try {
        stdexec::sync_wait(
            stdexec::when_all(stdexec::schedule(sched) | stdexec::then([&] {
                                  // Long sleep ensures the trigger fires and cancels us first.
                                  try {
                                      fiberexec::async_sleep_for(60s);
                                  } catch (...) {
                                  }
                                  // Stop is now requested; next async op must throw immediately.
                                  try {
                                      std::array<char, 4> buf{};
                                      fiberexec::async_read(read_fd, std::as_writable_bytes(std::span{buf}));
                                  } catch (std::system_error const& e) {
                                      threw = (e.code().value() == ECANCELED);
                                  }
                              }),
                              stdexec::schedule(sched) | stdexec::then([] { throw std::runtime_error("trigger"); })));
    } catch (...) {
    }

    ::close(read_fd);
    ::close(write_fd);

    REQUIRE(threw);
}

TEST_CASE("async_read cancelled automatically via sender stop token", "[cancellation]") {
    // when_all cancels remaining branches when any branch errors. With ADR-0001
    // implemented, the stop token flows into the reader fiber automatically so
    // async_read (no explicit token) is cancelled without any user wiring.
    using namespace std::chrono_literals;
    std::array<int, 2> pipefd{};
    REQUIRE(::pipe(pipefd.data()) == 0);
    auto [read_fd, write_fd] = pipefd;

    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();
    bool auto_cancelled = false;

    try {
        stdexec::sync_wait(stdexec::when_all(
            // Reader: blocks on empty pipe, no explicit stop token.
            stdexec::schedule(sched) | stdexec::then([&] {
                try {
                    std::array<char, 4> buf{};
                    fiberexec::async_read(read_fd, std::as_writable_bytes(std::span{buf}));
                } catch (std::system_error const& e) {
                    auto_cancelled = (e.code().value() == ECANCELED);
                }
            }),
            // Trigger: short sleep then throw, which causes when_all to
            // request stop on the reader's receiver environment.
            stdexec::schedule(sched) | stdexec::then([&] {
                fiberexec::async_sleep_for(10ms);
                throw std::runtime_error("trigger cancel");
            })));
    } catch (...) {
        // when_all propagates the error from the trigger fiber; ignore it here.
    }

    ::close(read_fd);
    ::close(write_fd);

    REQUIRE(auto_cancelled);
}

TEST_CASE("async_read and async_write suspend and resume fibers via io_uring", "[io]") {
    std::array<int, 2> pipefd{};
    REQUIRE(::pipe(pipefd.data()) == 0);
    auto [read_fd, write_fd] = pipefd;

    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    constexpr std::string_view kMsg = "ping";
    std::array<char, 4> buf{};

    // Dispatch writer and reader concurrently. The reader suspends until the
    // writer's data reaches the pipe buffer, exercising the CQE routing path.
    stdexec::sync_wait(stdexec::when_all(
        stdexec::schedule(sched) | stdexec::then([&] {
            fiberexec::async_write(write_fd, std::as_bytes(std::span<char const>{kMsg.data(), kMsg.size()}));
        }),
        stdexec::schedule(sched) |
            stdexec::then([&] { fiberexec::async_read(read_fd, std::as_writable_bytes(std::span{buf})); })));

    ::close(read_fd);
    ::close(write_fd);

    REQUIRE(std::string_view(buf.data(), buf.size()) == kMsg);
}
