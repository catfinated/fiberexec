#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

#include <catch2/catch_test_macros.hpp>

#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <stdexcept>
#include <string_view>
#include <system_error>

TEST_CASE("run executes a void callable and sends set_value", "[run]") {
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();
    bool ran = false;

    stdexec::sync_wait(fiberexec::run(sched, [&] { ran = true; }));

    REQUIRE(ran);
}

TEST_CASE("run returns callable result via set_value", "[run]") {
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    auto result = stdexec::sync_wait(fiberexec::run(sched, [] { return 42; }));

    REQUIRE(result.has_value());
    auto [val] = *result;
    REQUIRE(val == 42);
}

TEST_CASE("run propagates non-ECANCELED exception as set_error", "[run]") {
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();
    bool threw = false;

    try {
        stdexec::sync_wait(fiberexec::run(sched, [] { throw std::runtime_error("oops"); }));
    } catch (std::runtime_error const& e) {
        threw = (std::string_view{e.what()} == "oops");
    }

    REQUIRE(threw);
}

TEST_CASE("run maps ECANCELED system_error to set_stopped", "[run]") {
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    // set_stopped causes sync_wait to return nullopt.
    auto result =
        stdexec::sync_wait(fiberexec::run(sched, [] { throw std::system_error{ECANCELED, std::system_category()}; }));

    REQUIRE(!result.has_value());
}

TEST_CASE("run maps async_read cancellation to set_stopped", "[run][cancellation]") {
    // when_all errors in the trigger branch, which causes it to send a stop
    // request into the reader branch. async_read wakes with ECANCELED; run
    // maps that to set_stopped. when_all then propagates the trigger's
    // set_error as the overall result.
    using namespace std::chrono_literals;
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    std::array<int, 2> pipefd{};
    REQUIRE(::pipe(pipefd.data()) == 0);
    auto [read_fd, write_fd] = pipefd;

    bool auto_cancelled = false;

    try {
        stdexec::sync_wait(stdexec::when_all(fiberexec::run(sched,
                                                            [&] {
                                                                try {
                                                                    std::array<char, 4> buf{};
                                                                    fiberexec::async_read(read_fd, buf.data(),
                                                                                          buf.size());
                                                                } catch (std::system_error const& e) {
                                                                    auto_cancelled = (e.code().value() == ECANCELED);
                                                                    throw; // let run map ECANCELED to set_stopped
                                                                }
                                                            }),
                                             stdexec::schedule(sched) | stdexec::then([&] {
                                                 fiberexec::async_sleep_for(10ms);
                                                 throw std::runtime_error("trigger");
                                             })));
    } catch (...) {
        // trigger's error propagates from when_all via sync_wait; expected
    }

    ::close(read_fd);
    ::close(write_fd);

    REQUIRE(auto_cancelled);
}

// ---------------------------------------------------------------------------
// Pipe form: schedule(sched) | run(fn)
// ---------------------------------------------------------------------------

TEST_CASE("pipe run executes a void callable and sends set_value", "[run][pipe]") {
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();
    bool ran = false;

    stdexec::sync_wait(stdexec::schedule(sched) | fiberexec::run([&] { ran = true; }));

    REQUIRE(ran);
}

TEST_CASE("pipe run returns callable result via set_value", "[run][pipe]") {
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    auto result = stdexec::sync_wait(stdexec::schedule(sched) | fiberexec::run([] { return 42; }));

    REQUIRE(result.has_value());
    auto [val] = *result;
    REQUIRE(val == 42);
}

TEST_CASE("pipe run maps ECANCELED system_error to set_stopped", "[run][pipe]") {
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    auto result = stdexec::sync_wait(
        stdexec::schedule(sched) | fiberexec::run([] { throw std::system_error{ECANCELED, std::system_category()}; }));

    REQUIRE(!result.has_value());
}

TEST_CASE("pipe run propagates non-ECANCELED exception as set_error", "[run][pipe]") {
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();
    bool threw = false;

    try {
        stdexec::sync_wait(stdexec::schedule(sched) | fiberexec::run([] { throw std::runtime_error("oops"); }));
    } catch (std::runtime_error const& e) {
        threw = (std::string_view{e.what()} == "oops");
    }

    REQUIRE(threw);
}

TEST_CASE("pipe run maps async_read cancellation to set_stopped", "[run][pipe][cancellation]") {
    using namespace std::chrono_literals;
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    std::array<int, 2> pipefd{};
    REQUIRE(::pipe(pipefd.data()) == 0);
    auto [read_fd, write_fd] = pipefd;

    bool auto_cancelled = false;

    try {
        stdexec::sync_wait(stdexec::when_all(stdexec::schedule(sched) | fiberexec::run([&] {
                                                 try {
                                                     std::array<char, 4> buf{};
                                                     fiberexec::async_read(read_fd, buf.data(), buf.size());
                                                 } catch (std::system_error const& e) {
                                                     auto_cancelled = (e.code().value() == ECANCELED);
                                                     throw;
                                                 }
                                             }),
                                             stdexec::schedule(sched) | stdexec::then([&] {
                                                 fiberexec::async_sleep_for(10ms);
                                                 throw std::runtime_error("trigger");
                                             })));
    } catch (...) {
        // trigger's error propagates from when_all; expected
    }

    ::close(read_fd);
    ::close(write_fd);

    REQUIRE(auto_cancelled);
}
