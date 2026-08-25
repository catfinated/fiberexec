#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

#include <catch2/catch_test_macros.hpp>

#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <span>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>

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
        stdexec::sync_wait(stdexec::when_all(
            fiberexec::run(sched,
                           [&] {
                               try {
                                   std::array<char, 4> buf{};
                                   fiberexec::async_read(read_fd, std::as_writable_bytes(std::span{buf}));
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
                                                     fiberexec::async_read(read_fd,
                                                                           std::as_writable_bytes(std::span{buf}));
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

// ---------------------------------------------------------------------------
// Pipe form accepts only a fiberexec schedule sender
// ---------------------------------------------------------------------------

namespace {

struct noop_fn {
    void operator()() const {}
};

// Models "upstream | fiberexec::run(fn) compiles".
template <class Upstream>
concept pipeable_into_run = requires(Upstream up, noop_fn fn) { std::move(up) | fiberexec::run(fn); };

} // namespace

TEST_CASE("pipe run rejects senders other than schedule", "[run][pipe]") {
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    using schedule_t = decltype(stdexec::schedule(sched));

    // Positive control: without this the negative checks below could pass
    // vacuously (e.g. if run(fn) stopped being pipeable at all).
    STATIC_REQUIRE(pipeable_into_run<schedule_t>);

    // fn must run on a fiber for async_read/async_write to work, which is only
    // true directly downstream of a fiberexec schedule.  Piping run(fn) after a
    // sender that completes on some other thread would silently run fn
    // off-fiber, so it does not compile.
    STATIC_REQUIRE_FALSE(pipeable_into_run<decltype(stdexec::just())>);
    STATIC_REQUIRE_FALSE(pipeable_into_run<decltype(stdexec::just(42))>);

    // Known limitation: an adapted schedule sender still completes on the
    // fiber, but is no longer a schedule_sender, so it is rejected too.  A
    // future generic pipe form would relax this case (and only this case).
    STATIC_REQUIRE_FALSE(pipeable_into_run<decltype(stdexec::then(stdexec::schedule(sched), noop_fn{}))>);

    // The pipe form is the direct form: same sender type, not merely the same
    // semantics.
    STATIC_REQUIRE(std::is_same_v<decltype(stdexec::schedule(sched) | fiberexec::run(noop_fn{})),
                                  decltype(fiberexec::run(sched, noop_fn{}))>);
}
