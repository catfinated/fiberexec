#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <span>
#include <stdexcept>
#include <string_view>

namespace {

// A sender that has value type int but always signals set_stopped at runtime.
// Used to test that sync_wait returns nullopt on the stopped path.
struct always_stopped_sender {
    using sender_concept = stdexec::sender_tag;
    using completion_signatures = stdexec::completion_signatures<stdexec::set_value_t(int), stdexec::set_stopped_t()>;

    template <stdexec::receiver R> struct op {
        using operation_state_concept = stdexec::operation_state_tag;
        R rcvr_;
        void start() noexcept { stdexec::set_stopped(std::move(rcvr_)); }
    };

    template <stdexec::receiver R> [[nodiscard]] auto connect(R rcvr) const { return op<R>{std::move(rcvr)}; }
};

} // namespace

TEST_CASE("sync_wait suspends fiber and collects a single value", "[sync_wait]") {
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();
    int result = 0;

    stdexec::sync_wait(stdexec::schedule(sched) | stdexec::then([&] {
                           auto [val] =
                               *fiberexec::sync_wait(stdexec::schedule(sched) | stdexec::then([] { return 42; }));
                           result = val;
                       }));

    REQUIRE(result == 42);
}

TEST_CASE("sync_wait fans out with when_all and collects all values", "[sync_wait]") {
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();
    int sum = 0;

    stdexec::sync_wait(stdexec::schedule(sched) | stdexec::then([&] {
                           auto [a, b, c] = *fiberexec::sync_wait(
                               stdexec::when_all(stdexec::schedule(sched) | stdexec::then([] { return 1; }),
                                                 stdexec::schedule(sched) | stdexec::then([] { return 2; }),
                                                 stdexec::schedule(sched) | stdexec::then([] { return 3; })));
                           sum = a + b + c;
                       }));

    REQUIRE(sum == 6);
}

TEST_CASE("sync_wait propagates set_error as an exception", "[sync_wait]") {
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();
    bool threw = false;

    stdexec::sync_wait(stdexec::schedule(sched) | stdexec::then([&] {
                           try {
                               fiberexec::sync_wait(stdexec::schedule(sched) | stdexec::then([]() -> int {
                                                        throw std::runtime_error("inner error");
                                                    }));
                           } catch (std::runtime_error const& e) {
                               threw = (std::string_view{e.what()} == "inner error");
                           }
                       }));

    REQUIRE(threw);
}

TEST_CASE("sync_wait returns nullopt on set_stopped", "[sync_wait]") {
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();
    bool got_nullopt = false;

    stdexec::sync_wait(stdexec::schedule(sched) | stdexec::then([&] {
                           auto result = fiberexec::sync_wait(always_stopped_sender{});
                           got_nullopt = !result.has_value();
                       }));

    REQUIRE(got_nullopt);
}

TEST_CASE("sync_wait works with async I/O fan-out", "[sync_wait]") {
    // Fan out two concurrent async_recv calls and collect both results.
    using namespace std::chrono_literals;
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    std::array<int, 2> sv1{};
    std::array<int, 2> sv2{};
    REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv1.data()) == 0);
    REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv2.data()) == 0);
    auto [r1, w1] = sv1;
    auto [r2, w2] = sv2;

    constexpr std::string_view kA = "aaaa";
    constexpr std::string_view kB = "bbbb";
    std::array<char, 4> buf1{};
    std::array<char, 4> buf2{};

    stdexec::sync_wait(stdexec::when_all(
        // Reader fiber: fan out two reads using sync_wait
        stdexec::schedule(sched) | stdexec::then([&] {
            fiberexec::sync_wait(stdexec::when_all(
                stdexec::schedule(sched) |
                    stdexec::then([&] { fiberexec::async_recv(r1, std::as_writable_bytes(std::span{buf1})); }),
                stdexec::schedule(sched) |
                    stdexec::then([&] { fiberexec::async_recv(r2, std::as_writable_bytes(std::span{buf2})); })));
        }),
        // Writer fiber: send to both sockets
        stdexec::schedule(sched) | stdexec::then([&] {
            fiberexec::async_send(w1, std::as_bytes(std::span<char const>{kA.data(), kA.size()}));
            fiberexec::async_send(w2, std::as_bytes(std::span<char const>{kB.data(), kB.size()}));
        })));

    ::close(r1);
    ::close(w1);
    ::close(r2);
    ::close(w2);

    REQUIRE(std::string_view(buf1.data(), buf1.size()) == kA);
    REQUIRE(std::string_view(buf2.data(), buf2.size()) == kB);
}
