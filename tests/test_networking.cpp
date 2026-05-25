#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

#include <catch2/catch_test_macros.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <stop_token>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

TEST_CASE("async_recv and async_send exchange data via socketpair", "[networking]") {
    fiberexec::fiber_context ctx{2};
    std::array<int, 2> sv{};
    REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv.data()) == 0);
    auto [recv_fd, send_fd] = sv;

    auto sched = ctx.get_scheduler();
    constexpr std::string_view kMsg = "ping";
    std::array<char, 4> buf{};

    stdexec::sync_wait(stdexec::when_all(
        stdexec::schedule(sched) | stdexec::then([&] { fiberexec::async_send(send_fd, kMsg.data(), kMsg.size()); }),
        stdexec::schedule(sched) | stdexec::then([&] { fiberexec::async_recv(recv_fd, buf.data(), buf.size()); })));

    ::close(recv_fd);
    ::close(send_fd);

    REQUIRE(std::string_view(buf.data(), buf.size()) == kMsg);
}

TEST_CASE("async_accept and async_connect establish a TCP connection", "[networking]") {
    int server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    REQUIRE(server_fd >= 0);

    int opt = 1;
    ::setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;

    REQUIRE(::bind(server_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    REQUIRE(::listen(server_fd, 1) == 0);

    socklen_t addrlen = sizeof(addr);
    REQUIRE(::getsockname(server_fd, reinterpret_cast<sockaddr*>(&addr), &addrlen) == 0);

    fiberexec::fiber_context ctx{2};
    auto sched = ctx.get_scheduler();
    constexpr std::string_view kMsg = "hello";
    std::array<char, 5> buf{};
    int accepted_fd = -1;

    stdexec::sync_wait(stdexec::when_all(
        stdexec::schedule(sched) | stdexec::then([&] {
            sockaddr_storage peer{};
            socklen_t peerlen = sizeof(peer);
            accepted_fd = fiberexec::async_accept(server_fd, reinterpret_cast<sockaddr*>(&peer), &peerlen);
            fiberexec::async_recv(accepted_fd, buf.data(), buf.size());
        }),
        stdexec::schedule(sched) | stdexec::then([&] {
            int client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
            fiberexec::async_connect(client_fd, reinterpret_cast<sockaddr const*>(&addr), sizeof(addr));
            fiberexec::async_send(client_fd, kMsg.data(), kMsg.size());
            ::close(client_fd);
        })));

    if (accepted_fd >= 0) {
        ::close(accepted_fd);
    }
    ::close(server_fd);

    REQUIRE(std::string_view(buf.data(), buf.size()) == kMsg);
}

TEST_CASE("async_recv cancelled automatically via sender stop token", "[networking][cancellation]") {
    // when_all error branch cancels the blocked recv — no explicit stop token.
    using namespace std::chrono_literals;
    std::array<int, 2> sv{};
    REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv.data()) == 0);
    auto [recv_fd, send_fd] = sv;

    fiberexec::fiber_context ctx{2};
    auto sched = ctx.get_scheduler();
    bool auto_cancelled = false;

    try {
        stdexec::sync_wait(stdexec::when_all(stdexec::schedule(sched) | stdexec::then([&] {
                                                 try {
                                                     std::array<char, 4> buf{};
                                                     fiberexec::async_recv(recv_fd, buf.data(), buf.size());
                                                 } catch (std::system_error const& e) {
                                                     auto_cancelled = (e.code().value() == ECANCELED);
                                                 }
                                             }),
                                             stdexec::schedule(sched) | stdexec::then([&] {
                                                 fiberexec::async_sleep_for(10ms);
                                                 throw std::runtime_error("trigger cancel");
                                             })));
    } catch (...) {
    }

    ::close(recv_fd);
    ::close(send_fd);

    REQUIRE(auto_cancelled);
}

TEST_CASE("cancel queue drains correctly under load with many concurrent async_recv operations",
          "[networking][cancellation][stress]") {
    // Fans out N fibers each blocked on async_recv with a shared stop token.
    // A trigger thread fires request_stop() after a brief delay so all fibers
    // have had time to submit their SQEs. Each scheduler's cancel queue then
    // receives up to N/thread_count cancel requests simultaneously; this
    // verifies that flush_cancel_queue drains all of them and no CQEs are
    // lost or misattributed.
    using namespace std::chrono_literals;
    constexpr std::size_t N = 100;

    fiberexec::fiber_context ctx{4};
    auto sched = ctx.get_scheduler();

    // Empty read ends — every async_recv blocks until cancelled.
    std::vector<std::array<int, 2>> pairs(N);
    for (auto& p : pairs) {
        REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, p.data()) == 0);
    }

    std::atomic<int> cancelled{0};
    std::stop_source ss;

    std::thread trigger{[&ss] {
        std::this_thread::sleep_for(10ms);
        ss.request_stop();
    }};

    stdexec::sync_wait(stdexec::bulk(stdexec::schedule(sched), stdexec::par, N, [&](std::size_t i) {
        char buf{};
        try {
            fiberexec::async_recv(pairs.at(i).at(1), &buf, 1, 0, ss.get_token());
        } catch (std::system_error const& e) {
            if (e.code().value() == ECANCELED) {
                cancelled.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }));

    trigger.join();

    for (auto& [w, r] : pairs) {
        ::close(w);
        ::close(r);
    }

    REQUIRE(cancelled.load() == static_cast<int>(N));
}
