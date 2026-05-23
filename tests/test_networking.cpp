#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

#include <catch2/catch_test_macros.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <string_view>
#include <system_error>

namespace {
// NOLINTNEXTLINE(cert-err58-cpp,cppcoreguidelines-avoid-non-const-global-variables)
fiberexec::fiber_context g_ctx{2};
} // namespace

TEST_CASE("async_recv and async_send exchange data via socketpair", "[networking]") {
    std::array<int, 2> sv{};
    REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv.data()) == 0);
    auto [recv_fd, send_fd] = sv;

    auto sched = g_ctx.get_scheduler();
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

    auto sched = g_ctx.get_scheduler();
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

    auto sched = g_ctx.get_scheduler();
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
