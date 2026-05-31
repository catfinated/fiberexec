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
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace {

struct bound_server {
    int fd;
    sockaddr_in addr;
};

bound_server make_bound_server() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    REQUIRE(fd >= 0);
    int opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    REQUIRE(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    REQUIRE(::listen(fd, 1) == 0);
    socklen_t addrlen = sizeof(addr);
    REQUIRE(::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &addrlen) == 0);
    return {.fd = fd, .addr = addr};
}

} // namespace

TEST_CASE("async_send_recv sends and receives in a single linked pair", "[linked][networking]") {
    // Server echoes: recv "ping" then send "pong".
    // Client uses async_send_recv to do both sides of the exchange as one linked
    // pair — one io_uring_submit call, one fiber suspension.
    auto [server_fd, addr] = make_bound_server();

    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    constexpr std::string_view kPing = "ping";
    constexpr std::string_view kPong = "pong";
    std::array<char, 4> pong_buf{};

    stdexec::sync_wait(stdexec::when_all(
        fiberexec::run(sched,
                       [&] {
                           int conn = fiberexec::async_accept(server_fd, nullptr, nullptr);
                           std::array<char, 4> ping_buf{};
                           fiberexec::async_recv(conn, std::as_writable_bytes(std::span{ping_buf}));
                           fiberexec::async_send(conn,
                                                 std::as_bytes(std::span<char const>{kPong.data(), kPong.size()}));
                           fiberexec::async_close(conn);
                       }),
        fiberexec::run(sched, [&] {
            int fd = ::socket(AF_INET, SOCK_STREAM, 0);
            fiberexec::async_connect(fd, reinterpret_cast<sockaddr const*>(&addr), sizeof(addr));
            auto [sent, recvd] =
                fiberexec::async_send_recv(fd, std::as_bytes(std::span<char const>{kPing.data(), kPing.size()}),
                                           std::as_writable_bytes(std::span{pong_buf}));
            REQUIRE(std::cmp_equal(sent, kPing.size()));
            REQUIRE(std::cmp_equal(recvd, kPong.size()));
            fiberexec::async_close(fd);
        })));

    ::close(server_fd);
    REQUIRE(std::string_view(pong_buf.data(), pong_buf.size()) == kPong);
}

TEST_CASE("async_write_fsync writes data and syncs in a single linked pair", "[linked][io]") {
    // Write bytes and fsync as one linked pair, then read back to verify.
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    constexpr std::string_view kData = "durable";
    std::array<char, 7> readback{};

    stdexec::sync_wait(fiberexec::run(sched, [&] {
        std::string path = "/tmp/fiberexec_write_fsync_XXXXXX";
        int fd = ::mkstemp(path.data());
        REQUIRE(fd >= 0);
        ::unlink(path.c_str());

        ssize_t written =
            fiberexec::async_write_fsync(fd, std::as_bytes(std::span<char const>{kData.data(), kData.size()}));
        REQUIRE(std::cmp_equal(written, kData.size()));

        fiberexec::async_read(fd, std::as_writable_bytes(std::span{readback}));
        fiberexec::async_close(fd);
    }));

    REQUIRE(std::string_view(readback.data(), readback.size()) == kData);
}

TEST_CASE("async_send_recv cancelled via sender stop token", "[linked][cancellation]") {
    // The send half of the linked pair completes immediately (the server
    // absorbs the ping).  The recv half blocks because the server never
    // replies.  A trigger fiber throws after a short delay — when_all
    // propagates stop to the client fiber via the fiber-local stop token.
    // submit_linked_and_wait must cancel all awaitables in the chain and
    // the linked pair must wake with ECANCELED.
    using namespace std::chrono_literals;
    auto [server_fd, addr] = make_bound_server();

    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    constexpr std::string_view kPing = "ping";
    bool cancelled = false;
    int client_fd = -1;
    int conn_fd = -1;

    try {
        stdexec::sync_wait(stdexec::when_all(
            // Server: accept and absorb the ping, then return WITHOUT closing
            // the connection.  Keeping conn open prevents the client's recv
            // from seeing EOF; it blocks indefinitely until the trigger fires.
            fiberexec::run(sched,
                           [&] {
                               conn_fd = fiberexec::async_accept(server_fd, nullptr, nullptr);
                               std::array<char, 4> buf{};
                               fiberexec::async_recv(conn_fd, std::as_writable_bytes(std::span{buf}));
                           }),
            // Client: connect, then issue send+recv as a linked pair.
            // The recv blocks indefinitely — cancellation must wake it.
            fiberexec::run(sched,
                           [&] {
                               client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
                               fiberexec::async_connect(client_fd, reinterpret_cast<sockaddr const*>(&addr),
                                                        sizeof(addr));
                               std::array<char, 4> recv_buf{};
                               try {
                                   fiberexec::async_send_recv(
                                       client_fd, std::as_bytes(std::span<char const>{kPing.data(), kPing.size()}),
                                       std::as_writable_bytes(std::span{recv_buf}));
                               } catch (std::system_error const& e) {
                                   cancelled = (e.code().value() == ECANCELED);
                                   throw;
                               }
                           }),
            // Trigger: sleep briefly then throw to propagate stop to all branches.
            fiberexec::run(sched, [&] {
                fiberexec::async_sleep_for(10ms);
                throw std::runtime_error("trigger cancel");
            })));
    } catch (...) {
    }

    ::close(server_fd);
    if (conn_fd >= 0) {
        ::close(conn_fd);
    }
    if (client_fd >= 0) {
        ::close(client_fd);
    }
    REQUIRE(cancelled);
}
