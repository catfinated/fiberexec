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
#include <span>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <vector>

namespace {

struct bound_server {
    int fd;
    sockaddr_in addr;
};

std::array<int, 2> make_socket_pair() {
    std::array<int, 2> sv{};
    REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv.data()) == 0);
    return sv;
}

bound_server make_bound_server(int backlog = 1) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    REQUIRE(fd >= 0);
    int opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    REQUIRE(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    REQUIRE(::listen(fd, backlog) == 0);
    socklen_t addrlen = sizeof(addr);
    REQUIRE(::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &addrlen) == 0);
    return {.fd = fd, .addr = addr};
}

} // namespace

TEST_CASE("async_recv and async_send exchange data via socketpair", "[networking]") {
    auto [recv_fd, send_fd] = make_socket_pair();
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();
    constexpr std::string_view kMsg = "ping";
    std::array<char, 4> buf{};

    stdexec::sync_wait(stdexec::when_all(
        stdexec::schedule(sched) | stdexec::then([&] {
            fiberexec::async_send(send_fd, std::as_bytes(std::span<char const>{kMsg.data(), kMsg.size()}));
        }),
        stdexec::schedule(sched) |
            stdexec::then([&] { fiberexec::async_recv(recv_fd, std::as_writable_bytes(std::span{buf})); })));

    ::close(recv_fd);
    ::close(send_fd);

    REQUIRE(std::string_view(buf.data(), buf.size()) == kMsg);
}

TEST_CASE("async_accept and async_connect establish a TCP connection", "[networking]") {
    auto [server_fd, addr] = make_bound_server();

    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();
    constexpr std::string_view kMsg = "hello";
    std::array<char, 5> buf{};
    int accepted_fd = -1;

    stdexec::sync_wait(stdexec::when_all(
        stdexec::schedule(sched) | stdexec::then([&] {
            sockaddr_storage peer{};
            socklen_t peerlen = sizeof(peer);
            accepted_fd = fiberexec::async_accept(server_fd, reinterpret_cast<sockaddr*>(&peer), &peerlen);
            fiberexec::async_recv(accepted_fd, std::as_writable_bytes(std::span{buf}));
        }),
        stdexec::schedule(sched) | stdexec::then([&] {
            int client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
            fiberexec::async_connect(client_fd, reinterpret_cast<sockaddr const*>(&addr), sizeof(addr));
            fiberexec::async_send(client_fd, std::as_bytes(std::span<char const>{kMsg.data(), kMsg.size()}));
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
    auto [recv_fd, send_fd] = make_socket_pair();
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();
    bool auto_cancelled = false;

    try {
        stdexec::sync_wait(stdexec::when_all(stdexec::schedule(sched) | stdexec::then([&] {
                                                 try {
                                                     std::array<char, 4> buf{};
                                                     fiberexec::async_recv(recv_fd,
                                                                           std::as_writable_bytes(std::span{buf}));
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
    // Fans out N fibers each blocked on async_recv.  A trigger fiber sleeps
    // briefly then throws, causing when_all to propagate stop to all N fibers
    // via the fiber-local stop token installed by schedule_sender.  Each
    // scheduler's cancel queue receives up to N/thread_count cancel requests
    // simultaneously; this verifies that flush_cancel_queue drains all of them
    // and no CQEs are lost or misattributed.
    using namespace std::chrono_literals;
    constexpr std::size_t N = 100;

    fiberexec::context ctx{4};
    auto sched = ctx.get_scheduler();

    // Empty read ends — every async_recv blocks until cancelled.
    std::vector<std::array<int, 2>> pairs(N);
    for (auto& p : pairs) {
        p = make_socket_pair();
    }

    std::atomic<int> cancelled{0};

    try {
        stdexec::sync_wait(stdexec::when_all(
            stdexec::bulk(stdexec::schedule(sched), stdexec::par, N,
                          [&](std::size_t i) {
                              char buf{};
                              try {
                                  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
                                  fiberexec::async_recv(pairs[i][1], std::as_writable_bytes(std::span{&buf, 1}));
                              } catch (std::system_error const& e) {
                                  if (e.code().value() == ECANCELED) {
                                      cancelled.fetch_add(1, std::memory_order_relaxed);
                                  }
                              }
                          }),
            // Trigger: short sleep then throw to propagate stop to all N recv fibers.
            stdexec::schedule(sched) | stdexec::then([&] {
                fiberexec::async_sleep_for(10ms);
                throw std::runtime_error("trigger");
            })));
    } catch (...) {
    }

    for (auto& [w, r] : pairs) {
        ::close(w);
        ::close(r);
    }

    REQUIRE(cancelled.load() == static_cast<int>(N));
}

TEST_CASE("async_recv with timeout fires ECANCELED when no data arrives", "[networking][timeout]") {
    using namespace std::chrono_literals;
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    auto [recv_fd, send_fd] = make_socket_pair();
    bool timed_out = false;
    stdexec::sync_wait(fiberexec::run(sched, [&] {
        char buf{};
        try {
            fiberexec::async_recv(recv_fd, std::as_writable_bytes(std::span{&buf, 1}), 0, 50ms);
        } catch (std::system_error const& e) {
            timed_out = (e.code().value() == ECANCELED);
        }
    }));

    ::close(recv_fd);
    ::close(send_fd);

    REQUIRE(timed_out);
}

TEST_CASE("multishot_acceptor accepts multiple connections with one SQE", "[networking][multishot]") {
    auto [server_fd, addr] = make_bound_server(128);

    constexpr int kClients = 8;
    fiberexec::context ctx{4};
    auto sched = ctx.get_scheduler();
    std::atomic<int> accepted{0};

    auto make_client = [&] {
        return fiberexec::run(sched, [&] {
            int fd = ::socket(AF_INET, SOCK_STREAM, 0);
            fiberexec::async_connect(fd, reinterpret_cast<sockaddr const*>(&addr), sizeof(addr));
            ::close(fd);
        });
    };

    stdexec::sync_wait(
        stdexec::when_all(fiberexec::run(sched,
                                         [&] {
                                             fiberexec::multishot_acceptor acc{server_fd, nullptr, nullptr};
                                             while (auto fd = acc.next()) {
                                                 fiberexec::async_close(*fd);
                                                 if (accepted.fetch_add(1, std::memory_order_acq_rel) == kClients - 1) {
                                                     ::shutdown(server_fd, SHUT_RDWR);
                                                 }
                                             }
                                         }),
                          make_client(), make_client(), make_client(), make_client(), make_client(), make_client(),
                          make_client(), make_client()));

    ::close(server_fd);
    REQUIRE(accepted.load() == kClients);
}

TEST_CASE("async_recv with timeout completes normally when data arrives in time", "[networking][timeout]") {
    using namespace std::chrono_literals;
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    auto [recv_fd, send_fd] = make_socket_pair();
    constexpr char kByte = 42;
    char result{};
    stdexec::sync_wait(
        stdexec::when_all(fiberexec::run(sched,
                                         [&] {
                                             fiberexec::async_sleep_for(10ms);
                                             fiberexec::async_send(send_fd, std::as_bytes(std::span{&kByte, 1}));
                                         }),
                          fiberexec::run(sched, [&] {
                              fiberexec::async_recv(recv_fd, std::as_writable_bytes(std::span{&result, 1}), 0, 500ms);
                          })));

    ::close(recv_fd);
    ::close(send_fd);

    REQUIRE(result == kByte);
}

TEST_CASE("multishot_recv delivers data and returns nullopt on EOF", "[networking][multishot]") {
    auto [recv_fd, send_fd] = make_socket_pair();

    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    constexpr std::string_view kMsg1 = "hello";
    constexpr std::string_view kMsg2 = "world";
    constexpr std::size_t buf_size{256UL};
    constexpr std::size_t buf_count{8UL};
    std::string received;

    stdexec::sync_wait(stdexec::when_all(
        fiberexec::run(sched,
                       [&] {
                           fiberexec::multishot_recv mr{recv_fd, buf_size, buf_count};
                           while (auto buf = mr.next()) {
                               auto span = buf->data();
                               received.append(reinterpret_cast<char const*>(span.data()), span.size());
                           }
                       }),
        fiberexec::run(sched, [&] {
            fiberexec::async_send(send_fd, std::as_bytes(std::span<char const>{kMsg1.data(), kMsg1.size()}));
            fiberexec::async_send(send_fd, std::as_bytes(std::span<char const>{kMsg2.data(), kMsg2.size()}));
            fiberexec::async_close(send_fd);
            send_fd = -1;
        })));

    ::close(recv_fd);
    if (send_fd >= 0) {
        ::close(send_fd);
    }

    REQUIRE(received == std::string(kMsg1) + std::string(kMsg2));
}

TEST_CASE("async_send_zc with fixed_buffer_pool delivers data via zero-copy send", "[networking][fixed-buffers]") {
    // AF_UNIX socketpairs do not support IORING_OP_SEND_ZC; use TCP.
    auto [server_fd, addr] = make_bound_server();

    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    constexpr std::string_view kMsg = "zero-copy";
    std::array<char, 9> buf{};

    stdexec::sync_wait(
        stdexec::when_all(fiberexec::run(sched,
                                         [&] {
                                             int conn_fd = fiberexec::async_accept(server_fd, nullptr, nullptr);
                                             fiberexec::async_recv(conn_fd, std::as_writable_bytes(std::span{buf}));
                                             fiberexec::async_close(conn_fd);
                                         }),
                          fiberexec::run(sched, [&] {
                              int fd = ::socket(AF_INET, SOCK_STREAM, 0);
                              fiberexec::async_connect(fd, reinterpret_cast<sockaddr const*>(&addr), sizeof(addr));
                              fiberexec::fixed_buffer_pool pool{64, 4};
                              auto fb = pool.borrow();
                              auto span = fb.data();
                              std::memcpy(span.data(), kMsg.data(), kMsg.size());
                              fiberexec::async_send_zc(fd, fb, kMsg.size());
                              fiberexec::async_close(fd);
                          })));

    ::close(server_fd);

    REQUIRE(std::string_view(buf.data(), kMsg.size()) == kMsg);
}

TEST_CASE("fixed_buffer_pool blocks borrow until a buffer is returned", "[networking][fixed-buffers]") {
    // Pool has one buffer. The single send fiber borrows, sends 'A', then
    // the fixed_buffer destructor returns the slot; the second borrow succeeds
    // and sends 'B'. AF_UNIX does not support IORING_OP_SEND_ZC; use TCP.
    auto [server_fd, addr] = make_bound_server();

    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    constexpr std::string_view kMsg = "AB";
    std::array<char, 2> buf{};
    std::atomic<int> sends_done{0};

    stdexec::sync_wait(
        stdexec::when_all(fiberexec::run(sched,
                                         [&] {
                                             int conn_fd = fiberexec::async_accept(server_fd, nullptr, nullptr);
                                             // Read exactly 2 bytes; loop handles TCP partial reads.
                                             std::size_t total = 0;
                                             while (total < buf.size()) {
                                                 ssize_t n = fiberexec::async_recv(
                                                     conn_fd, std::as_writable_bytes(std::span{buf}.subspan(total)));
                                                 if (n <= 0) {
                                                     break;
                                                 }
                                                 total += static_cast<std::size_t>(n);
                                             }
                                             fiberexec::async_close(conn_fd);
                                         }),
                          fiberexec::run(sched, [&] {
                              int fd = ::socket(AF_INET, SOCK_STREAM, 0);
                              fiberexec::async_connect(fd, reinterpret_cast<sockaddr const*>(&addr), sizeof(addr));
                              fiberexec::fixed_buffer_pool pool{64, 1};
                              {
                                  auto fb = pool.borrow();
                                  fb.data().front() = std::byte{'A'};
                                  fiberexec::async_send_zc(fd, fb, 1);
                                  sends_done.fetch_add(1, std::memory_order_relaxed);
                                  // fb destroyed here → buffer returned to pool
                              }
                              {
                                  auto fb = pool.borrow(); // would have blocked if buffer not returned
                                  fb.data().front() = std::byte{'B'};
                                  fiberexec::async_send_zc(fd, fb, 1);
                                  sends_done.fetch_add(1, std::memory_order_relaxed);
                              }
                              fiberexec::async_close(fd);
                          })));

    ::close(server_fd);

    REQUIRE(sends_done.load() == 2);
    REQUIRE(std::string_view(buf.data(), kMsg.size()) == kMsg);
}
