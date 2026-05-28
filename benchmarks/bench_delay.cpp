// bench_delay.cpp — server-side processing delay: the fiber suspension payoff.
//
// Loopback I/O completes near-instantaneously; async ops return before a fiber
// would meaningfully yield.  This benchmark introduces a 1 ms server-side delay
// between recv and send — representing a slow upstream call, database query, or
// disk read — so the suspension property of fibers is exercised directly.
//
// A fiber suspended during the delay frees its OS thread to run another fiber.
// hardware_concurrency() threads can therefore serve thousands of concurrently
// blocked connections.  OS threads cannot: each sleeping connection consumes an
// entire thread for the duration of the delay.
//
// Concurrency sweep: 1 / 10 / 100 / 1000 connections.
// Thread baseline is capped at 100: 1000 sleeping OS threads is not a useful
// operating point and risks exhausting stack memory.
// kRoundTrips = 10 (vs bench_echo's 100) so each iteration completes in ~10 ms.

#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

// clang-format off
#include <exec/asio/asio_thread_pool.hpp>
#include <exec/asio/use_sender.hpp>
#include <boost/asio.hpp>
// clang-format on

#include <benchmark/benchmark.h>
#include <liburing.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <bit>
#include <chrono>
#include <cstdint>
#include <latch>
#include <span>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
namespace asio = boost::asio;

namespace {

constexpr int kRoundTrips = 10;
constexpr std::size_t kMsgSize = 64;
constexpr auto kDelay = 1ms;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

int make_server_socket() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    ::listen(fd, 4096);
    return fd;
}

sockaddr_in bound_addr(int fd) {
    sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
    return addr;
}

sockaddr_in asio_local_addr(asio::ip::tcp::acceptor const& acc) {
    auto ep = acc.local_endpoint();
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(ep.port());
    return addr;
}

// Blocking client — identical across all server benchmarks so client overhead
// does not skew the comparison.
void run_blocking_client(sockaddr_in const& server_addr) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ::connect(fd, reinterpret_cast<sockaddr const*>(&server_addr), sizeof(server_addr));
    std::array<char, kMsgSize> buf{};
    for (int i = 0; i < kRoundTrips; ++i) {
        ::send(fd, buf.data(), kMsgSize, MSG_NOSIGNAL);
        ::recv(fd, buf.data(), kMsgSize, MSG_WAITALL);
    }
    ::close(fd);
}

// ---------------------------------------------------------------------------
// Per-implementation connection handlers
// ---------------------------------------------------------------------------

// Fiber: async_sleep_for suspends the fiber without blocking the OS thread.
// Other fibers on the same thread continue to make progress during the delay.
void fiber_delay_handler(int conn_fd) {
    std::array<char, kMsgSize> buf{};
    for (int i = 0; i < kRoundTrips; ++i) {
        ssize_t n = fiberexec::async_recv(conn_fd, std::as_writable_bytes(std::span{buf}), MSG_WAITALL);
        fiberexec::async_sleep_for(kDelay);
        fiberexec::async_send(conn_fd, std::as_bytes(std::span{buf.data(), static_cast<std::size_t>(n)}), MSG_NOSIGNAL);
    }
    fiberexec::async_close(conn_fd);
}

// Thread: sleep_for blocks the OS thread for the entire delay.
// No other work can happen on this thread while the connection is "processing".
void thread_delay_handler(int conn_fd) {
    std::array<char, kMsgSize> buf{};
    for (int i = 0; i < kRoundTrips; ++i) {
        ssize_t n = ::recv(conn_fd, buf.data(), kMsgSize, MSG_WAITALL);
        if (n <= 0) {
            break;
        }
        std::this_thread::sleep_for(kDelay);
        ::send(conn_fd, buf.data(), static_cast<std::size_t>(n), MSG_NOSIGNAL);
    }
    ::close(conn_fd);
}

// Asio: steady_timer yields the coroutine during the delay.
asio::awaitable<void> asio_delay_handler(asio::ip::tcp::socket sock) {
    std::array<char, kMsgSize> buf{};
    auto ex = sock.get_executor();
    // NOLINTNEXTLINE(clang-analyzer-core.NullDereference)
    for (int i = 0; i < kRoundTrips; ++i) {
        co_await asio::async_read(sock, asio::buffer(buf), asio::use_awaitable);
        asio::steady_timer timer{ex, kDelay};
        co_await timer.async_wait(asio::use_awaitable);
        co_await asio::async_write(sock, asio::buffer(buf), asio::use_awaitable);
    }
}

// Takes acceptor by pointer — coroutine lifetime can outlive the caller's frame.
asio::awaitable<void> asio_delay_accept_loop(asio::ip::tcp::acceptor* acc, int64_t num_conns) {
    for (int64_t i = 0; i < num_conns; ++i) {
        auto sock = co_await acc->async_accept(asio::use_awaitable);
        asio::co_spawn(acc->get_executor(), asio_delay_handler(std::move(sock)), asio::detached);
    }
}

// ---------------------------------------------------------------------------
// io_uring delay baseline
//
// State machine with IORING_OP_TIMEOUT between recv and send.
// Transitions per connection:
//   recv  →(recv CQE, res>0)→  timeout  →(timeout CQE, res==-ETIME)→  send
//         →(send CQE, rounds_left>0)→  recv   ... (repeat)
//         →(send CQE, rounds_left==0)→  close
//
// All three op types tag their SQE with the same DelayConn*; the state field
// determines what action a CQE triggers.  Accept CQEs are distinguished by a
// static sentinel address.
// ---------------------------------------------------------------------------

struct DelayConn {
    int fd{};
    int rounds_left{};
    enum class State : uint8_t { recv, timeout, send } state{};
    ssize_t bytes_ready{};
    std::array<char, kMsgSize> buf{};
    __kernel_timespec ts{};
};

int g_delay_sentinel{}; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

} // namespace

// ---------------------------------------------------------------------------
// BM_FiberEchoDelay
// ---------------------------------------------------------------------------
static void BM_FiberEchoDelay(benchmark::State& state) {
    int64_t const num_conns = state.range(0);
    fiberexec::context ctx{std::thread::hardware_concurrency()};
    auto sched = ctx.get_scheduler();

    for ([[maybe_unused]] auto _ : state) {
        int server_fd = make_server_socket();
        sockaddr_in const addr = bound_addr(server_fd);
        fiberexec::channel<int> ch{std::bit_ceil(static_cast<std::size_t>(num_conns) + 1)};

        std::thread srv([&] {
            stdexec::sync_wait(stdexec::when_all(
                fiberexec::run(sched,
                               [&] {
                                   for (int64_t i = 0; i < num_conns; ++i) {
                                       int conn = fiberexec::async_accept(server_fd, nullptr, nullptr);
                                       if (conn < 0) {
                                           break;
                                       }
                                       if (ch.push(conn) != fiberexec::channel_op_status::success) {
                                           fiberexec::async_close(conn);
                                       }
                                   }
                                   ch.close();
                               }),
                stdexec::bulk(stdexec::schedule(sched), stdexec::par, static_cast<std::size_t>(num_conns),
                              [&](std::size_t) {
                                  int conn{};
                                  if (ch.pop(conn) == fiberexec::channel_op_status::success) {
                                      fiber_delay_handler(conn);
                                  }
                              })));
        });

        std::vector<std::thread> clients;
        clients.reserve(static_cast<std::size_t>(num_conns));
        for (int64_t i = 0; i < num_conns; ++i) {
            clients.emplace_back(run_blocking_client, std::cref(addr));
        }
        for (auto& t : clients) {
            t.join();
        }
        srv.join();
        ::close(server_fd);
    }

    state.SetItemsProcessed(state.iterations() * num_conns * kRoundTrips);
}
BENCHMARK(BM_FiberEchoDelay)->Arg(1)->Arg(10)->Arg(100)->Arg(1000)->UseRealTime();

// ---------------------------------------------------------------------------
// BM_ThreadEchoDelay
//
// Capped at 100 connections: beyond that, hundreds of OS threads sleeping
// simultaneously exhausts stack memory and adds heavy scheduler overhead.
// The gap between this and BM_FiberEchoDelay at num_conns=100 shows the
// cost of each thread blocking during the delay vs. each fiber suspending.
// ---------------------------------------------------------------------------
static void BM_ThreadEchoDelay(benchmark::State& state) {
    int64_t const num_conns = state.range(0);

    for ([[maybe_unused]] auto _ : state) {
        int server_fd = make_server_socket();
        sockaddr_in const addr = bound_addr(server_fd);
        std::latch done{num_conns};

        std::thread accept_thread([server_fd, num_conns, &done] {
            for (int64_t i = 0; i < num_conns; ++i) {
                int conn = ::accept(server_fd, nullptr, nullptr);
                if (conn < 0) {
                    break;
                }
                std::thread([conn, &done] {
                    thread_delay_handler(conn);
                    done.count_down();
                }).detach();
            }
        });

        std::vector<std::thread> clients;
        clients.reserve(static_cast<std::size_t>(num_conns));
        for (int64_t i = 0; i < num_conns; ++i) {
            clients.emplace_back(run_blocking_client, std::cref(addr));
        }
        for (auto& t : clients) {
            t.join();
        }
        accept_thread.join();
        done.wait();
        ::close(server_fd);
    }

    state.SetItemsProcessed(state.iterations() * num_conns * kRoundTrips);
}
BENCHMARK(BM_ThreadEchoDelay)->Arg(1)->Arg(10)->Arg(100)->UseRealTime();

// ---------------------------------------------------------------------------
// BM_AsioEchoDelay
// ---------------------------------------------------------------------------
static void BM_AsioEchoDelay(benchmark::State& state) {
    int64_t const num_conns = state.range(0);
    asio::thread_pool pool{std::thread::hardware_concurrency()};

    for ([[maybe_unused]] auto _ : state) {
        asio::ip::tcp::acceptor acceptor{pool.get_executor(), {asio::ip::address_v4::loopback(), 0}};
        sockaddr_in const addr = asio_local_addr(acceptor);

        asio::co_spawn(pool.get_executor(), asio_delay_accept_loop(&acceptor, num_conns), asio::detached);

        std::vector<std::thread> clients;
        clients.reserve(static_cast<std::size_t>(num_conns));
        for (int64_t i = 0; i < num_conns; ++i) {
            clients.emplace_back(run_blocking_client, std::cref(addr));
        }
        for (auto& t : clients) {
            t.join();
        }
    }

    state.SetItemsProcessed(state.iterations() * num_conns * kRoundTrips);
}
BENCHMARK(BM_AsioEchoDelay)->Arg(1)->Arg(10)->Arg(100)->Arg(1000)->UseRealTime();

// ---------------------------------------------------------------------------
// BM_IoUringEchoDelay
// ---------------------------------------------------------------------------
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
static void BM_IoUringEchoDelay(benchmark::State& state) {
    int64_t const num_conns = state.range(0);

    io_uring ring{};
    io_uring_queue_init(4096, &ring, 0);

    __kernel_timespec const kDelayTs{
        .tv_sec = 0,
        .tv_nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(kDelay).count(),
    };

    for ([[maybe_unused]] auto _ : state) {
        int server_fd = make_server_socket();
        sockaddr_in const addr = bound_addr(server_fd);

        {
            auto* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_accept(sqe, server_fd, nullptr, nullptr, 0);
            io_uring_sqe_set_data(sqe, &g_delay_sentinel);
            io_uring_submit(&ring);
        }

        std::vector<std::thread> clients;
        clients.reserve(static_cast<std::size_t>(num_conns));
        for (int64_t i = 0; i < num_conns; ++i) {
            clients.emplace_back(run_blocking_client, std::cref(addr));
        }

        int64_t accepted = 0;
        int64_t closed = 0;

        while (closed < num_conns) {
            io_uring_cqe* cqe = nullptr;
            io_uring_wait_cqe(&ring, &cqe);
            int const res = cqe->res;
            void* const ud = io_uring_cqe_get_data(cqe);
            io_uring_cqe_seen(&ring, cqe);

            if (ud == static_cast<void*>(&g_delay_sentinel)) {
                if (res >= 0) {
                    ++accepted;
                    // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
                    auto* conn = new DelayConn{
                        .fd = res, .rounds_left = kRoundTrips, .state = DelayConn::State::recv, .ts = kDelayTs};
                    auto* sqe = io_uring_get_sqe(&ring);
                    io_uring_prep_recv(sqe, conn->fd, conn->buf.data(), kMsgSize, MSG_WAITALL);
                    io_uring_sqe_set_data(sqe, conn);
                    if (accepted < num_conns) {
                        auto* sqe2 = io_uring_get_sqe(&ring);
                        io_uring_prep_accept(sqe2, server_fd, nullptr, nullptr, 0);
                        io_uring_sqe_set_data(sqe2, &g_delay_sentinel);
                    }
                    io_uring_submit(&ring);
                }
            } else {
                // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
                auto* conn = static_cast<DelayConn*>(ud);
                switch (conn->state) {
                case DelayConn::State::recv:
                    if (res > 0) {
                        conn->bytes_ready = res;
                        conn->state = DelayConn::State::timeout;
                        auto* sqe = io_uring_get_sqe(&ring);
                        io_uring_prep_timeout(sqe, &conn->ts, 0, 0);
                        io_uring_sqe_set_data(sqe, conn);
                        io_uring_submit(&ring);
                    } else {
                        // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
                        delete conn;
                        ++closed;
                    }
                    break;
                case DelayConn::State::timeout:
                    // res == -ETIME: timeout fired normally; proceed to send.
                    conn->state = DelayConn::State::send;
                    {
                        auto* sqe = io_uring_get_sqe(&ring);
                        io_uring_prep_send(sqe, conn->fd, conn->buf.data(), static_cast<std::size_t>(conn->bytes_ready),
                                           MSG_NOSIGNAL);
                        io_uring_sqe_set_data(sqe, conn);
                        io_uring_submit(&ring);
                    }
                    break;
                case DelayConn::State::send:
                    if (--conn->rounds_left > 0) {
                        conn->state = DelayConn::State::recv;
                        auto* sqe = io_uring_get_sqe(&ring);
                        io_uring_prep_recv(sqe, conn->fd, conn->buf.data(), kMsgSize, MSG_WAITALL);
                        io_uring_sqe_set_data(sqe, conn);
                        io_uring_submit(&ring);
                    } else {
                        ::close(conn->fd);
                        // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
                        delete conn;
                        ++closed;
                    }
                    break;
                }
            }
        }

        for (auto& t : clients) {
            t.join();
        }
        ::close(server_fd);
    }

    state.SetItemsProcessed(state.iterations() * num_conns * kRoundTrips);
    io_uring_queue_exit(&ring);
}
BENCHMARK(BM_IoUringEchoDelay)->Arg(1)->Arg(10)->Arg(100)->Arg(1000)->UseRealTime();

int main(int argc, char** argv) {
    struct rlimit lim{};
    if (::getrlimit(RLIMIT_NOFILE, &lim) == 0) {
        lim.rlim_cur = lim.rlim_max;
        ::setrlimit(RLIMIT_NOFILE, &lim);
    }
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}
