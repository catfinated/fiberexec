#include <fiberexec/fiberexec.hpp>

#include <fiberexec/detail/fiber_ops.hpp>

#include <benchmark/benchmark.h>

// exec/asio headers — configured by cmake/Dependencies.cmake to use Boost.Asio.
// Must be included before <boost/asio.hpp> so the configured asio_config.hpp
// is found first in the include path.
// clang-format off
#include <exec/asio/asio_thread_pool.hpp>
#include <exec/asio/use_sender.hpp>
#include <boost/asio.hpp>
// clang-format on

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cstdint>
#include <latch>
#include <thread>
#include <vector>

namespace asio = boost::asio;

// ---------------------------------------------------------------------------
// Common helpers
// ---------------------------------------------------------------------------
namespace {

// Each connection exchanges this many messages before closing.
constexpr int kRoundTrips = 100;
// Small fixed payload — fits in a single TCP segment on loopback.
constexpr std::size_t kMsgSize = 64;

int make_server_socket() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0; // let the kernel assign a free port
    ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    ::listen(fd, 4096); // large backlog so all clients can connect up-front
    return fd;
}

sockaddr_in bound_addr(int fd) {
    sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
    return addr;
}

// Client: runs in an OS thread with blocking syscalls.
// The same client code is used for all server benchmarks so client-side
// overhead is identical and does not skew the comparison.
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

// Server-side handler for the fiber echo benchmark.
// Runs inside a fiberexec fiber; async_recv/send yield the fiber rather than
// blocking the OS thread.
void fiber_conn_handler(int conn_fd) {
    std::array<char, kMsgSize> buf{};
    for (int i = 0; i < kRoundTrips; ++i) {
        ssize_t n = fiberexec::async_recv(conn_fd, buf.data(), kMsgSize);
        fiberexec::async_send(conn_fd, buf.data(), static_cast<std::size_t>(n));
    }
    ::close(conn_fd);
}

// Server-side handler for the thread echo baseline.
// Runs in its own OS thread; recv/send block the thread while waiting.
void thread_conn_handler(int conn_fd) {
    std::array<char, kMsgSize> buf{};
    for (int i = 0; i < kRoundTrips; ++i) {
        ssize_t n = ::recv(conn_fd, buf.data(), kMsgSize, MSG_WAITALL);
        ::send(conn_fd, buf.data(), static_cast<std::size_t>(n), MSG_NOSIGNAL);
    }
    ::close(conn_fd);
}

// ---------------------------------------------------------------------------
// Asio helpers — shared by BM_AsioEchoServer and BM_AsioExecEchoServer.
// ---------------------------------------------------------------------------

// Convert an Asio acceptor's bound endpoint to a sockaddr_in so the existing
// run_blocking_client helper can connect to it.
sockaddr_in asio_local_addr(asio::ip::tcp::acceptor const& acc) {
    auto ep = acc.local_endpoint();
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(ep.port());
    return addr;
}

// Connection handler used by both Asio benchmarks.  Reads and echoes
// kRoundTrips fixed-size messages on a single accepted socket.
asio::awaitable<void> asio_conn_handler(asio::ip::tcp::socket sock) {
    std::array<char, kMsgSize> buf{};
    for (int i = 0; i < kRoundTrips; ++i) {
        co_await asio::async_read(sock, asio::buffer(buf), asio::use_awaitable);
        co_await asio::async_write(sock, asio::buffer(buf), asio::use_awaitable);
    }
}

// Accept loop for BM_AsioEchoServer.  Runs as an Asio coroutine; each
// accepted socket is handed off to a detached handler coroutine.
// Takes the acceptor by pointer rather than reference: coroutine lifetimes can
// outlive their caller's stack frame, making reference parameters unsafe.
asio::awaitable<void> asio_accept_loop(asio::ip::tcp::acceptor* acc, int64_t num_conns) {
    for (int64_t i = 0; i < num_conns; ++i) {
        auto sock = co_await acc->async_accept(asio::use_awaitable);
        asio::co_spawn(acc->get_executor(), asio_conn_handler(std::move(sock)), asio::detached);
    }
}

} // namespace

// ---------------------------------------------------------------------------
// BM_FiberEchoServer
//
// The server runs inside a fiberexec fiber pool. An accept-loop fiber accepts
// num_conns connections; for each it posts a handler fiber via
// detail::schedule_task (the same mechanism run() uses internally). With N
// pool threads, up to N fibers can be waiting on io_uring CQEs simultaneously
// while the OS threads remain free for other fibers.
//
// Clients are OS threads with blocking syscalls — identical to the Asio and
// thread baselines — so client-side overhead does not affect the comparison.
//
// When all clients have joined, every client has completed kRoundTrips
// round-trips, which implies every server handler also completed, which
// implies the accept loop accepted all num_conns connections and exited.
// It is therefore safe to close the server socket at that point.
// ---------------------------------------------------------------------------
static void BM_FiberEchoServer(benchmark::State& state) {
    int64_t const num_conns = state.range(0);

    // Context is created once and reused across iterations to amortize
    // thread-pool and io_uring ring startup cost.
    fiberexec::fiber_context ctx{std::thread::hardware_concurrency()};

    for ([[maybe_unused]] auto _ : state) {
        int server_fd = make_server_socket();
        sockaddr_in const addr = bound_addr(server_fd);

        // Barrier: each handler fiber counts down when it closes its fd.
        // The iteration body waits before closing the server socket so that
        // open connection fds don't accumulate across auto-calibrated iterations.
        std::latch done{num_conns};

        // Post the accept loop onto the pool. It accepts num_conns connections
        // and posts a handler fiber per connection.
        //
        // We use detail::schedule_task directly because when_all requires a
        // compile-time-fixed number of senders; a runtime-variable fan-out
        // needs a lower-level post mechanism.
        fiberexec::detail::schedule_task(ctx.pool(), [&ctx, server_fd, num_conns, &done] {
            for (int64_t i = 0; i < num_conns; ++i) {
                int conn = fiberexec::async_accept(server_fd, nullptr, nullptr);
                if (conn < 0) {
                    break;
                }
                fiberexec::detail::schedule_task(ctx.pool(), [conn, &done] {
                    fiber_conn_handler(conn);
                    done.count_down();
                });
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

        // Wait for all handler fibers to close their fds before the next iter.
        done.wait();
        ::close(server_fd);
    }

    state.SetItemsProcessed(state.iterations() * num_conns * kRoundTrips);
}
BENCHMARK(BM_FiberEchoServer)->Arg(1)->Arg(10)->Arg(100)->Arg(1000)->UseRealTime();

// ---------------------------------------------------------------------------
// BM_ThreadEchoServer
//
// Classic thread-per-connection server. A dedicated accept thread dequeues
// connections and detaches a handler thread for each. At num_conns=100 this
// creates ~200 OS threads (100 server handlers + 100 clients); at 1000 it
// would require ~2000 threads, so that point is omitted.
// ---------------------------------------------------------------------------
static void BM_ThreadEchoServer(benchmark::State& state) {
    int64_t const num_conns = state.range(0);

    for ([[maybe_unused]] auto _ : state) {
        int server_fd = make_server_socket();
        sockaddr_in const addr = bound_addr(server_fd);

        // Barrier: each detached handler thread counts down when it closes its
        // fd, preventing open fds from accumulating across iterations.
        std::latch done{num_conns};

        std::thread accept_thread([server_fd, num_conns, &done] {
            for (int64_t i = 0; i < num_conns; ++i) {
                int conn = ::accept(server_fd, nullptr, nullptr);
                if (conn < 0) {
                    break;
                }
                std::thread([conn, &done] {
                    thread_conn_handler(conn);
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
        // Wait for all handler threads to close their fds before the next iter.
        done.wait();
        ::close(server_fd);
    }

    state.SetItemsProcessed(state.iterations() * num_conns * kRoundTrips);
}
BENCHMARK(BM_ThreadEchoServer)->Arg(1)->Arg(10)->Arg(100)->UseRealTime();

// ---------------------------------------------------------------------------
// BM_AsioEchoServer
//
// Server uses a plain Boost.Asio thread pool (asio::thread_pool) with C++20
// coroutines.  The accept loop and per-connection handlers are
// asio::awaitable<void> coroutines launched via asio::co_spawn.  This is a
// common pattern in production Asio codebases.
//
// The pool is created once and reused across iterations; a fresh acceptor
// (new kernel port) is allocated per iteration.
// ---------------------------------------------------------------------------
static void BM_AsioEchoServer(benchmark::State& state) {
    int64_t const num_conns = state.range(0);
    asio::thread_pool pool{std::thread::hardware_concurrency()};

    for ([[maybe_unused]] auto _ : state) {
        asio::ip::tcp::acceptor acceptor{pool.get_executor(), {asio::ip::address_v4::loopback(), 0}};
        sockaddr_in const addr = asio_local_addr(acceptor);

        asio::co_spawn(pool.get_executor(), asio_accept_loop(&acceptor, num_conns), asio::detached);

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
BENCHMARK(BM_AsioEchoServer)->Arg(1)->Arg(10)->Arg(100)->Arg(1000)->UseRealTime();

// ---------------------------------------------------------------------------
// BM_AsioExecEchoServer
//
// Server uses exec::asio::asio_thread_pool — stdexec's P2300 scheduler backed
// by Boost.Asio's thread pool.  This is the execution model from the asioexec
// integration in the stdexec repo.
//
// The accept loop runs in a dedicated OS thread and drives each individual
// accept as a P2300 sender via exec::asio::use_sender + stdexec::sync_wait.
// This is the natural asioexec pattern for sequential async work: use_sender
// converts an Asio async operation into a P2300 sender; sync_wait drives it to
// completion on the calling thread while the pool's io_context threads handle
// the actual I/O.  Once the socket is in hand, a per-connection coroutine
// handler is spawned on the pool executor.
//
// Connection handlers use asio::use_awaitable (same as BM_AsioEchoServer) —
// inside a sequential loop there is no advantage to re-expressing each async
// step as a sender, and the Asio coroutine path is the ergonomic choice.
// ---------------------------------------------------------------------------
static void BM_AsioExecEchoServer(benchmark::State& state) {
    int64_t const num_conns = state.range(0);
    exec::asio::asio_thread_pool pool{static_cast<std::uint32_t>(std::thread::hardware_concurrency())};

    for ([[maybe_unused]] auto _ : state) {
        asio::ip::tcp::acceptor acceptor{pool.get_executor(), {asio::ip::address_v4::loopback(), 0}};
        sockaddr_in const addr = asio_local_addr(acceptor);

        // Accept loop: each accept is a P2300 sender (use_sender); sync_wait
        // blocks this dedicated thread while the pool's io_context delivers the
        // completion.  The pool is therefore never starved — only the accept
        // thread blocks.
        std::thread accept_thread([&] {
            for (int64_t i = 0; i < num_conns; ++i) {
                auto opt = stdexec::sync_wait(acceptor.async_accept(exec::asio::use_sender));
                if (!opt) {
                    break;
                }
                auto sock = std::move(std::get<0>(*opt));
                asio::co_spawn(pool.get_executor(), asio_conn_handler(std::move(sock)), asio::detached);
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
    }

    state.SetItemsProcessed(state.iterations() * num_conns * kRoundTrips);
}
BENCHMARK(BM_AsioExecEchoServer)->Arg(1)->Arg(10)->Arg(100)->Arg(1000)->UseRealTime();

// The 1000-connection benchmark opens ~2000 fds simultaneously (1000 client
// sockets + 1000 accepted fds + io_uring/eventfd overhead per thread). The
// default soft limit (often 1024) is too low; raise it to the hard limit
// before any benchmarks run.
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
