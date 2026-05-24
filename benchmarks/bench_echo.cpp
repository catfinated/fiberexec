#include <fiberexec/fiberexec.hpp>

#include <fiberexec/detail/fiber_ops.hpp>

#include <benchmark/benchmark.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <thread>
#include <vector>

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
// The same client code is used for both fiber and thread server benchmarks
// so client-side overhead is identical and does not skew the comparison.
void run_blocking_client(sockaddr_in const& server_addr) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ::connect(fd, reinterpret_cast<sockaddr const*>(&server_addr), sizeof(server_addr));
    char buf[kMsgSize] = {};
    for (int i = 0; i < kRoundTrips; ++i) {
        ::send(fd, buf, kMsgSize, MSG_NOSIGNAL);
        ::recv(fd, buf, kMsgSize, MSG_WAITALL);
    }
    ::close(fd);
}

// Server-side handler for the fiber echo benchmark.
// Runs inside a fiberexec fiber; async_recv/send yield the fiber rather than
// blocking the OS thread.
void fiber_conn_handler(int conn_fd) {
    char buf[kMsgSize];
    for (int i = 0; i < kRoundTrips; ++i) {
        ssize_t n = fiberexec::async_recv(conn_fd, buf, kMsgSize);
        if (n <= 0)
            break;
        fiberexec::async_send(conn_fd, buf, static_cast<std::size_t>(n));
    }
    ::close(conn_fd);
}

// Server-side handler for the thread echo baseline.
// Runs in its own OS thread; recv/send block the thread while waiting.
void thread_conn_handler(int conn_fd) {
    char buf[kMsgSize];
    for (int i = 0; i < kRoundTrips; ++i) {
        ssize_t n = ::recv(conn_fd, buf, kMsgSize, MSG_WAITALL);
        if (n <= 0)
            break;
        ::send(conn_fd, buf, static_cast<std::size_t>(n), MSG_NOSIGNAL);
    }
    ::close(conn_fd);
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
// Clients are OS threads with blocking syscalls — identical to the thread
// baseline — so client-side overhead does not affect the comparison.
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

    for (auto _ : state) {
        int server_fd = make_server_socket();
        sockaddr_in const addr = bound_addr(server_fd);

        // Post the accept loop onto the pool. It accepts num_conns connections
        // and posts a handler fiber per connection.
        //
        // We use detail::schedule_task directly because when_all requires a
        // compile-time-fixed number of senders; a runtime-variable fan-out
        // needs a lower-level post mechanism.
        fiberexec::detail::schedule_task(ctx.pool(), [&ctx, server_fd, num_conns] {
            for (int64_t i = 0; i < num_conns; ++i) {
                int conn = fiberexec::async_accept(server_fd, nullptr, nullptr);
                if (conn < 0)
                    break;
                fiberexec::detail::schedule_task(ctx.pool(), [conn] { fiber_conn_handler(conn); });
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
//
// This is the baseline that the fiber server is measured against: fibers
// should outperform threads as num_conns grows, because fiber creation and
// context switching are cheaper and memory overhead per fiber is smaller.
// ---------------------------------------------------------------------------
static void BM_ThreadEchoServer(benchmark::State& state) {
    int64_t const num_conns = state.range(0);

    for (auto _ : state) {
        int server_fd = make_server_socket();
        sockaddr_in const addr = bound_addr(server_fd);

        std::thread accept_thread([server_fd, num_conns] {
            for (int64_t i = 0; i < num_conns; ++i) {
                int conn = ::accept(server_fd, nullptr, nullptr);
                if (conn < 0)
                    break;
                std::thread(thread_conn_handler, conn).detach();
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

        // All clients done → all handlers done → accept loop done.
        accept_thread.join();
        ::close(server_fd);
    }

    state.SetItemsProcessed(state.iterations() * num_conns * kRoundTrips);
}
BENCHMARK(BM_ThreadEchoServer)->Arg(1)->Arg(10)->Arg(100)->UseRealTime();

BENCHMARK_MAIN();
