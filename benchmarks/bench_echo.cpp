#include <fiberexec/fiberexec.hpp>

#include <benchmark/benchmark.h>

// exec/asio headers — configured by cmake/Dependencies.cmake to use Boost.Asio.
// Must be included before <boost/asio.hpp> so the configured asio_config.hpp
// is found first in the include path.
// clang-format off
#include <exec/asio/asio_thread_pool.hpp>
#include <exec/asio/use_sender.hpp>
#include <boost/asio.hpp>
// clang-format on

#include <common/asio_helpers.hpp>
#include <common/tcp_helpers.hpp>

#include <sys/resource.h>
#include <unistd.h>

#include <liburing.h>

#include <array>
#include <bit>
#include <cstdint>
#include <latch>
#include <span>
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
        ssize_t n = fiberexec::async_recv(conn_fd, std::as_writable_bytes(std::span{buf}), MSG_WAITALL);
        fiberexec::async_send(conn_fd, std::as_bytes(std::span{buf.data(), static_cast<std::size_t>(n)}), MSG_NOSIGNAL);
    }
    fiberexec::async_close(conn_fd);
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

// Connection handler used by both Asio benchmarks.  Reads and echoes
// kRoundTrips fixed-size messages on a single accepted socket.
asio::awaitable<void> asio_conn_handler(asio::ip::tcp::socket sock) {
    std::array<char, kMsgSize> buf{};
    // NOLINTNEXTLINE(clang-analyzer-core.NullDereference)
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

// ---------------------------------------------------------------------------
// Helpers for the message-size sweep (BM_*EchoMsgSize).
//
// These mirror the fixed-size handlers above but accept a runtime msg_size so
// the buffer uses std::vector.  The fiber handler passes MSG_WAITALL so a
// single async_recv call fills the buffer, matching the thread handler's
// semantics and removing the need for a partial-read loop.
// ---------------------------------------------------------------------------

// Fixed concurrency for the message-size sweep benchmarks.
constexpr int64_t kMsgSizeBenchConns = 10;

void run_blocking_client_dyn(sockaddr_in const& server_addr, std::size_t msg_size) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ::connect(fd, reinterpret_cast<sockaddr const*>(&server_addr), sizeof(server_addr));
    std::vector<char> buf(msg_size);
    for (int i = 0; i < kRoundTrips; ++i) {
        ::send(fd, buf.data(), msg_size, MSG_NOSIGNAL);
        ::recv(fd, buf.data(), msg_size, MSG_WAITALL);
    }
    ::close(fd);
}

void fiber_conn_handler_dyn(int conn_fd, std::size_t msg_size) {
    std::vector<char> buf(msg_size);
    for (int i = 0; i < kRoundTrips; ++i) {
        fiberexec::async_recv(conn_fd, std::as_writable_bytes(std::span{buf}), MSG_WAITALL);
        fiberexec::async_send(conn_fd, std::as_bytes(std::span{buf}), MSG_NOSIGNAL);
    }
    fiberexec::async_close(conn_fd);
}

void thread_conn_handler_dyn(int conn_fd, std::size_t msg_size) {
    std::vector<char> buf(msg_size);
    for (int i = 0; i < kRoundTrips; ++i) {
        ssize_t n = ::recv(conn_fd, buf.data(), msg_size, MSG_WAITALL);
        if (n <= 0) {
            break;
        }
        ::send(conn_fd, buf.data(), static_cast<std::size_t>(n), MSG_NOSIGNAL);
    }
    ::close(conn_fd);
}

asio::awaitable<void> asio_conn_handler_dyn(asio::ip::tcp::socket sock, std::size_t msg_size) {
    std::vector<char> buf(msg_size);
    for (int i = 0; i < kRoundTrips; ++i) {
        co_await asio::async_read(sock, asio::buffer(buf), asio::use_awaitable);
        co_await asio::async_write(sock, asio::buffer(buf), asio::use_awaitable);
    }
}

// Takes acceptor and msg_size by value / pointer (not reference) — coroutine
// lifetimes can outlive the caller's stack frame.
asio::awaitable<void> asio_accept_loop_dyn(asio::ip::tcp::acceptor* acc, int64_t num_conns, std::size_t msg_size) {
    for (int64_t i = 0; i < num_conns; ++i) {
        auto sock = co_await acc->async_accept(asio::use_awaitable);
        asio::co_spawn(acc->get_executor(), asio_conn_handler_dyn(std::move(sock), msg_size), asio::detached);
    }
}

// ---------------------------------------------------------------------------
// Raw io_uring baseline helpers
//
// A minimal state machine for an echo connection driven purely by io_uring,
// with no fibers, coroutines, or P2300 overhead.  The pending operation
// (recv or send) is stored directly in the connection struct so the SQE
// user_data slot holds a plain IoUringConn* with no encoding tricks.
//
// A static sentinel address marks accept CQEs so the event loop can
// distinguish them from connection CQEs without an extra allocation.
// ---------------------------------------------------------------------------

struct IoUringConn {
    int fd;
    int rounds_left;
    bool pending_recv; // true while a recv is in flight, false while a send is
    std::vector<char> buf;
};

// Sentinel: its address is distinct from any heap-allocated IoUringConn*.
int g_accept_sentinel{}; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

} // namespace

// ---------------------------------------------------------------------------
// BM_FiberEchoServer
//
// The server runs inside a fiberexec fiber pool using the public API:
// fiberexec::run for the accept loop, fiberexec::channel<int> for the
// connection queue, and stdexec::bulk for N concurrent handler fibers.
// With N pool threads, up to N fibers can wait on io_uring CQEs simultaneously
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
    fiberexec::context ctx{std::thread::hardware_concurrency()};
    auto sched = ctx.get_scheduler();

    for ([[maybe_unused]] auto _ : state) {
        int server_fd = make_server_socket();
        sockaddr_in const addr = bound_addr(server_fd);
        fiberexec::channel<int> ch{std::bit_ceil(static_cast<std::size_t>(num_conns) + 1)};

        // Server runs in a background thread so the main thread can launch
        // clients concurrently.  when_all naturally synchronizes: sync_wait
        // returns only when both the accept loop and all N worker fibers are
        // done, replacing the explicit std::latch from the old pattern.
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
                                      fiber_conn_handler(conn);
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
BENCHMARK(BM_FiberEchoServer)->Arg(1)->Arg(10)->Arg(100)->Arg(1000)->UseRealTime();

// ---------------------------------------------------------------------------
// BM_FiberEchoServer1T
//
// Same as BM_FiberEchoServer but with a single worker thread, matching the
// parallelism of BM_IoUringEchoServer.  The delta between these two isolates
// the pure cost of the Boost.Fiber + P2300 sender/receiver layer over a raw
// io_uring event loop.  The delta between BM_FiberEchoServer1T and
// BM_FiberEchoServer shows the benefit of adding more threads.
// ---------------------------------------------------------------------------
static void BM_FiberEchoServer1T(benchmark::State& state) {
    int64_t const num_conns = state.range(0);
    fiberexec::context ctx{1};
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
                                      fiber_conn_handler(conn);
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
BENCHMARK(BM_FiberEchoServer1T)->Arg(1)->Arg(10)->Arg(100)->Arg(1000)->UseRealTime();

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

// ---------------------------------------------------------------------------
// BM_IoUringEchoServer
//
// Baseline: a hand-rolled single-ring io_uring event loop with no fibers and
// no P2300 overhead.  Each accepted connection is represented by an IoUringConn
// that records which operation is in flight; the SQE user_data slot holds a
// plain pointer to it.  On each CQE the event loop transitions the connection:
// recv complete → submit send, send complete → submit recv (or close).
//
// One ring, one event-loop thread.  fiberexec uses hardware_concurrency()
// threads; the gap between the two at low concurrency isolates the fiber+P2300
// overhead, while the gap at high concurrency reflects the parallelism
// difference.  Both effects are intentional and documented.
// ---------------------------------------------------------------------------
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
static void BM_IoUringEchoServer(benchmark::State& state) {
    int64_t const num_conns = state.range(0);

    io_uring ring{};
    io_uring_queue_init(256, &ring, 0);

    for ([[maybe_unused]] auto _ : state) {
        int server_fd = make_server_socket();
        sockaddr_in const addr = bound_addr(server_fd);

        {
            auto* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_accept(sqe, server_fd, nullptr, nullptr, 0);
            io_uring_sqe_set_data(sqe, &g_accept_sentinel);
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

            if (ud == static_cast<void*>(&g_accept_sentinel)) {
                if (res >= 0) {
                    ++accepted;
                    auto* conn =
                        new IoUringConn{.fd = res,
                                        .rounds_left = kRoundTrips,
                                        .pending_recv = true,
                                        .buf = std::vector<char>(kMsgSize)}; // NOLINT(cppcoreguidelines-owning-memory)
                    auto* sqe = io_uring_get_sqe(&ring);
                    io_uring_prep_recv(sqe, conn->fd, conn->buf.data(), kMsgSize, MSG_WAITALL);
                    io_uring_sqe_set_data(sqe, conn);
                    if (accepted < num_conns) {
                        auto* sqe2 = io_uring_get_sqe(&ring);
                        io_uring_prep_accept(sqe2, server_fd, nullptr, nullptr, 0);
                        io_uring_sqe_set_data(sqe2, &g_accept_sentinel);
                    }
                    io_uring_submit(&ring);
                }
            } else {
                auto* conn = static_cast<IoUringConn*>(ud); // NOLINT(cppcoreguidelines-owning-memory)
                if (conn->pending_recv) {
                    if (res > 0) {
                        conn->pending_recv = false;
                        auto* sqe = io_uring_get_sqe(&ring);
                        io_uring_prep_send(sqe, conn->fd, conn->buf.data(), static_cast<std::size_t>(res),
                                           MSG_NOSIGNAL);
                        io_uring_sqe_set_data(sqe, conn);
                        io_uring_submit(&ring);
                    } else {
                        ::close(conn->fd);
                        delete conn; // NOLINT(cppcoreguidelines-owning-memory)
                        ++closed;
                    }
                } else {
                    if (--conn->rounds_left > 0) {
                        conn->pending_recv = true;
                        auto* sqe = io_uring_get_sqe(&ring);
                        io_uring_prep_recv(sqe, conn->fd, conn->buf.data(), kMsgSize, MSG_WAITALL);
                        io_uring_sqe_set_data(sqe, conn);
                        io_uring_submit(&ring);
                    } else {
                        ::close(conn->fd);
                        delete conn; // NOLINT(cppcoreguidelines-owning-memory)
                        ++closed;
                    }
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
BENCHMARK(BM_IoUringEchoServer)->Arg(1)->Arg(10)->Arg(100)->Arg(1000)->UseRealTime();

// ---------------------------------------------------------------------------
// Message-size sweep benchmarks (BM_*EchoMsgSize)
//
// Fix concurrency at kMsgSizeBenchConns (10) — the level where fiberexec shows
// the clearest latency advantage over Asio — and vary message size across 64 B,
// 512 B, 4 KB, and 64 KB. Reports bytes/second so throughput is comparable
// across sizes. The research question: does fiberexec's advantage narrow at
// larger payloads as per-op dispatch overhead becomes a smaller fraction of the
// total transfer time?
// ---------------------------------------------------------------------------
static void BM_FiberEchoMsgSize(benchmark::State& state) {
    auto const msg_size = static_cast<std::size_t>(state.range(0));
    fiberexec::context ctx{std::thread::hardware_concurrency()};
    auto sched = ctx.get_scheduler();

    for ([[maybe_unused]] auto _ : state) {
        int server_fd = make_server_socket();
        sockaddr_in const addr = bound_addr(server_fd);
        fiberexec::channel<int> ch{std::bit_ceil(static_cast<std::size_t>(kMsgSizeBenchConns) + 1)};

        std::thread srv([&] {
            stdexec::sync_wait(stdexec::when_all(
                fiberexec::run(sched,
                               [&] {
                                   for (int64_t i = 0; i < kMsgSizeBenchConns; ++i) {
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
                stdexec::bulk(stdexec::schedule(sched), stdexec::par, static_cast<std::size_t>(kMsgSizeBenchConns),
                              [&](std::size_t) {
                                  int conn{};
                                  if (ch.pop(conn) == fiberexec::channel_op_status::success) {
                                      fiber_conn_handler_dyn(conn, msg_size);
                                  }
                              })));
        });

        std::vector<std::thread> clients;
        clients.reserve(static_cast<std::size_t>(kMsgSizeBenchConns));
        for (int64_t i = 0; i < kMsgSizeBenchConns; ++i) {
            clients.emplace_back(run_blocking_client_dyn, std::cref(addr), msg_size);
        }
        for (auto& t : clients) {
            t.join();
        }
        srv.join();
        ::close(server_fd);
    }

    state.SetBytesProcessed(state.iterations() * kMsgSizeBenchConns * kRoundTrips * state.range(0));
}
BENCHMARK(BM_FiberEchoMsgSize)->Arg(64)->Arg(512)->Arg(4096)->Arg(65536)->UseRealTime();

static void BM_ThreadEchoMsgSize(benchmark::State& state) {
    auto const msg_size = static_cast<std::size_t>(state.range(0));

    for ([[maybe_unused]] auto _ : state) {
        int server_fd = make_server_socket();
        sockaddr_in const addr = bound_addr(server_fd);
        std::latch done{kMsgSizeBenchConns};

        std::thread accept_thread([server_fd, msg_size, &done] {
            for (int64_t i = 0; i < kMsgSizeBenchConns; ++i) {
                int conn = ::accept(server_fd, nullptr, nullptr);
                if (conn < 0) {
                    break;
                }
                std::thread([conn, msg_size, &done] {
                    thread_conn_handler_dyn(conn, msg_size);
                    done.count_down();
                }).detach();
            }
        });

        std::vector<std::thread> clients;
        clients.reserve(static_cast<std::size_t>(kMsgSizeBenchConns));
        for (int64_t i = 0; i < kMsgSizeBenchConns; ++i) {
            clients.emplace_back(run_blocking_client_dyn, std::cref(addr), msg_size);
        }
        for (auto& t : clients) {
            t.join();
        }
        accept_thread.join();
        done.wait();
        ::close(server_fd);
    }

    state.SetBytesProcessed(state.iterations() * kMsgSizeBenchConns * kRoundTrips * state.range(0));
}
BENCHMARK(BM_ThreadEchoMsgSize)->Arg(64)->Arg(512)->Arg(4096)->Arg(65536)->UseRealTime();

static void BM_AsioEchoMsgSize(benchmark::State& state) {
    auto const msg_size = static_cast<std::size_t>(state.range(0));
    asio::thread_pool pool{std::thread::hardware_concurrency()};

    for ([[maybe_unused]] auto _ : state) {
        asio::ip::tcp::acceptor acceptor{pool.get_executor(), {asio::ip::address_v4::loopback(), 0}};
        sockaddr_in const addr = asio_local_addr(acceptor);

        asio::co_spawn(pool.get_executor(), asio_accept_loop_dyn(&acceptor, kMsgSizeBenchConns, msg_size),
                       asio::detached);

        std::vector<std::thread> clients;
        clients.reserve(static_cast<std::size_t>(kMsgSizeBenchConns));
        for (int64_t i = 0; i < kMsgSizeBenchConns; ++i) {
            clients.emplace_back(run_blocking_client_dyn, std::cref(addr), msg_size);
        }
        for (auto& t : clients) {
            t.join();
        }
    }

    state.SetBytesProcessed(state.iterations() * kMsgSizeBenchConns * kRoundTrips * state.range(0));
}
BENCHMARK(BM_AsioEchoMsgSize)->Arg(64)->Arg(512)->Arg(4096)->Arg(65536)->UseRealTime();

static void BM_AsioExecEchoMsgSize(benchmark::State& state) {
    auto const msg_size = static_cast<std::size_t>(state.range(0));
    exec::asio::asio_thread_pool pool{static_cast<std::uint32_t>(std::thread::hardware_concurrency())};

    for ([[maybe_unused]] auto _ : state) {
        asio::ip::tcp::acceptor acceptor{pool.get_executor(), {asio::ip::address_v4::loopback(), 0}};
        sockaddr_in const addr = asio_local_addr(acceptor);

        std::thread accept_thread([&acceptor, &pool, msg_size] {
            for (int64_t i = 0; i < kMsgSizeBenchConns; ++i) {
                auto opt = stdexec::sync_wait(acceptor.async_accept(exec::asio::use_sender));
                if (!opt) {
                    break;
                }
                auto sock = std::move(std::get<0>(*opt));
                asio::co_spawn(pool.get_executor(), asio_conn_handler_dyn(std::move(sock), msg_size), asio::detached);
            }
        });

        std::vector<std::thread> clients;
        clients.reserve(static_cast<std::size_t>(kMsgSizeBenchConns));
        for (int64_t i = 0; i < kMsgSizeBenchConns; ++i) {
            clients.emplace_back(run_blocking_client_dyn, std::cref(addr), msg_size);
        }
        for (auto& t : clients) {
            t.join();
        }
        accept_thread.join();
    }

    state.SetBytesProcessed(state.iterations() * kMsgSizeBenchConns * kRoundTrips * state.range(0));
}
BENCHMARK(BM_AsioExecEchoMsgSize)->Arg(64)->Arg(512)->Arg(4096)->Arg(65536)->UseRealTime();

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
static void BM_IoUringEchoMsgSize(benchmark::State& state) {
    auto const msg_size = static_cast<std::size_t>(state.range(0));

    io_uring ring{};
    io_uring_queue_init(256, &ring, 0);

    for ([[maybe_unused]] auto _ : state) {
        int server_fd = make_server_socket();
        sockaddr_in const addr = bound_addr(server_fd);

        {
            auto* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_accept(sqe, server_fd, nullptr, nullptr, 0);
            io_uring_sqe_set_data(sqe, &g_accept_sentinel);
            io_uring_submit(&ring);
        }

        std::vector<std::thread> clients;
        clients.reserve(static_cast<std::size_t>(kMsgSizeBenchConns));
        for (int64_t i = 0; i < kMsgSizeBenchConns; ++i) {
            clients.emplace_back(run_blocking_client_dyn, std::cref(addr), msg_size);
        }

        int64_t accepted = 0;
        int64_t closed = 0;

        while (closed < kMsgSizeBenchConns) {
            io_uring_cqe* cqe = nullptr;
            io_uring_wait_cqe(&ring, &cqe);
            int const res = cqe->res;
            void* const ud = io_uring_cqe_get_data(cqe);
            io_uring_cqe_seen(&ring, cqe);

            if (ud == static_cast<void*>(&g_accept_sentinel)) {
                if (res >= 0) {
                    ++accepted;
                    auto* conn =
                        new IoUringConn{.fd = res,
                                        .rounds_left = kRoundTrips,
                                        .pending_recv = true,
                                        .buf = std::vector<char>(msg_size)}; // NOLINT(cppcoreguidelines-owning-memory)
                    auto* sqe = io_uring_get_sqe(&ring);
                    io_uring_prep_recv(sqe, conn->fd, conn->buf.data(), msg_size, MSG_WAITALL);
                    io_uring_sqe_set_data(sqe, conn);
                    if (accepted < kMsgSizeBenchConns) {
                        auto* sqe2 = io_uring_get_sqe(&ring);
                        io_uring_prep_accept(sqe2, server_fd, nullptr, nullptr, 0);
                        io_uring_sqe_set_data(sqe2, &g_accept_sentinel);
                    }
                    io_uring_submit(&ring);
                }
            } else {
                auto* conn = static_cast<IoUringConn*>(ud); // NOLINT(cppcoreguidelines-owning-memory)
                if (conn->pending_recv) {
                    if (res > 0) {
                        conn->pending_recv = false;
                        auto* sqe = io_uring_get_sqe(&ring);
                        io_uring_prep_send(sqe, conn->fd, conn->buf.data(), static_cast<std::size_t>(res),
                                           MSG_NOSIGNAL);
                        io_uring_sqe_set_data(sqe, conn);
                        io_uring_submit(&ring);
                    } else {
                        ::close(conn->fd);
                        delete conn; // NOLINT(cppcoreguidelines-owning-memory)
                        ++closed;
                    }
                } else {
                    if (--conn->rounds_left > 0) {
                        conn->pending_recv = true;
                        auto* sqe = io_uring_get_sqe(&ring);
                        io_uring_prep_recv(sqe, conn->fd, conn->buf.data(), msg_size, MSG_WAITALL);
                        io_uring_sqe_set_data(sqe, conn);
                        io_uring_submit(&ring);
                    } else {
                        ::close(conn->fd);
                        delete conn; // NOLINT(cppcoreguidelines-owning-memory)
                        ++closed;
                    }
                }
            }
        }

        for (auto& t : clients) {
            t.join();
        }
        ::close(server_fd);
    }

    state.SetBytesProcessed(state.iterations() * kMsgSizeBenchConns * kRoundTrips * state.range(0));
    io_uring_queue_exit(&ring);
}
BENCHMARK(BM_IoUringEchoMsgSize)->Arg(64)->Arg(512)->Arg(4096)->Arg(65536)->UseRealTime();

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
