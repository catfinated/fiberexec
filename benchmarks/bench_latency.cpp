// bench_latency.cpp — per-request latency with p50/p99/p999 percentile reporting.
//
// Unlike bench_echo (throughput over many batched round-trips), each iteration
// here is a single send+recv round-trip timed individually.  The server runs
// persistently for the lifetime of the benchmark function; N client connections
// are established once before the loop and reused across all iterations in a
// round-robin pattern.
//
// UseManualTime() makes the main Google Benchmark metric the per-iteration
// latency (mean of SetIterationTime values).  p50/p99/p999 are accumulated in
// a vector and reported as custom counters (in microseconds) after the loop.
//
// MinTime(5.0) ensures enough samples for a meaningful p999 (at least
// ~100 000 iterations at loopback latencies).

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
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <latch>
#include <span>
#include <stop_token>
#include <thread>
#include <vector>

namespace asio = boost::asio;

namespace {

constexpr std::size_t kMsg = 64;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

int make_listener() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = 0;
    ::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
    ::listen(fd, 512);
    return fd;
}

sockaddr_in get_addr(int fd) {
    sockaddr_in a{};
    socklen_t len = sizeof(a);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
    return a;
}

sockaddr_in asio_addr(asio::ip::tcp::acceptor const& acc) {
    auto ep = acc.local_endpoint();
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = htons(ep.port());
    return a;
}

void report_percentiles(benchmark::State& state, std::vector<int64_t>& lat) {
    std::ranges::sort(lat);
    std::size_t const n = lat.size();
    if (n == 0) {
        return;
    }
    // Bounds guaranteed by the n==0 guard and percentile arithmetic above.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    auto const p50 = lat[n / 2];
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    auto const p99 = lat[n * 99 / 100];
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    auto const p999 = lat[std::min(n - 1, n * 999 / 1000)];
    state.counters["p50_us"] = static_cast<double>(p50) / 1000.0;
    state.counters["p99_us"] = static_cast<double>(p99) / 1000.0;
    state.counters["p999_us"] = static_cast<double>(p999) / 1000.0;
}

// Benchmark loop shared by all variants: round-robins through fds[], times
// each send+recv with steady_clock, accumulates nanosecond samples.
void run_latency_loop(benchmark::State& state, std::vector<int> const& fds, std::vector<int64_t>& lat) {
    std::array<char, kMsg> buf{};
    std::size_t idx = 0;
    std::size_t const n = fds.size();

    for ([[maybe_unused]] auto _ : state) {
        int fd = fds[idx++ % n]; // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        auto t0 = std::chrono::steady_clock::now();
        ::send(fd, buf.data(), kMsg, MSG_NOSIGNAL);
        ::recv(fd, buf.data(), kMsg, MSG_WAITALL);
        auto t1 = std::chrono::steady_clock::now();
        int64_t const ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
        lat.push_back(ns);
        state.SetIterationTime(static_cast<double>(ns) * 1e-9);
    }
}

// Connect num_conns clients to addr; spin until the server has accepted them
// all (signalled via n_accepted atomic).
std::vector<int> connect_clients(sockaddr_in const& addr, int64_t num_conns, std::atomic<int> const& n_accepted) {
    std::vector<int> fds(static_cast<std::size_t>(num_conns));
    for (auto& fd : fds) {
        fd = ::socket(AF_INET, SOCK_STREAM, 0);
        ::connect(fd, reinterpret_cast<sockaddr const*>(&addr), sizeof(addr));
    }
    while (n_accepted.load(std::memory_order_acquire) < static_cast<int>(num_conns)) {
        std::this_thread::yield();
    }
    return fds;
}

// ---------------------------------------------------------------------------
// io_uring state for the persistent baseline event loop
// ---------------------------------------------------------------------------

struct LatencyConn {
    int fd{};
    bool pending_recv{};
    std::array<char, kMsg> buf{};
};
// Address used as a CQE tag to distinguish accept completions from connection
// completions. The value is never read; only its address matters.
int g_lat_accept_sentinel{}; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// ---------------------------------------------------------------------------
// fiber_echo_latency_impl
//
// Shared implementation for all fiberexec latency variants.  num_threads
// controls the pool size:
//   hardware_concurrency() — production configuration (BM_FiberEchoLatency)
//   1                      — single thread, isolates multi-thread effects
//                            (BM_FiberEchoLatency1T)
//   num_conns              — one thread per fiber, perfect 1:1 balance
//                            (BM_FiberEchoLatencyNT)
//
// Server uses the public API: fiberexec::run for the accept loop,
// fiberexec::channel<int> for the connection queue, stdexec::bulk for the
// worker pool.  sync_wait runs in a background thread so the benchmark loop
// can drive blocking send/recv on the main thread without starving the server.
// ---------------------------------------------------------------------------
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
void fiber_echo_latency_impl(benchmark::State& state, unsigned num_threads) {
    int64_t const num_conns = state.range(0);

    int listener = make_listener();
    sockaddr_in const addr = get_addr(listener);

    fiberexec::context ctx{num_threads};
    auto sched = ctx.get_scheduler();
    fiberexec::channel<int> ch{std::bit_ceil(static_cast<std::size_t>(num_conns) + 1)};
    std::stop_source ss;
    std::atomic<int> n_accepted{0};

    std::thread srv([&] {
        stdexec::sync_wait(stdexec::when_all(
            fiberexec::run(sched,
                           [&] {
                               try {
                                   while (true) {
                                       int fd = fiberexec::async_accept(listener, nullptr, nullptr, std::nullopt,
                                                                        ss.get_token());
                                       n_accepted.fetch_add(1, std::memory_order_release);
                                       if (ch.push(fd) != fiberexec::channel_op_status::success) {
                                           fiberexec::async_close(fd);
                                       }
                                   }
                               } catch (std::system_error const& e) {
                                   if (e.code().value() != ECANCELED) {
                                       throw;
                                   }
                               }
                               ch.close();
                           }),
            stdexec::bulk(
                stdexec::schedule(sched), stdexec::par, static_cast<std::size_t>(num_conns), [&](std::size_t) {
                    int fd{};
                    while (ch.pop(fd) == fiberexec::channel_op_status::success) {
                        std::array<char, kMsg> buf{};
                        try {
                            while (true) {
                                ssize_t n =
                                    fiberexec::async_recv(fd, std::as_writable_bytes(std::span{buf}), MSG_WAITALL);
                                if (n <= 0) {
                                    break;
                                }
                                fiberexec::async_send(fd,
                                                      std::as_bytes(std::span{buf.data(), static_cast<std::size_t>(n)}),
                                                      MSG_NOSIGNAL);
                            }
                        } catch (...) {
                        }
                        fiberexec::async_close(fd);
                    }
                })));
    });

    std::vector<int> fds = connect_clients(addr, num_conns, n_accepted);
    std::vector<int64_t> lat;
    lat.reserve(1 << 20);

    run_latency_loop(state, fds, lat);

    ss.request_stop();
    for (auto fd : fds) {
        ::close(fd);
    }
    srv.join();
    ::close(listener);

    report_percentiles(state, lat);
}

// Production configuration: hardware_concurrency() pool threads.
void BM_FiberEchoLatency(benchmark::State& state) {
    fiber_echo_latency_impl(state, std::thread::hardware_concurrency());
}
BENCHMARK(BM_FiberEchoLatency)->Arg(1)->Arg(10)->Arg(100)->UseManualTime()->MinTime(5.0);

// Diagnostic: single pool thread.  If p999 stays flat across N=1/10/100 and
// matches the raw io_uring baseline, multi-thread work distribution is the
// cause of the rising tail.  If p999 still rises, the cause is intrinsic to
// managing more fibers in Boost.Fiber's scheduler.
void BM_FiberEchoLatency1T(benchmark::State& state) { fiber_echo_latency_impl(state, 1); }
BENCHMARK(BM_FiberEchoLatency1T)->Arg(1)->Arg(10)->Arg(100)->UseManualTime()->MinTime(5.0);

// Diagnostic: one pool thread per fiber (perfect 1:1 balance, no thread shares
// more than one connection).  N=100 is omitted — 100 OS threads is not a useful
// operating point, only a diagnostic.  If p999 at N=10 drops toward the 1T or
// io_uring baseline, uneven fiber-to-thread distribution is the cause.
void BM_FiberEchoLatencyNT(benchmark::State& state) {
    auto const num_conns = static_cast<unsigned>(state.range(0));
    fiber_echo_latency_impl(state, num_conns);
}
BENCHMARK(BM_FiberEchoLatencyNT)->Arg(1)->Arg(10)->UseManualTime()->MinTime(5.0);

// ---------------------------------------------------------------------------
// BM_ThreadEchoLatency
//
// Thread-per-connection server.  Each accepted connection gets a dedicated OS
// thread that loops recv/send until EOF.
// ---------------------------------------------------------------------------
void BM_ThreadEchoLatency(benchmark::State& state) {
    int64_t const num_conns = state.range(0);

    int listener = make_listener();
    sockaddr_in const addr = get_addr(listener);

    std::latch done{num_conns};
    std::atomic<int> n_accepted{0};

    std::thread acceptor([&] {
        for (int64_t i = 0; i < num_conns; ++i) {
            int conn = ::accept(listener, nullptr, nullptr);
            if (conn < 0) {
                break;
            }
            n_accepted.fetch_add(1, std::memory_order_release);
            std::thread([conn, &done] {
                std::array<char, kMsg> buf{};
                while (true) {
                    ssize_t n = ::recv(conn, buf.data(), kMsg, MSG_WAITALL);
                    if (n <= 0) {
                        break;
                    }
                    ::send(conn, buf.data(), static_cast<std::size_t>(n), MSG_NOSIGNAL);
                }
                ::close(conn);
                done.count_down();
            }).detach();
        }
    });

    std::vector<int> fds = connect_clients(addr, num_conns, n_accepted);
    std::vector<int64_t> lat;
    lat.reserve(1 << 20);

    run_latency_loop(state, fds, lat);

    for (auto fd : fds) {
        ::close(fd);
    }
    acceptor.join();
    done.wait();
    ::close(listener);

    report_percentiles(state, lat);
}
BENCHMARK(BM_ThreadEchoLatency)->Arg(1)->Arg(10)->Arg(100)->UseManualTime()->MinTime(5.0);

// ---------------------------------------------------------------------------
// BM_AsioEchoLatency
//
// Asio coroutine echo server.  Each accepted socket is handed to a detached
// coroutine that loops async_read/async_write until EOF.
// ---------------------------------------------------------------------------

asio::awaitable<void> asio_lat_handler(asio::ip::tcp::socket sock, std::latch* done) {
    std::array<char, kMsg> buf{};
    try {
        // NOLINTNEXTLINE(clang-analyzer-core.NullDereference)
        while (true) {
            auto [ec, n] = co_await asio::async_read(sock, asio::buffer(buf), asio::transfer_exactly(kMsg),
                                                     asio::as_tuple(asio::use_awaitable));
            if (ec || n == 0) {
                break;
            }
            co_await asio::async_write(sock, asio::buffer(buf, n), asio::use_awaitable);
        }
    } catch (...) {
    }
    done->count_down();
}

asio::awaitable<void>
asio_lat_accept_loop(asio::ip::tcp::acceptor* acc, int64_t num_conns, std::latch* done, std::atomic<int>* n_accepted) {
    for (int64_t i = 0; i < num_conns; ++i) {
        auto [ec, sock] = co_await acc->async_accept(asio::as_tuple(asio::use_awaitable));
        if (ec) {
            break;
        }
        n_accepted->fetch_add(1, std::memory_order_release);
        asio::co_spawn(acc->get_executor(), asio_lat_handler(std::move(sock), done), asio::detached);
    }
}

void BM_AsioEchoLatency(benchmark::State& state) {
    int64_t const num_conns = state.range(0);

    asio::thread_pool pool{std::thread::hardware_concurrency()};
    asio::ip::tcp::acceptor acceptor{pool.get_executor(), {asio::ip::address_v4::loopback(), 0}};
    sockaddr_in const addr = asio_addr(acceptor);

    std::latch done{num_conns};
    std::atomic<int> n_accepted{0};

    asio::co_spawn(pool.get_executor(), asio_lat_accept_loop(&acceptor, num_conns, &done, &n_accepted), asio::detached);

    std::vector<int> fds = connect_clients(addr, num_conns, n_accepted);
    std::vector<int64_t> lat;
    lat.reserve(1 << 20);

    run_latency_loop(state, fds, lat);

    for (auto fd : fds) {
        ::close(fd);
    }
    done.wait();

    report_percentiles(state, lat);
}
BENCHMARK(BM_AsioEchoLatency)->Arg(1)->Arg(10)->Arg(100)->UseManualTime()->MinTime(5.0);

// ---------------------------------------------------------------------------
// BM_IoUringEchoLatency
//
// Raw io_uring event loop (no fibers, no P2300) run in a background thread.
// Connections are persistent: the state machine loops recv→send until the
// client closes the connection.  Exits when all num_conns connections have
// closed.
// ---------------------------------------------------------------------------
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
void BM_IoUringEchoLatency(benchmark::State& state) {
    int64_t const num_conns = state.range(0);

    int listener = make_listener();
    sockaddr_in const addr = get_addr(listener);

    std::atomic<int> n_accepted{0};

    // Event loop runs in a background thread so the benchmark loop (on the
    // main thread) can do blocking send/recv without starving the event loop.
    std::thread event_loop_thread([&] {
        io_uring ring{};
        io_uring_queue_init(256, &ring, 0);

        {
            auto* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_accept(sqe, listener, nullptr, nullptr, 0);
            io_uring_sqe_set_data(sqe, &g_lat_accept_sentinel);
            io_uring_submit(&ring);
        }

        int64_t accepted = 0;
        int64_t closed = 0;

        while (closed < num_conns) {
            io_uring_cqe* cqe = nullptr;
            io_uring_wait_cqe(&ring, &cqe);
            int const res = cqe->res;
            void* const ud = io_uring_cqe_get_data(cqe);
            io_uring_cqe_seen(&ring, cqe);

            if (ud == static_cast<void*>(&g_lat_accept_sentinel)) {
                if (res >= 0) {
                    ++accepted;
                    n_accepted.fetch_add(1, std::memory_order_release);
                    // NOLINTNEXTLINE(cppcoreguidelines-owning-memory): lifetime tied to SQE
                    auto* conn = new LatencyConn{.fd = res, .pending_recv = true, .buf = {}};
                    auto* sqe = io_uring_get_sqe(&ring);
                    io_uring_prep_recv(sqe, conn->fd, conn->buf.data(), kMsg, MSG_WAITALL);
                    io_uring_sqe_set_data(sqe, conn);
                    if (accepted < num_conns) {
                        auto* sqe2 = io_uring_get_sqe(&ring);
                        io_uring_prep_accept(sqe2, listener, nullptr, nullptr, 0);
                        io_uring_sqe_set_data(sqe2, &g_lat_accept_sentinel);
                    }
                    io_uring_submit(&ring);
                }
            } else {
                auto* conn = static_cast<LatencyConn*>(ud); // NOLINT(cppcoreguidelines-owning-memory)
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
                    conn->pending_recv = true;
                    auto* sqe = io_uring_get_sqe(&ring);
                    io_uring_prep_recv(sqe, conn->fd, conn->buf.data(), kMsg, MSG_WAITALL);
                    io_uring_sqe_set_data(sqe, conn);
                    io_uring_submit(&ring);
                }
            }
        }

        io_uring_queue_exit(&ring);
    });

    std::vector<int> fds = connect_clients(addr, num_conns, n_accepted);
    std::vector<int64_t> lat;
    lat.reserve(1 << 20);

    run_latency_loop(state, fds, lat);

    for (auto fd : fds) {
        ::close(fd);
    }
    event_loop_thread.join();
    ::close(listener);

    report_percentiles(state, lat);
}
BENCHMARK(BM_IoUringEchoLatency)->Arg(1)->Arg(10)->Arg(100)->UseManualTime()->MinTime(5.0);

} // namespace

BENCHMARK_MAIN();
