#include <fiberexec/fiberexec.hpp>

#include <exec/static_thread_pool.hpp>
#include <stdexec/execution.hpp>

#include <benchmark/benchmark.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

// ---------------------------------------------------------------------------
// Setup helpers
// ---------------------------------------------------------------------------

static std::vector<std::array<int, 2>> make_pairs(int n) {
    std::vector<std::array<int, 2>> pairs(n);
    for (auto& p : pairs) {
        if (::socketpair(AF_UNIX, SOCK_STREAM, 0, p.data()) != 0) {
            throw std::runtime_error(std::string("socketpair: ") + std::strerror(errno));
        }
    }
    return pairs;
}

static void close_pairs(std::vector<std::array<int, 2>>& pairs) {
    for (auto& p : pairs) {
        ::close(p[0]);
        ::close(p[1]);
    }
}

// Write one byte into every write end to make all read ends immediately readable.
static void fill_pairs(const std::vector<std::array<int, 2>>& pairs) {
    char b = 'x';
    for (const auto& p : pairs) {
        if (::write(p[0], &b, 1) != 1) {
            throw std::runtime_error("fill_pairs: write failed");
        }
    }
}

// ---------------------------------------------------------------------------
// 3a. Fan-out: fiberexec scheduler vs exec::static_thread_pool, both via
//     stdexec::bulk with stdexec::par.  The algorithm is identical; only the
//     scheduler changes.  fiberexec::fiber_domain::transform_sender intercepts
//     the bulk_chunked sender and dispatches N concurrent fibers via
//     detail::schedule_task so work is actually distributed across pool threads.
// ---------------------------------------------------------------------------

static void BM_FiberFanOut(benchmark::State& state) {
    const int n = static_cast<int>(state.range(0));
    fiberexec::context ctx{4};
    auto sched = ctx.get_scheduler();

    auto pairs = make_pairs(n);

    for ([[maybe_unused]] auto _ : state) {
        fill_pairs(pairs);

        stdexec::sync_wait(
            stdexec::bulk(stdexec::schedule(sched), stdexec::par, static_cast<std::size_t>(n), [&](std::size_t i) {
                char buf;
                fiberexec::async_recv(pairs[i][1], &buf, 1);
                benchmark::DoNotOptimize(buf);
            }));
    }

    close_pairs(pairs);
    state.SetItemsProcessed(state.iterations() * n);
}
BENCHMARK(BM_FiberFanOut)->Arg(2)->Arg(8)->Arg(32)->Arg(128)->UseRealTime();

static void BM_ThreadFanOut(benchmark::State& state) {
    const int n = static_cast<int>(state.range(0));
    exec::static_thread_pool pool{4};
    auto sched = pool.get_scheduler();

    auto pairs = make_pairs(n);

    for ([[maybe_unused]] auto _ : state) {
        fill_pairs(pairs);

        stdexec::sync_wait(
            stdexec::bulk(stdexec::schedule(sched), stdexec::par, static_cast<std::size_t>(n), [&](std::size_t i) {
                char buf;
                if (::read(pairs[i][1], &buf, 1) != 1) {
                    throw std::runtime_error("BM_ThreadFanOut: read failed");
                }
                benchmark::DoNotOptimize(buf);
            }));
    }

    close_pairs(pairs);
    state.SetItemsProcessed(state.iterations() * n);
}
BENCHMARK(BM_ThreadFanOut)->Arg(2)->Arg(8)->Arg(32)->Arg(128)->UseRealTime();

BENCHMARK_MAIN();
