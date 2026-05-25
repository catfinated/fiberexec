#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

#include <boost/fiber/all.hpp>

#include <benchmark/benchmark.h>

#include <semaphore>
#include <thread>

// ---------------------------------------------------------------------------
// 1a. Fiber context switch latency
//
// Two fibers alternate boost::this_fiber::yield() on a single-thread pool.
// Each iteration contains kN round-trips (2·kN individual switches).
// SetItemsProcessed lets Google Benchmark report ns/switch alongside ns/iter.
// ---------------------------------------------------------------------------
static void BM_FiberContextSwitch(benchmark::State& state) {
    fiberexec::context ctx{1};
    auto sched = ctx.get_scheduler();

    constexpr int64_t kN = 10'000;

    for ([[maybe_unused]] auto _ : state) {
        stdexec::sync_wait(fiberexec::run(sched, [] {
            boost::fibers::fiber other{[] {
                for (int64_t i = 0; i < kN; ++i) {
                    boost::this_fiber::yield();
                }
            }};
            for (int64_t i = 0; i < kN; ++i) {
                boost::this_fiber::yield();
            }
            other.join();
        }));
    }
    state.SetItemsProcessed(state.iterations() * kN * 2);
}
// UseRealTime: fiber work runs on the pool thread, so the benchmark thread's
// CPU time is negligible (it sleeps in sync_wait). Wall time is meaningful.
BENCHMARK(BM_FiberContextSwitch)->UseRealTime();

// ---------------------------------------------------------------------------
// 1a (baseline). Thread context switch latency
//
// Two threads ping-pong via std::binary_semaphore. Each acquire blocks the
// calling thread until the other thread releases, forcing a kernel context
// switch. Same kN and SetItemsProcessed convention as BM_FiberContextSwitch
// so the ns/switch numbers are directly comparable.
// ---------------------------------------------------------------------------
static void BM_ThreadContextSwitch(benchmark::State& state) {
    constexpr int64_t kN = 10'000;

    for ([[maybe_unused]] auto _ : state) {
        std::binary_semaphore sem_a{0};
        std::binary_semaphore sem_b{0};

        std::thread other{[&] {
            for (int64_t i = 0; i < kN; ++i) {
                sem_b.acquire();
                sem_a.release();
            }
        }};

        for (int64_t i = 0; i < kN; ++i) {
            sem_b.release();
            sem_a.acquire();
        }

        other.join();
    }
    state.SetItemsProcessed(state.iterations() * kN * 2);
}
BENCHMARK(BM_ThreadContextSwitch)->UseRealTime();

// ---------------------------------------------------------------------------
// 1b. Schedule-and-run overhead (fiberexec::run)
//
// Full round-trip cost: enqueue task → pool thread picks it up → fiber switch
// → execute noop → set_value → sync_wait unblocks. This is the floor for any
// workload dispatched through run(sched, fn).
// ---------------------------------------------------------------------------
static void BM_RunNoop(benchmark::State& state) {
    fiberexec::context ctx{1};
    auto sched = ctx.get_scheduler();

    for ([[maybe_unused]] auto _ : state) {
        stdexec::sync_wait(fiberexec::run(sched, [] { benchmark::DoNotOptimize(0); }));
    }
}
BENCHMARK(BM_RunNoop)->UseRealTime();

// ---------------------------------------------------------------------------
// 1b (comparison). Schedule-and-run overhead (schedule | then)
//
// Same round-trip as BM_RunNoop but via the stdexec schedule|then path.
// Should be similar cost; any difference reflects the extra receiver machinery
// in run vs then.
// ---------------------------------------------------------------------------
static void BM_ScheduleThenNoop(benchmark::State& state) {
    fiberexec::context ctx{1};
    auto sched = ctx.get_scheduler();

    for ([[maybe_unused]] auto _ : state) {
        stdexec::sync_wait(stdexec::schedule(sched) | stdexec::then([] { benchmark::DoNotOptimize(0); }));
    }
}
BENCHMARK(BM_ScheduleThenNoop)->UseRealTime();

BENCHMARK_MAIN();
