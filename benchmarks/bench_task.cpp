// bench_task.cpp — fiberexec::task vs std::function<void()>
//
// Both types are benchmarked in the same binary so that
// --benchmark_enable_random_interleaving can interleave repetitions and cancel
// out temporal drift (CPU frequency changes, cache warming, scheduler jitter).
//
// Two closure sizes are tested:
//
//   Small  (8 bytes, one pointer)  — both implementations use SBO inline storage.
//   Medium (24 bytes, std::array<int,6>) — std::function heap-allocates on most
//     x86-64 implementations whose internal buffer is ~16 bytes; fiberexec::task
//     fits it inline in its 64-byte buffer.
//
// Two operations are timed:
//   Construct  — wrapping a lambda in the callable wrapper.
//   Move+Call  — construct, move once (models enqueue→dispatch), then invoke.

#include <fiberexec/task.hpp>

#include <benchmark/benchmark.h>

#include <array>
#include <functional>

namespace {

// ---------------------------------------------------------------------------
// Small closure — 8 bytes (one reference capture, stored as a pointer)
// ---------------------------------------------------------------------------

void BM_StdFunction_Construct_Small(benchmark::State& state) {
    int x = 0;
    for ([[maybe_unused]] auto _ : state) {
        std::function<void()> f{[&x] { benchmark::DoNotOptimize(x++); }};
        benchmark::DoNotOptimize(f);
    }
}
BENCHMARK(BM_StdFunction_Construct_Small);

void BM_Task_Construct_Small(benchmark::State& state) {
    int x = 0;
    for ([[maybe_unused]] auto _ : state) {
        fiberexec::task f{[&x] { benchmark::DoNotOptimize(x++); }};
        benchmark::DoNotOptimize(f);
    }
}
BENCHMARK(BM_Task_Construct_Small);

void BM_StdFunction_MoveAndCall_Small(benchmark::State& state) {
    int x = 0;
    for ([[maybe_unused]] auto _ : state) {
        std::function<void()> f{[&x] { benchmark::DoNotOptimize(x++); }};
        auto g = std::move(f);
        g();
    }
}
BENCHMARK(BM_StdFunction_MoveAndCall_Small);

void BM_Task_MoveAndCall_Small(benchmark::State& state) {
    int x = 0;
    for ([[maybe_unused]] auto _ : state) {
        fiberexec::task f{[&x] { benchmark::DoNotOptimize(x++); }};
        auto g = std::move(f);
        g();
    }
}
BENCHMARK(BM_Task_MoveAndCall_Small);

// ---------------------------------------------------------------------------
// Medium closure — 24 bytes (std::array<int,6> captured by value)
// Exceeds std::function's ~16-byte SBO buffer; fits in task's 64-byte buffer.
// ---------------------------------------------------------------------------

void BM_StdFunction_Construct_Medium(benchmark::State& state) {
    std::array<int, 6> data{1, 2, 3, 4, 5, 6};
    for ([[maybe_unused]] auto _ : state) {
        std::function<void()> f{[data]() mutable { benchmark::DoNotOptimize(data); }};
        benchmark::DoNotOptimize(f);
    }
}
BENCHMARK(BM_StdFunction_Construct_Medium);

void BM_Task_Construct_Medium(benchmark::State& state) {
    std::array<int, 6> data{1, 2, 3, 4, 5, 6};
    for ([[maybe_unused]] auto _ : state) {
        fiberexec::task f{[data]() mutable { benchmark::DoNotOptimize(data); }};
        benchmark::DoNotOptimize(f);
    }
}
BENCHMARK(BM_Task_Construct_Medium);

void BM_StdFunction_MoveAndCall_Medium(benchmark::State& state) {
    std::array<int, 6> data{1, 2, 3, 4, 5, 6};
    for ([[maybe_unused]] auto _ : state) {
        std::function<void()> f{[data]() mutable { benchmark::DoNotOptimize(data); }};
        auto g = std::move(f);
        g();
    }
}
BENCHMARK(BM_StdFunction_MoveAndCall_Medium);

void BM_Task_MoveAndCall_Medium(benchmark::State& state) {
    std::array<int, 6> data{1, 2, 3, 4, 5, 6};
    for ([[maybe_unused]] auto _ : state) {
        fiberexec::task f{[data]() mutable { benchmark::DoNotOptimize(data); }};
        auto g = std::move(f);
        g();
    }
}
BENCHMARK(BM_Task_MoveAndCall_Medium);

} // namespace

BENCHMARK_MAIN();
