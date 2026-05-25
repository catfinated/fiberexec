#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstring>
#include <iostream>
#include <numeric>

// Fan-out and gather from inside a fiber using fiberexec::sync_wait.
//
// A collector fiber fans out four concurrent async_read operations using
// fiberexec::sync_wait(when_all(...)). Only the calling fiber suspends — the
// OS thread stays free to run the four producer fibers during the wait.
//
// fiberexec::sync_wait is the fiber-aware counterpart to stdexec::sync_wait.
// Calling stdexec::sync_wait from inside a fiber would block the OS thread
// and starve every other fiber sharing it. fiberexec::sync_wait suspends only
// the calling fiber and returns control to the scheduler.
//
// Observe in the output:
//   - All four reads are submitted before any write completes.
//   - Results arrive out of order (driven by each producer's delay).
//   - Collected results are in original order regardless of arrival order.
//   - Total elapsed time ≈ max(delay) = 40 ms, not sum(delays) = 100 ms.

using namespace std::chrono_literals;

int main() {
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    constexpr int kN = 4;
    constexpr std::array<std::chrono::milliseconds, kN> kDelays{40ms, 10ms, 30ms, 20ms};
    constexpr std::array<int, kN> kValues{100, 200, 300, 400};

    std::array<std::array<int, 2>, kN> pairs{};
    for (auto& p : pairs) {
        if (::socketpair(AF_UNIX, SOCK_STREAM, 0, p.data()) != 0) {
            std::cerr << "socketpair: " << std::strerror(errno) << '\n';
            return 1;
        }
    }

    const auto t0 = std::chrono::steady_clock::now();
    auto ms = [&] {
        return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    };

    stdexec::sync_wait(stdexec::when_all(
        // Four producer fibers — each sleeps for a different delay then writes.
        fiberexec::run(sched,
                       [&] {
                           fiberexec::async_sleep_for(kDelays.at(0));
                           fiberexec::async_write(pairs.at(0).at(1), &kValues.at(0), sizeof(int));
                           std::cout << "[+" << ms() << "ms] wrote " << kValues.at(0) << '\n' << std::flush;
                       }),
        fiberexec::run(sched,
                       [&] {
                           fiberexec::async_sleep_for(kDelays.at(1));
                           fiberexec::async_write(pairs.at(1).at(1), &kValues.at(1), sizeof(int));
                           std::cout << "[+" << ms() << "ms] wrote " << kValues.at(1) << '\n' << std::flush;
                       }),
        fiberexec::run(sched,
                       [&] {
                           fiberexec::async_sleep_for(kDelays.at(2));
                           fiberexec::async_write(pairs.at(2).at(1), &kValues.at(2), sizeof(int));
                           std::cout << "[+" << ms() << "ms] wrote " << kValues.at(2) << '\n' << std::flush;
                       }),
        fiberexec::run(sched,
                       [&] {
                           fiberexec::async_sleep_for(kDelays.at(3));
                           fiberexec::async_write(pairs.at(3).at(1), &kValues.at(3), sizeof(int));
                           std::cout << "[+" << ms() << "ms] wrote " << kValues.at(3) << '\n' << std::flush;
                       }),
        // Collector fiber: sequential setup, then fan-out via fiberexec::sync_wait.
        fiberexec::run(sched, [&] {
            std::cout << "[+" << ms() << "ms] collector: submitting reads\n" << std::flush;

            // All four reads are submitted simultaneously. This fiber suspends;
            // the OS thread is free to run the producer fibers while we wait.
            auto [a, b, c, d] = *fiberexec::sync_wait(
                stdexec::when_all(fiberexec::run(sched,
                                                 [&] {
                                                     int v{};
                                                     fiberexec::async_read(pairs.at(0).at(0), &v, sizeof(v));
                                                     return v;
                                                 }),
                                  fiberexec::run(sched,
                                                 [&] {
                                                     int v{};
                                                     fiberexec::async_read(pairs.at(1).at(0), &v, sizeof(v));
                                                     return v;
                                                 }),
                                  fiberexec::run(sched,
                                                 [&] {
                                                     int v{};
                                                     fiberexec::async_read(pairs.at(2).at(0), &v, sizeof(v));
                                                     return v;
                                                 }),
                                  fiberexec::run(sched, [&] {
                                      int v{};
                                      fiberexec::async_read(pairs.at(3).at(0), &v, sizeof(v));
                                      return v;
                                  })));

            std::cout << "[+" << ms() << "ms] collected: " << a << ' ' << b << ' ' << c << ' ' << d << '\n';
            std::cout << "sum = " << (a + b + c + d) << "  (expected "
                      << std::accumulate(kValues.begin(), kValues.end(), 0) << ")\n";
        })));

    for (auto& [read_fd, write_fd] : pairs) {
        ::close(read_fd);
        ::close(write_fd);
    }
}
