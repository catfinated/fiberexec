#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <vector>

// Parallel gather via stdexec::bulk on a fiberexec scheduler.
//
// N producer threads each write a result into the write end of a socketpair.
// stdexec::bulk fans out N fibers — one per pair — that concurrently wait on
// async_recv. Because each fiber suspends cooperatively on its io_uring recv,
// all four pool threads remain free to service other fibers while waiting.
// Once every fiber has received its value the bulk sender completes and the
// aggregated sum is printed.
//
// This is the canonical "fan-out / gather" pattern: dispatch N concurrent
// async operations with a single bulk call and collect all results before
// proceeding.

int main() {
    constexpr int N = 16;

    fiberexec::context ctx{4};
    auto sched = ctx.get_scheduler();

    // Create N socketpairs.  pairs[i][0] = write end, pairs[i][1] = read end.
    std::vector<std::array<int, 2>> pairs(N);
    for (auto& p : pairs) {
        if (::socketpair(AF_UNIX, SOCK_STREAM, 0, p.data()) != 0) {
            std::cerr << "socketpair: " << std::strerror(errno) << '\n';
            return 1;
        }
    }

    // Results written by the fibers.
    std::vector<std::uint32_t> results(N, 0);

    // Producer threads: each writes one uint32_t into its socket.
    std::vector<std::thread> producers;
    producers.reserve(N);
    for (int i = 0; i < N; ++i) {
        producers.emplace_back([&pairs, i] {
            auto value = static_cast<std::uint32_t>(i * i);
            ::send(pairs.at(static_cast<std::size_t>(i)).at(0), &value, sizeof(value), MSG_NOSIGNAL);
        });
    }

    // Fan out N fiber recvs via bulk, then gather into results[].
    stdexec::sync_wait(
        stdexec::bulk(stdexec::schedule(sched), stdexec::par, static_cast<std::size_t>(N), [&](std::size_t i) {
            fiberexec::async_recv(pairs.at(i).at(1), &results.at(i), sizeof(results.at(i)), MSG_WAITALL);
        }));

    for (auto& t : producers) {
        t.join();
    }
    for (auto& [w, r] : pairs) {
        ::close(w);
        ::close(r);
    }

    std::uint32_t sum = 0;
    for (int i = 0; i < N; ++i) {
        std::cout << "result[" << i << "] = " << results.at(static_cast<std::size_t>(i)) << '\n';
        sum += results.at(static_cast<std::size_t>(i));
    }
    std::cout << "sum = " << sum << "  (expected " << (N - 1) * N * ((2 * N) - 1) / 6 << ")\n";
}
