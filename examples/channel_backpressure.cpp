#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

#include <chrono>
#include <iostream>

// Backpressure demonstration via channel.
//
// A fast producer pushes N items into a small bounded channel (capacity 4,
// usable slots 3).  A slow consumer simulates processing work with a per-item
// delay.  Because the channel fills faster than it drains, the producer fiber
// suspends cooperatively on push() whenever the channel is full — the OS
// thread stays free to run the consumer fiber during those suspensions.
//
// Observe in the output:
//   - Items 0-2 are produced immediately (filling the 3 usable slots).
//   - Item 3 onward is produced only after the consumer pops the preceding
//     item, showing the consumer driving the producer rate.
//   - Total runtime ≈ N × per-item delay, not N × 0 (producer rate).

int main() {
    using namespace std::chrono_literals;
    constexpr int kItems = 12;
    constexpr auto kDelay = 30ms;

    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    fiberexec::channel<int> ch{4}; // capacity 4 → 3 usable slots

    const auto t0 = std::chrono::steady_clock::now();
    auto ms = [&] {
        return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    };

    stdexec::sync_wait(stdexec::when_all(
        // Fast producer: pushes as quickly as the channel allows.
        fiberexec::run(sched,
                       [&] {
                           for (int i = 0; i < kItems; ++i) {
                               (void)ch.push(i);
                               std::cout << "[+" << ms() << "ms] produced " << i << '\n' << std::flush;
                           }
                           ch.close();
                       }),
        // Slow consumer: simulates work with a per-item delay.
        fiberexec::run(sched, [&] {
            int v{};
            while (ch.pop(v) == fiberexec::channel_op_status::success) {
                fiberexec::async_sleep_for(kDelay);
                std::cout << "[+" << ms() << "ms] consumed " << v << '\n' << std::flush;
            }
        })));

    std::cout << "total: " << ms() << " ms  (expected ~" << (kItems * kDelay / 1ms) << " ms)\n";
}
