#include <fiberexec/fiber_context.hpp>

#include <boost/fiber/all.hpp>
#include <liburing.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <queue>
#include <system_error>
#include <thread>
#include <vector>

namespace fiberexec {

// ---------------------------------------------------------------------------
// fiber_pool — owns the OS threads, fiber scheduler, and per-thread io_uring
// ---------------------------------------------------------------------------

class fiber_pool {
public:
    explicit fiber_pool(std::uint32_t thread_count)
        : running_{true} {
        threads_.reserve(thread_count);
        for (std::uint32_t i = 0; i < thread_count; ++i) {
            threads_.emplace_back([this, thread_count] { worker(thread_count); });
        }
    }

    ~fiber_pool() {
        stop();
        for (auto& t : threads_) {
            if (t.joinable()) {
                t.join();
            }
        }
    }

    fiber_pool(fiber_pool const&) = delete;
    fiber_pool& operator=(fiber_pool const&) = delete;
    fiber_pool(fiber_pool&&) = delete;
    fiber_pool& operator=(fiber_pool&&) = delete;

    void post(task work) {
        {
            std::scoped_lock lock{mtx_};
            work_queue_.push(std::move(work));
        }
        cv_.notify_one();
    }

private:
    void worker(std::uint32_t thread_count) {
        boost::fibers::use_scheduling_algorithm<boost::fibers::algo::work_stealing>(thread_count);

        io_uring ring{};
        if (int res = io_uring_queue_init(k_ring_entries, &ring, 0); res < 0) {
            throw std::system_error(-res, std::system_category(), "io_uring_queue_init");
        }
        thread_ring_ = &ring;

        while (true) {
            std::function<void()> work;

            {
                std::unique_lock lock{mtx_};
                cv_.wait(lock, [this]() { return !work_queue_.empty() || !running_; });

                if (!running_ && work_queue_.empty()) {
                    break;
                }

                if (!work_queue_.empty()) {
                    work = std::move(work_queue_.front());
                    work_queue_.pop();
                }
            }

            if (work) {
                boost::fibers::fiber(std::move(work)).detach();
            }
            // Yield so the scheduler runs the new fiber before we block
            // again on cv_.wait().
            boost::this_fiber::yield();
        }

        thread_ring_ = nullptr;
        io_uring_queue_exit(&ring);
    }

    void stop() {
        {
            std::scoped_lock lock{mtx_};
            running_ = false;
        }
        cv_.notify_all();
    }

    static constexpr unsigned k_ring_entries = 256;
    static thread_local io_uring* thread_ring_;

    std::atomic<bool> running_;
    std::vector<std::thread> threads_;

    std::mutex mtx_;
    std::condition_variable cv_;
    std::queue<task> work_queue_;
};

thread_local io_uring* fiber_pool::thread_ring_ = nullptr;

// ---------------------------------------------------------------------------
// schedule_task — non-template bridge between header and Boost.Fiber
// ---------------------------------------------------------------------------

namespace detail {

void schedule_task(fiber_pool& pool, task work) noexcept { pool.post(std::move(work)); }

} // namespace detail

// ---------------------------------------------------------------------------
// fiber_context
// ---------------------------------------------------------------------------

fiber_context::fiber_context(std::uint32_t thread_count)
    : pool_(std::make_unique<fiber_pool>(thread_count)) {}

fiber_context::~fiber_context() = default;

} // namespace fiberexec
