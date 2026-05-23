#include <fiberexec/fiber_context.hpp>

#include <boost/fiber/all.hpp>
#include <liburing.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <atomic>
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
        : running_{true}
        , eventfd_{::eventfd(0, EFD_SEMAPHORE | EFD_NONBLOCK | EFD_CLOEXEC)} {
        if (eventfd_ < 0) {
            throw std::system_error(errno, std::system_category(), "eventfd");
        }
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
        ::close(eventfd_);
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
        const uint64_t val = 1;
        ::write(eventfd_, &val, sizeof(val));
    }

private:
    void worker(std::uint32_t thread_count) {
        boost::fibers::use_scheduling_algorithm<boost::fibers::algo::work_stealing>(thread_count);

        io_uring ring{};
        if (int res = io_uring_queue_init(k_ring_entries, &ring, 0); res < 0) {
            throw std::system_error(-res, std::system_category(), "io_uring_queue_init");
        }
        thread_ring_ = &ring;

        while (running_.load(std::memory_order_relaxed)) {
            // Arm an async read on the pool eventfd. With EFD_SEMAPHORE each
            // write(1) allows exactly one read to complete, so exactly one
            // worker wakes per post().
            uint64_t efd_val = 0;
            io_uring_sqe* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_read(sqe, eventfd_, &efd_val, sizeof(efd_val), 0);
            sqe->user_data = k_eventfd_tag;
            io_uring_submit(&ring);

            io_uring_cqe* cqe = nullptr;
            io_uring_wait_cqe(&ring, &cqe);
            const int res = cqe->res;
            io_uring_cqe_seen(&ring, cqe);

            if (res < 0) {
                // Unexpected error on the eventfd read — skip dispatch.
                continue;
            }

            task work;
            {
                std::scoped_lock lock{mtx_};
                if (!work_queue_.empty()) {
                    work = std::move(work_queue_.front());
                    work_queue_.pop();
                }
            }

            if (work) {
                boost::fibers::fiber(std::move(work)).detach();
            }

            // Yield so the scheduler runs any newly launched fiber before
            // we block again in io_uring_wait_cqe.
            boost::this_fiber::yield();
        }

        thread_ring_ = nullptr;
        io_uring_queue_exit(&ring);
    }

    void stop() {
        running_.store(false);
        // Write one token per worker thread so every blocked io_uring_wait_cqe
        // returns and each worker observes running_ = false.
        const uint64_t val = threads_.size();
        ::write(eventfd_, &val, sizeof(val));
    }

    static constexpr unsigned k_ring_entries = 256;
    static constexpr uint64_t k_eventfd_tag = 0;
    static thread_local io_uring* thread_ring_;

    std::atomic<bool> running_;
    int eventfd_;
    std::vector<std::thread> threads_;
    std::mutex mtx_;
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
