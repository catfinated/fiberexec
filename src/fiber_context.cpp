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

namespace {

thread_local io_uring* tl_ring = nullptr; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// Parked on the suspended fiber's stack; pointer lives in sqe->user_data.
struct io_awaitable {
    boost::fibers::promise<int> promise;
};

} // namespace

// ---------------------------------------------------------------------------
// detail — I/O helpers callable from within fiber bodies
// ---------------------------------------------------------------------------

namespace detail {

io_uring* current_ring() noexcept { return tl_ring; }

int submit_and_wait(io_uring_sqe* sqe) {
    io_awaitable awaitable;
    auto future = awaitable.promise.get_future();
    io_uring_sqe_set_data(sqe, std::addressof(awaitable));
    if (int ret = io_uring_submit(tl_ring); ret < 0) {
        throw std::system_error(-ret, std::system_category(), "io_uring_submit");
    }
    return future.get(); // suspends this fiber until the CQE arrives
}

} // namespace detail

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
        uint64_t const val = 1;
        ::write(eventfd_, &val, sizeof(val));
    }

private:
    void worker(std::uint32_t thread_count) {
        boost::fibers::use_scheduling_algorithm<boost::fibers::algo::work_stealing>(thread_count);

        io_uring ring{};
        if (int res = io_uring_queue_init(k_ring_entries, &ring, 0); res < 0) {
            throw std::system_error(-res, std::system_category(), "io_uring_queue_init");
        }
        tl_ring = &ring;

        uint64_t efd_val = 0;
        auto arm_eventfd = [&] {
            io_uring_sqe* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_read(sqe, eventfd_, &efd_val, sizeof(efd_val), 0);
            io_uring_sqe_set_data64(sqe, k_eventfd_tag);
            io_uring_submit(&ring);
        };
        arm_eventfd(); // arm once before entering the loop

        while (running_.load(std::memory_order_relaxed)) {
            io_uring_cqe* cqe = nullptr;
            io_uring_wait_cqe(&ring, &cqe);
            auto const tag = io_uring_cqe_get_data64(cqe);
            int const res = cqe->res;
            io_uring_cqe_seen(&ring, cqe);

            if (tag == k_eventfd_tag) {
                if (res >= 0) {
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
                }
                arm_eventfd(); // re-arm only after consuming an eventfd CQE
            } else {
                // I/O completion — resume the suspended fiber.
                // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
                auto* awaitable = reinterpret_cast<io_awaitable*>(tag);
                awaitable->promise.set_value(res);
            }

            // Yield so the scheduler can run any fibers that became ready.
            boost::this_fiber::yield();
        }

        tl_ring = nullptr;
        io_uring_queue_exit(&ring);
    }

    void stop() {
        running_.store(false);
        // Write one token per worker so every blocked io_uring_wait_cqe returns.
        uint64_t const val = threads_.size();
        ::write(eventfd_, &val, sizeof(val));
    }

    static constexpr unsigned k_ring_entries = 256;
    static constexpr uint64_t k_eventfd_tag = 0;

    std::atomic<bool> running_;
    int eventfd_;
    std::vector<std::thread> threads_;
    std::mutex mtx_;
    std::queue<task> work_queue_;
};

// ---------------------------------------------------------------------------
// schedule_task
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
