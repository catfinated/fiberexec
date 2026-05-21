#include <fiberexec/fiber_context.hpp>

#include <boost/fiber/all.hpp>
#include <liburing.h>

#include <thread>
#include <vector>

namespace fiberexec {

// ---------------------------------------------------------------------------
// fiber_pool — owns the OS threads, fiber scheduler, and per-thread io_uring
// ---------------------------------------------------------------------------

class fiber_pool {
public:
    explicit fiber_pool(std::uint32_t thread_count) {
        threads_.reserve(thread_count);
        for (std::uint32_t i = 0; i < thread_count; ++i) {
            threads_.emplace_back([this] { worker(); });
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

    // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
    void post(boost::fibers::fiber fiber) {
        // TODO: submit fiber to the work-stealing scheduler
        fiber.detach();
    }

private:
    // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
    void worker() {
        // TODO: configure Boost.Fiber work-stealing scheduler on this thread,
        //       set up a per-thread io_uring instance, and drive the event loop.
        boost::this_fiber::yield();
    }

    void stop() {
        // TODO: signal all fibers and io_uring instances to drain and exit
    }

    std::vector<std::thread> threads_;
};

// ---------------------------------------------------------------------------
// schedule_task — non-template bridge between header and Boost.Fiber
// ---------------------------------------------------------------------------

namespace detail {

void schedule_task(fiber_pool& pool, std::function<void()> work) noexcept {
    pool.post(boost::fibers::fiber(std::move(work)));
}

} // namespace detail

// ---------------------------------------------------------------------------
// fiber_context
// ---------------------------------------------------------------------------

fiber_context::fiber_context(std::uint32_t thread_count)
    : pool_(std::make_unique<fiber_pool>(thread_count)) {}

fiber_context::~fiber_context() = default;

} // namespace fiberexec
