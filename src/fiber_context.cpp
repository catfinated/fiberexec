#include <fiberexec/context.hpp>

#include <boost/fiber/all.hpp>
#include <liburing.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <queue>
#include <stop_token>
#include <system_error>
#include <thread>
#include <vector>

namespace fiberexec {

namespace {

class io_uring_scheduler; // forward declaration for tl_scheduler

thread_local io_uring* tl_ring = nullptr;                // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
thread_local io_uring_scheduler* tl_scheduler = nullptr; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// Per-fiber stop token. fiber_specific_ptr gives each Boost.Fiber its own
// slot; thread_local would be wrong because multiple fibers share a thread.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
boost::fibers::fiber_specific_ptr<std::stop_token> fiber_stop_token;

// Parked on the suspended fiber's stack; pointer lives in sqe->user_data.
struct io_awaitable {
    boost::fibers::promise<int> promise;
};

// Sentinel tags that occupy values aligned heap pointers can never have.
constexpr uint64_t k_work_tag = 0;
constexpr uint64_t k_notify_tag = 1;
constexpr uint64_t k_cancel_tag = 2;

// ---------------------------------------------------------------------------
// io_uring_scheduler
//
// A per-thread Boost.Fiber scheduling algorithm whose suspend_until() blocks
// directly in io_uring_wait_cqe instead of a condition variable. This
// eliminates the separate event-loop fiber that the previous work_stealing
// approach required: the thread either runs fibers or blocks in io_uring,
// nothing else.
//
// awakened() / notify() may be called from any OS thread; the ready queue is
// protected by a mutex accordingly.
// ---------------------------------------------------------------------------
class io_uring_scheduler : public boost::fibers::algo::algorithm {
public:
    io_uring_scheduler(
        std::atomic<bool>* running, int work_efd, std::mutex* mtx, std::queue<task>* work_queue, std::size_t stack_size)
        : running_{running}
        , work_efd_{work_efd}
        , mtx_{mtx}
        , work_queue_{work_queue}
        , stack_size_{stack_size}
        , notify_fd_{::eventfd(0, EFD_SEMAPHORE | EFD_NONBLOCK | EFD_CLOEXEC)} {
        if (notify_fd_ < 0) {
            throw std::system_error(errno, std::system_category(), "eventfd (notify)");
        }
        if (int ret = io_uring_queue_init(k_ring_entries, &ring_, 0); ret < 0) {
            throw std::system_error(-ret, std::system_category(), "io_uring_queue_init");
        }
        tl_ring = &ring_;
        tl_scheduler = this;
        arm_work_efd();
        arm_notify();
    }

    ~io_uring_scheduler() override {
        tl_ring = nullptr;
        tl_scheduler = nullptr;
        io_uring_queue_exit(&ring_);
        ::close(notify_fd_);
    }

    io_uring_scheduler(io_uring_scheduler const&) = delete;
    io_uring_scheduler& operator=(io_uring_scheduler const&) = delete;
    io_uring_scheduler(io_uring_scheduler&&) = delete;
    io_uring_scheduler& operator=(io_uring_scheduler&&) = delete;

    // May be called from another OS thread (e.g. cross-thread CV notification).
    void awakened(boost::fibers::context* ctx) noexcept override {
        std::scoped_lock lk{ready_mtx_};
        ready_.push(ctx);
    }

    boost::fibers::context* pick_next() noexcept override {
        std::scoped_lock lk{ready_mtx_};
        if (ready_.empty()) {
            return nullptr;
        }
        auto* ctx = ready_.front();
        ready_.pop();
        return ctx;
    }

    bool has_ready_fibers() const noexcept override {
        std::scoped_lock lk{ready_mtx_};
        return !ready_.empty();
    }

    // Called when there are no runnable fibers. Block in io_uring until a CQE
    // arrives or abs_time is reached, then drain all available CQEs so the
    // newly runnable fibers are in the ready queue before we return.
    void suspend_until(std::chrono::steady_clock::time_point const& abs_time) noexcept override {
        // Fast path: skip the blocking call if CQEs are already available.
        io_uring_cqe* cqe = nullptr;
        if (io_uring_peek_cqe(&ring_, &cqe) != 0 || cqe == nullptr) {
            if (abs_time == std::chrono::steady_clock::time_point::max()) {
                if (io_uring_wait_cqe(&ring_, &cqe) != 0) {
                    return;
                }
            } else {
                auto const now = std::chrono::steady_clock::now();
                if (abs_time <= now) {
                    return;
                }
                auto const ns = std::chrono::duration_cast<std::chrono::nanoseconds>(abs_time - now);
                __kernel_timespec ts{};
                ts.tv_sec = ns.count() / 1'000'000'000LL;
                ts.tv_nsec = ns.count() % 1'000'000'000LL;
                if (io_uring_wait_cqe_timeout(&ring_, &cqe, &ts) != 0) {
                    return;
                }
            }
        }
        drain_cqes();
    }

    // Called from another OS thread to interrupt a blocked suspend_until().
    void notify() noexcept override {
        uint64_t val = 1;
        ::write(notify_fd_, &val, sizeof(val));
    }

    // Thread-safe. Queues a cancel request for the in-flight op whose SQE
    // user_data is user_data, then wakes the owning thread so it can submit
    // the IORING_OP_ASYNC_CANCEL SQE on the next drain pass.
    void request_cancel(void* user_data) noexcept {
        {
            std::scoped_lock lk{cancel_queue_mtx_};
            cancel_queue_.push(user_data);
        }
        uint64_t val = 1;
        ::write(notify_fd_, &val, sizeof(val));
    }

private:
    static constexpr unsigned k_ring_entries = 256;

    void arm_work_efd() {
        io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        io_uring_prep_read(sqe, work_efd_, &work_efd_val_, sizeof(work_efd_val_), 0);
        io_uring_sqe_set_data64(sqe, k_work_tag);
        io_uring_submit(&ring_);
    }

    void arm_notify() {
        io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        io_uring_prep_read(sqe, notify_fd_, &notify_val_, sizeof(notify_val_), 0);
        io_uring_sqe_set_data64(sqe, k_notify_tag);
        io_uring_submit(&ring_);
    }

    // Submit cancel SQEs for all requests queued via request_cancel(). Must be
    // called from the owning thread only; arm_notify's io_uring_submit will
    // flush these together with the re-arm SQE.
    void flush_cancel_queue() noexcept {
        std::scoped_lock lk{cancel_queue_mtx_};
        while (!cancel_queue_.empty()) {
            io_uring_sqe* csqe = io_uring_get_sqe(&ring_);
            if (csqe == nullptr) {
                break; // ring full; items remain for next wakeup
            }
            io_uring_prep_cancel(csqe, cancel_queue_.front(), 0);
            io_uring_sqe_set_data64(csqe, k_cancel_tag);
            cancel_queue_.pop();
        }
    }

    // Process every CQE currently in the ring. Creates fibers for new work,
    // resumes I/O-suspended fibers, and re-arms the watched fds.
    void drain_cqes() noexcept {
        io_uring_cqe* cqe = nullptr;
        while (io_uring_peek_cqe(&ring_, &cqe) == 0 && cqe != nullptr) {
            auto const tag = io_uring_cqe_get_data64(cqe);
            int const res = cqe->res;
            io_uring_cqe_seen(&ring_, cqe);
            cqe = nullptr;

            if (tag == k_work_tag) {
                if (res >= 0 && running_->load(std::memory_order_relaxed)) {
                    task work;
                    {
                        std::scoped_lock lk{*mtx_};
                        if (!work_queue_->empty()) {
                            work = std::move(work_queue_->front());
                            work_queue_->pop();
                        }
                    }
                    if (work) {
                        boost::fibers::fiber(std::allocator_arg, boost::fibers::fixedsize_stack{stack_size_},
                                             std::move(work))
                            .detach();
                    }
                }
                // Guard re-arm: an unconditional re-arm after the stop token
                // would consume a token meant for another thread's notify fd.
                if (running_->load(std::memory_order_acquire)) {
                    arm_work_efd();
                }
            } else if (tag == k_notify_tag) {
                flush_cancel_queue();
                arm_notify();
            } else if (tag == k_cancel_tag) {
                // Cancel op completed (res==0 found, -ENOENT not found): ignore.
            } else {
                // I/O completion — resume the suspended fiber.
                // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
                auto* awaitable = reinterpret_cast<io_awaitable*>(tag);
                awaitable->promise.set_value(res);
            }
        }
    }

    std::atomic<bool>* running_;   // non-owning — pool lifecycle flag
    int work_efd_;                 // non-owning — shared work eventfd
    std::mutex* mtx_;              // non-owning — shared work queue mutex
    std::queue<task>* work_queue_; // non-owning — shared work queue
    std::size_t stack_size_;
    int notify_fd_;
    io_uring ring_{};
    mutable std::mutex ready_mtx_;
    std::queue<boost::fibers::context*> ready_;
    uint64_t work_efd_val_{0};
    uint64_t notify_val_{0};
    std::mutex cancel_queue_mtx_;
    std::queue<void*> cancel_queue_;
};

} // namespace

// ---------------------------------------------------------------------------
// detail — I/O helpers callable from within fiber bodies
// ---------------------------------------------------------------------------

namespace detail {

io_uring* current_ring() noexcept { return tl_ring; }

void install_fiber_stop_token(std::stop_token tok) {
    // fiber_specific_ptr takes ownership of the raw pointer and deletes it
    // when the fiber exits. NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
    fiber_stop_token.reset(new std::stop_token(std::move(tok)));
}

std::stop_token current_fiber_stop_token() {
    auto* p = fiber_stop_token.get();
    return p != nullptr ? *p : std::stop_token{};
}

int submit_and_wait(io_uring_sqe* sqe, std::stop_token st) {
    io_awaitable awaitable;
    auto future = awaitable.promise.get_future();
    io_uring_sqe_set_data(sqe, std::addressof(awaitable));
    if (int ret = io_uring_submit(tl_ring); ret < 0) {
        throw std::system_error(-ret, std::system_category(), "io_uring_submit");
    }
    if (st.stop_possible()) {
        // Capture scheduler pointer now; tl_scheduler is thread-local and
        // the callback may fire from a different thread.
        auto* sched = tl_scheduler;
        std::stop_callback cb{std::move(st),
                              [sched, ud = std::addressof(awaitable)]() noexcept { sched->request_cancel(ud); }};
        return future.get(); // cb destructs here, deregistering the callback
    }
    return future.get(); // suspends this fiber until the CQE arrives
}

int submit_and_wait_with_timeout(io_uring_sqe* sqe, std::chrono::nanoseconds timeout, std::stop_token st) {
    // Chain a linked timeout: the kernel cancels sqe if it doesn't complete
    // within timeout, returning -ECANCELED for the op and -ETIME for the
    // timeout SQE (which drain_cqes discards via k_cancel_tag).
    sqe->flags |= IOSQE_IO_LINK;

    io_awaitable awaitable;
    auto future = awaitable.promise.get_future();
    io_uring_sqe_set_data(sqe, std::addressof(awaitable));

    io_uring_sqe* tsqe = io_uring_get_sqe(tl_ring);
    if (tsqe == nullptr) {
        throw std::runtime_error("submit_and_wait_with_timeout: io_uring ring full");
    }
    // ts lives on the fiber's stack; valid for the duration of the wait
    // because Boost.Fiber preserves the stack while the fiber is suspended.
    __kernel_timespec ts{};
    ts.tv_sec = timeout.count() / 1'000'000'000LL;
    ts.tv_nsec = timeout.count() % 1'000'000'000LL;
    io_uring_prep_link_timeout(tsqe, &ts, 0);
    io_uring_sqe_set_data64(tsqe, k_cancel_tag);

    if (int ret = io_uring_submit(tl_ring); ret < 0) {
        throw std::system_error(-ret, std::system_category(), "io_uring_submit");
    }
    if (st.stop_possible()) {
        auto* sched = tl_scheduler;
        std::stop_callback cb{std::move(st),
                              [sched, ud = std::addressof(awaitable)]() noexcept { sched->request_cancel(ud); }};
        return future.get();
    }
    return future.get();
}

} // namespace detail

// ---------------------------------------------------------------------------
// fiber_pool — owns the OS threads, per-thread schedulers, and shared queues
// ---------------------------------------------------------------------------

class fiber_pool {
public:
    explicit fiber_pool(std::uint32_t thread_count, std::size_t stack_size)
        : running_{true}
        , stack_size_{stack_size}
        , eventfd_{::eventfd(0, EFD_SEMAPHORE | EFD_NONBLOCK | EFD_CLOEXEC)} {
        if (eventfd_ < 0) {
            throw std::system_error(errno, std::system_category(), "eventfd");
        }
        threads_.reserve(thread_count);
        for (std::uint32_t i = 0; i < thread_count; ++i) {
            threads_.emplace_back([this] { worker(); });
        }
    }

    ~fiber_pool() {
        stop();
        for (auto& thr : threads_) {
            if (thr.joinable()) {
                thr.join();
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
    void worker() {
        boost::fibers::use_scheduling_algorithm<io_uring_scheduler>(&running_, eventfd_, &mtx_, &work_queue_,
                                                                    stack_size_);

        // Park the main fiber here. The scheduler's suspend_until() drives all
        // io_uring and fiber dispatch. stop() notifies the CV to unblock us.
        std::unique_lock<boost::fibers::mutex> lk{shutdown_mtx_};
        shutdown_cv_.wait(lk, [this] { return !running_.load(std::memory_order_relaxed); });
    }

    void stop() {
        std::unique_lock<boost::fibers::mutex> lk{shutdown_mtx_};
        running_.store(false, std::memory_order_release);
        // notify_all wakes every parked main fiber via each scheduler's notify(),
        // which writes to that thread's per-thread eventfd to interrupt io_uring.
        shutdown_cv_.notify_all();
    }

    boost::fibers::condition_variable shutdown_cv_;
    boost::fibers::mutex shutdown_mtx_;
    std::atomic<bool> running_;
    std::size_t stack_size_;
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
// context
// ---------------------------------------------------------------------------

context::context(std::uint32_t thread_count, std::size_t stack_size)
    : pool_(std::make_unique<fiber_pool>(thread_count, stack_size)) {}

context::~context() = default;

} // namespace fiberexec
