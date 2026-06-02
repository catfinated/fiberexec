#include <fiberexec/context.hpp>

#include <fiberexec/detail/cqe_handler.hpp>
#include <fiberexec/fixed_buffer_pool.hpp>
#include <fiberexec/fixed_fd_table.hpp>

#include <boost/fiber/all.hpp>
#include <liburing.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <queue>
#include <span>
#include <stop_token>
#include <system_error>
#include <thread>
#include <vector>

namespace fiberexec {

namespace {

class io_uring_scheduler; // forward declaration for tl_scheduler

// NOLINTBEGIN(cppcoreguidelines-avoid-non-const-global-variables)
thread_local io_uring* tl_ring = nullptr;
thread_local io_uring_scheduler* tl_scheduler = nullptr;
thread_local fixed_buffer_pool* tl_fixed_buffer_pool = nullptr;
thread_local fixed_fd_table* tl_fixed_fd_table = nullptr;
// Per-fiber stop token. fiber_specific_ptr gives each Boost.Fiber its own
// slot; thread_local would be wrong because multiple fibers share a thread.
boost::fibers::fiber_specific_ptr<std::stop_token> fiber_stop_token;
// NOLINTEND(cppcoreguidelines-avoid-non-const-global-variables)

// Parked on the suspended fiber's stack; pointer (upcast to cqe_handler*)
// lives in sqe->user_data. complete() delivers the CQE result to the fiber.
struct io_awaitable : detail::cqe_handler {
    void complete(io_uring* /*ring*/, int res, uint32_t /*flags*/) noexcept override { promise.set_value(res); }
    boost::fibers::promise<int> promise;
};

// Sentinel tags that occupy values aligned heap pointers can never have.
constexpr uint64_t k_work_tag = 0;
constexpr uint64_t k_notify_tag = 1;
constexpr uint64_t k_cancel_tag = 2;

} // namespace

// ---------------------------------------------------------------------------
// fiber_pool — owns the OS threads, per-thread schedulers, and shared queues.
// Defined here (in fiberexec namespace, matching the header forward declaration)
// so io_uring_scheduler can hold a fiber_pool* and access its members directly.
// worker() is defined after io_uring_scheduler because it instantiates it as
// a template argument.
// ---------------------------------------------------------------------------
class fiber_pool {
public:
    explicit fiber_pool(context_options const& opts)
        : running_{true}
        , opts_{opts}
        , eventfd_{::eventfd(0, EFD_SEMAPHORE | EFD_NONBLOCK | EFD_CLOEXEC)} {
        if (eventfd_ < 0) {
            throw std::system_error(errno, std::system_category(), "eventfd");
        }
        threads_.reserve(opts_.thread_count);
        for (std::uint32_t i = 0; i < opts_.thread_count; ++i) {
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
    friend class io_uring_scheduler;

    void worker(); // defined after io_uring_scheduler is complete

    void stop() {
        pool_stop_source_.request_stop();
        std::unique_lock<boost::fibers::mutex> lk{shutdown_mtx_};
        running_.store(false, std::memory_order_release);
        // notify_all wakes every parked main fiber via each scheduler's notify(),
        // which writes to that thread's per-thread eventfd to interrupt io_uring.
        shutdown_cv_.notify_all();
    }

    boost::fibers::condition_variable shutdown_cv_;
    boost::fibers::mutex shutdown_mtx_;
    std::atomic<bool> running_;
    context_options opts_;
    int eventfd_;
    std::atomic<std::size_t> in_flight_{0};
    std::stop_source pool_stop_source_;
    std::vector<std::thread> threads_;
    std::mutex mtx_;
    std::queue<task> work_queue_;
};

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
namespace {

class io_uring_scheduler : public boost::fibers::algo::algorithm {
public:
    explicit io_uring_scheduler(fiber_pool* pool)
        : pool_{pool}
        , notify_fd_{::eventfd(0, EFD_SEMAPHORE | EFD_NONBLOCK | EFD_CLOEXEC)} {
        if (notify_fd_ < 0) {
            throw std::system_error(errno, std::system_category(), "eventfd (notify)");
        }
        // Prefer the three-flag combination (kernel 6.1+):
        //   SINGLE_ISSUER   — only this thread submits; allows internal kernel optimisations
        //                     and is required by DEFER_TASKRUN.
        //   COOP_TASKRUN    — suppress involuntary task_work delivery; completions are
        //                     processed cooperatively when the thread calls into io_uring.
        //   DEFER_TASKRUN   — further defer task_work until io_uring_enter is called with
        //                     IORING_ENTER_GETEVENTS (i.e. io_uring_wait_cqe*), reducing
        //                     context switches on the completion path.
        // Fall back to plain init on kernels that pre-date these flags.
        io_uring_params params{};
        params.flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_COOP_TASKRUN | IORING_SETUP_DEFER_TASKRUN;
        int ret = io_uring_queue_init_params(k_ring_entries, &ring_, &params);
        if (ret == -EINVAL) {
            ret = io_uring_queue_init(k_ring_entries, &ring_, 0);
        }
        if (ret < 0) {
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

    std::stop_token pool_stop_token() const noexcept { return pool_->pool_stop_source_.get_token(); }

private:
    static constexpr unsigned k_ring_entries = 256;

    void arm_work_efd() {
        io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        io_uring_prep_read(sqe, pool_->eventfd_, &work_efd_val_, sizeof(work_efd_val_), 0);
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

    void launch_fiber(task work) noexcept {
        pool_->in_flight_.fetch_add(1, std::memory_order_relaxed);
        auto tracked = [fn = std::move(work), pool = pool_]() mutable {
            fn();
            if (pool->in_flight_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                pool->shutdown_cv_.notify_all();
            }
        };
        boost::fibers::fiber(std::allocator_arg, boost::fibers::fixedsize_stack{pool_->opts_.stack_size},
                             std::move(tracked))
            .detach();
    }

    // Process every CQE currently in the ring. Creates fibers for new work,
    // resumes I/O-suspended fibers, and re-arms the watched fds.
    void drain_cqes() noexcept {
        io_uring_cqe* cqe = nullptr;
        while (io_uring_peek_cqe(&ring_, &cqe) == 0 && cqe != nullptr) {
            auto const tag = io_uring_cqe_get_data64(cqe);
            int const res = cqe->res;
            uint32_t const flags = cqe->flags;
            io_uring_cqe_seen(&ring_, cqe);
            cqe = nullptr;

            if (tag == k_work_tag) {
                if (res >= 0 && pool_->running_.load(std::memory_order_relaxed)) {
                    task work;
                    {
                        std::scoped_lock lk{pool_->mtx_};
                        if (!pool_->work_queue_.empty()) {
                            work = std::move(pool_->work_queue_.front());
                            pool_->work_queue_.pop();
                        }
                    }
                    if (work) {
                        launch_fiber(std::move(work));
                    }
                }
                // Guard re-arm: an unconditional re-arm after the stop token
                // would consume a token meant for another thread's notify fd.
                if (pool_->running_.load(std::memory_order_acquire)) {
                    arm_work_efd();
                }
            } else if (tag == k_notify_tag) {
                flush_cancel_queue();
                arm_notify();
            } else if (tag == k_cancel_tag) {
                // Cancel op completed (res==0 found, -ENOENT not found): ignore.
            } else {
                // I/O completion — dispatch to the registered handler.
                // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
                reinterpret_cast<detail::cqe_handler*>(tag)->complete(&ring_, res, flags);
            }
        }
    }

    fiber_pool* pool_;
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
fixed_buffer_pool* current_fixed_buffer_pool() noexcept { return tl_fixed_buffer_pool; }
fixed_fd_table* current_fd_table() noexcept { return tl_fixed_fd_table; }

void install_fiber_stop_token(std::stop_token tok) {
    if (!tok.stop_possible()) {
        return; // non-stoppable token — leave fiber_stop_token null, skip heap allocation
    }
    // fiber_specific_ptr takes ownership of the raw pointer and deletes it
    // when the fiber exits. NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
    fiber_stop_token.reset(new std::stop_token(std::move(tok)));
}

std::stop_token current_fiber_stop_token() {
    auto* p = fiber_stop_token.get();
    return p != nullptr ? *p : std::stop_token{};
}

int submit_and_wait(io_uring_sqe* sqe) {
    io_awaitable awaitable;
    auto future = awaitable.promise.get_future();
    io_uring_sqe_set_data(sqe, static_cast<detail::cqe_handler*>(std::addressof(awaitable)));
    if (int ret = io_uring_submit(tl_ring); ret < 0) {
        throw std::system_error(-ret, std::system_category(), "io_uring_submit");
    }
    auto* sched = tl_scheduler;
    auto const cancel = [sched, ud = std::addressof(awaitable)]() noexcept { sched->request_cancel(ud); };
    std::stop_callback pool_cb{sched->pool_stop_token(), cancel};
    auto fiber_st = current_fiber_stop_token();
    if (fiber_st.stop_possible()) {
        std::stop_callback fiber_cb{std::move(fiber_st), cancel};
        return future.get();
    }
    return future.get();
}

int submit_and_wait_with_timeout(io_uring_sqe* sqe, std::chrono::nanoseconds timeout) {
    // Chain a linked timeout: the kernel cancels sqe if it doesn't complete
    // within timeout, returning -ECANCELED for the op and -ETIME for the
    // timeout SQE (which drain_cqes discards via k_cancel_tag).
    sqe->flags |= IOSQE_IO_LINK;

    io_awaitable awaitable;
    auto future = awaitable.promise.get_future();
    io_uring_sqe_set_data(sqe, static_cast<detail::cqe_handler*>(std::addressof(awaitable)));

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
    auto* sched = tl_scheduler;
    auto const cancel = [sched, ud = std::addressof(awaitable)]() noexcept { sched->request_cancel(ud); };
    std::stop_callback pool_cb{sched->pool_stop_token(), cancel};
    auto fiber_st = current_fiber_stop_token();
    if (fiber_st.stop_possible()) {
        std::stop_callback fiber_cb{std::move(fiber_st), cancel};
        return future.get();
    }
    return future.get();
}

void submit_linked_and_wait(std::span<io_uring_sqe*> sqes, std::span<int> out) {
    std::size_t const n = sqes.size();
    if (n == 0 || n > k_max_linked_ops || n != out.size()) {
        throw std::invalid_argument("submit_linked_and_wait: chain length must be in [1, k_max_linked_ops]");
    }

    // Stack-allocate one awaitable per SQE. Boost.Fiber preserves the calling
    // fiber's stack while it is suspended, so these remain valid until all
    // CQEs arrive.  Unused array slots (indices n..k_max_linked_ops-1) are
    // default-constructed but never given a future; promise shared-state is
    // allocated lazily in get_future(), so they cost nothing here.
    std::array<io_awaitable, k_max_linked_ops> aws;
    std::array<boost::fibers::future<int>, k_max_linked_ops> futs;
    // Pre-capture awaitable addresses for the stop callbacks. Stored as void*
    // in a value-copyable array so the lambda owns stable pointers independent
    // of the aws array's stack lifetime (though both live equally long here).
    std::array<void*, k_max_linked_ops> cancel_targets{};

    // Wire each SQE to its awaitable. A plain size_t counter alongside
    // range-for over the span keeps array access through .at() (bounds-checked).
    {
        std::size_t i = 0;
        for (io_uring_sqe* sqe : sqes.subspan(0, n)) {
            io_awaitable& aw = aws.at(i);
            futs.at(i) = aw.promise.get_future();
            io_uring_sqe_set_data(sqe, static_cast<cqe_handler*>(&aw));
            cancel_targets.at(i) = static_cast<cqe_handler*>(&aw);
            ++i;
        }
    }
    // Set IOSQE_IO_LINK on every SQE except the last.
    for (io_uring_sqe* sqe : sqes.subspan(0, n - 1)) {
        sqe->flags |= IOSQE_IO_LINK;
    }

    if (int ret = io_uring_submit(tl_ring); ret < 0) {
        throw std::system_error(-ret, std::system_category(), "io_uring_submit (linked chain)");
    }

    // Cancel all N awaitables on stop. Only the in-flight one will succeed;
    // completed ones return -ENOENT (silently discarded by drain_cqes). The
    // kernel then cascades -ECANCELED to any not-yet-started linked SQEs.
    // cancel_targets is captured by value so it is valid inside the callback
    // regardless of what the fiber's stack looks like when it fires.
    auto* sched = tl_scheduler;
    auto const cancel = [sched, cancel_targets, n]() noexcept {
        for (std::size_t j = 0; j < n; ++j) {
            sched->request_cancel(cancel_targets.at(j));
        }
    };
    // Collect all results. The stop callbacks must stay alive for the entire
    // wait, not just one future, so we keep them in scope around wait_all().
    auto const wait_all = [&] {
        std::size_t i = 0;
        for (int& result : out.subspan(0, n)) {
            result = futs.at(i).get();
            ++i;
        }
    };
    std::stop_callback pool_cb{sched->pool_stop_token(), cancel};
    auto fiber_st = current_fiber_stop_token();
    if (fiber_st.stop_possible()) {
        std::stop_callback fiber_cb{std::move(fiber_st), cancel};
        wait_all();
    } else {
        wait_all();
    }
}

void submit_cancel(void* handler) noexcept {
    if (tl_ring == nullptr) {
        return;
    }
    io_uring_sqe* sqe = io_uring_get_sqe(tl_ring);
    if (sqe == nullptr) {
        return;
    }
    io_uring_prep_cancel(sqe, handler, 0);
    io_uring_sqe_set_data64(sqe, k_cancel_tag);
    io_uring_submit(tl_ring);
}

void schedule_task(fiber_pool& pool, task work) noexcept { pool.post(std::move(work)); }

} // namespace detail

void fiber_pool::worker() {
    boost::fibers::use_scheduling_algorithm<io_uring_scheduler>(this);

    // After use_scheduling_algorithm the ring is live (tl_ring is set).
    // Initialise per-thread kernel resources so all fibers on this thread can
    // share them.  Stack locals are destroyed when worker() returns, which is
    // before thread-local cleanup tears down the io_uring_scheduler and
    // clears tl_ring — so unregister_buffers / unregister_files see a live ring.
    std::optional<fixed_buffer_pool> fbp;
    std::optional<fixed_fd_table> fdt;

    if (opts_.fixed_buffer_size > 0 && opts_.fixed_buffer_count > 0) {
        fbp.emplace(opts_.fixed_buffer_size, opts_.fixed_buffer_count);
        tl_fixed_buffer_pool = &*fbp;
    }
    if (opts_.registered_fd_capacity > 0) {
        fdt.emplace(opts_.registered_fd_capacity);
        tl_fixed_fd_table = &*fdt;
    }

    // Park the main fiber here. The scheduler's suspend_until() drives all
    // io_uring and fiber dispatch. stop() notifies the CV to unblock us.
    // We also wait for all in-flight fibers to drain before exiting.
    std::unique_lock<boost::fibers::mutex> lk{shutdown_mtx_};
    shutdown_cv_.wait(lk, [this] {
        return !running_.load(std::memory_order_relaxed) && in_flight_.load(std::memory_order_acquire) == 0;
    });

    // Null out the thread-local pointers before the optionals are destroyed.
    tl_fixed_buffer_pool = nullptr;
    tl_fixed_fd_table = nullptr;
}

// ---------------------------------------------------------------------------
// context
// ---------------------------------------------------------------------------

context::context(context_options const& opts)
    : pool_(std::make_unique<fiber_pool>(opts)) {}

context::context(std::uint32_t thread_count, std::size_t stack_size)
    : context(context_options{.thread_count = thread_count, .stack_size = stack_size}) {}

context::~context() = default;

} // namespace fiberexec
