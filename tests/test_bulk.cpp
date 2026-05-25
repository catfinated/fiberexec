#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <vector>

// ---------------------------------------------------------------------------
// stdexec::bulk on fiberexec::scheduler
//
// fiber_domain::transform_sender intercepts bulk_chunked_t and fans out N
// fibers via detail::schedule_task so indices run concurrently across pool
// threads rather than sequentially on one thread.
// ---------------------------------------------------------------------------

TEST_CASE("bulk visits every index exactly once", "[bulk]") {
    fiberexec::context ctx{4};
    auto sched = ctx.get_scheduler();
    constexpr std::size_t N = 64;

    std::vector<std::atomic<int>> hits(N);
    for (auto& h : hits) {
        h.store(0);
    }

    stdexec::sync_wait(stdexec::bulk(stdexec::schedule(sched), stdexec::par, N,
                                     [&](std::size_t i) { hits.at(i).fetch_add(1, std::memory_order_relaxed); }));

    for (std::size_t i = 0; i < N; ++i) {
        REQUIRE(hits.at(i).load() == 1);
    }
}

TEST_CASE("bulk with N=0 completes immediately without calling the function", "[bulk]") {
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    bool called = false;
    stdexec::sync_wait(
        stdexec::bulk(stdexec::schedule(sched), stdexec::par, std::size_t{0}, [&](std::size_t) { called = true; }));

    REQUIRE_FALSE(called);
}

TEST_CASE("bulk with N=1 executes the single index", "[bulk]") {
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    std::size_t seen = std::numeric_limits<std::size_t>::max();
    stdexec::sync_wait(
        stdexec::bulk(stdexec::schedule(sched), stdexec::par, std::size_t{1}, [&](std::size_t i) { seen = i; }));

    REQUIRE(seen == 0);
}

TEST_CASE("bulk propagates exception from worker fiber as set_error", "[bulk]") {
    fiberexec::context ctx{4};
    auto sched = ctx.get_scheduler();
    constexpr std::size_t N = 8;

    bool caught = false;
    try {
        stdexec::sync_wait(stdexec::bulk(stdexec::schedule(sched), stdexec::par, N, [](std::size_t i) {
            if (i == 3) {
                throw std::runtime_error("fiber error");
            }
        }));
    } catch (std::runtime_error const& e) {
        caught = (std::string_view{e.what()} == "fiber error");
    }

    REQUIRE(caught);
}

TEST_CASE("bulk distributes work across multiple pool threads", "[bulk]") {
    // Each fiber records which OS thread it ran on. With N > thread_count and
    // a 4-thread pool, we expect more than one thread to be observed.
    fiberexec::context ctx{4};
    auto sched = ctx.get_scheduler();
    constexpr std::size_t N = 128;

    std::mutex mtx;
    std::set<std::thread::id> unique_threads;

    stdexec::sync_wait(stdexec::bulk(stdexec::schedule(sched), stdexec::par, N, [&](std::size_t) {
        std::scoped_lock lock{mtx};
        unique_threads.insert(std::this_thread::get_id());
    }));

    REQUIRE(unique_threads.size() > 1);
}
