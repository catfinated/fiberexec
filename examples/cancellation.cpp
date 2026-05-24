// cancellation.cpp
//
// Demonstrates fiberexec::run + stdexec::upon_stopped for clean cancellation
// handling.
//
// A reader fiber tries to read a message from a pipe before a deadline. A
// timer fiber signals cancellation via a stop_source if the deadline expires.
// Because fiberexec::run maps ECANCELED → set_stopped, the downstream
// stdexec::upon_stopped can convert the cancellation to a fallback value.
// The overall sender always completes with set_value — no exception handling
// or error-code matching at the composition site.
//
// Two scenarios run from main() via sync_wait (never nested inside a fiber):
//   1. Data pre-written to pipe: reader completes immediately, timer fires later
//      but the stop request is a no-op since the read is already done.
//   2. Pipe stays empty: timer fires first, cancels the blocking read, and
//      upon_stopped supplies the fallback.

#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

#include <unistd.h>

#include <array>
#include <chrono>
#include <iostream>
#include <stop_token>
#include <string>
#include <string_view>

using namespace std::chrono_literals;

int main() {
    fiberexec::fiber_context ctx{2};
    auto sched = ctx.get_scheduler();

    // -------------------------------------------------------------------------
    // Scenario 1: data arrives before the deadline.
    //
    // Data is pre-written to the pipe so the reader returns immediately.
    // The timer fires 20ms later, calls request_stop(), but the stop_callback
    // was already deregistered when async_read returned, so it is a no-op.
    // upon_stopped is never invoked; sync_wait returns the actual message.
    // -------------------------------------------------------------------------
    {
        std::array<int, 2> pipefd{};
        if (::pipe(pipefd.data()) != 0) {
            return 1;
        }
        auto [rfd, wfd] = pipefd;

        constexpr std::string_view kMsg = "hello from writer";
        ::write(wfd, kMsg.data(), kMsg.size());
        ::close(wfd);

        std::stop_source ss;

        auto reader = fiberexec::run(sched,
                                     [rfd, tok = ss.get_token()] {
                                         std::array<char, 64> buf{};
                                         auto n = fiberexec::async_read(rfd, buf.data(), buf.size(), tok);
                                         return std::string(buf.data(), static_cast<std::size_t>(n));
                                     }) |
                      stdexec::upon_stopped([] { return std::string{"(timed out)"}; });

        auto timer = stdexec::schedule(sched) | stdexec::then([&ss] {
                         fiberexec::async_sleep_for(20ms);
                         ss.request_stop();
                     });

        auto [message] = *stdexec::sync_wait(stdexec::when_all(std::move(reader), timer));
        std::cout << "scenario 1 (data arrives):  " << message << '\n';

        ::close(rfd);
    }

    // -------------------------------------------------------------------------
    // Scenario 2: deadline fires before any data arrives.
    //
    // The pipe stays empty; the 30ms timer fires and calls request_stop().
    // The stop_callback cancels the in-flight async_read via IORING_OP_ASYNC_CANCEL.
    // async_read throws system_error(ECANCELED); run() maps it to set_stopped.
    // upon_stopped converts set_stopped → set_value with the fallback string.
    // -------------------------------------------------------------------------
    {
        std::array<int, 2> pipefd{};
        if (::pipe(pipefd.data()) != 0) {
            return 1;
        }
        auto [rfd, wfd] = pipefd;

        std::stop_source ss;

        auto reader = fiberexec::run(sched,
                                     [rfd, tok = ss.get_token()] {
                                         std::array<char, 64> buf{};
                                         auto n = fiberexec::async_read(rfd, buf.data(), buf.size(), tok);
                                         return std::string(buf.data(), static_cast<std::size_t>(n));
                                     }) |
                      stdexec::upon_stopped([] { return std::string{"(timed out)"}; });

        auto timer = stdexec::schedule(sched) | stdexec::then([&ss] {
                         fiberexec::async_sleep_for(30ms);
                         ss.request_stop();
                     });

        auto [message] = *stdexec::sync_wait(stdexec::when_all(std::move(reader), timer));
        std::cout << "scenario 2 (timeout fires): " << message << '\n';

        ::close(rfd);
        ::close(wfd);
    }

    return 0;
}
