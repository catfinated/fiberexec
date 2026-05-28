// cancellation.cpp
//
// Demonstrates fiberexec::run + stdexec::upon_stopped for clean cancellation
// handling using the timeout parameter on async ops.
//
// A reader fiber tries to read a message from a pipe before a deadline.
// Because fiberexec::run maps ECANCELED → set_stopped, the downstream
// stdexec::upon_stopped can convert the cancellation to a fallback value.
// The overall sender always completes with set_value — no exception handling
// or error-code matching at the composition site.
//
// Two scenarios run from main() via sync_wait (never nested inside a fiber):
//   1. Data pre-written to pipe: reader completes immediately, before timeout.
//   2. Pipe stays empty: timeout fires, ECANCELED → set_stopped → fallback.

#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

#include <unistd.h>

#include <array>
#include <chrono>
#include <iostream>
#include <span>
#include <string>
#include <string_view>

using namespace std::chrono_literals;

int main() {
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    // -------------------------------------------------------------------------
    // Scenario 1: data arrives before the deadline.
    //
    // Data is pre-written to the pipe so the reader returns immediately,
    // well within the 200 ms timeout.
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

        auto reader = fiberexec::run(sched,
                                     [rfd] {
                                         std::array<char, 64> buf{};
                                         auto n =
                                             fiberexec::async_read(rfd, std::as_writable_bytes(std::span{buf}), 200ms);
                                         return std::string(buf.data(), static_cast<std::size_t>(n));
                                     }) |
                      stdexec::upon_stopped([] { return std::string{"(timed out)"}; });

        auto [message] = *stdexec::sync_wait(reader);
        std::cout << "scenario 1 (data arrives):  " << message << '\n';

        ::close(rfd);
    }

    // -------------------------------------------------------------------------
    // Scenario 2: deadline fires before any data arrives.
    //
    // The pipe stays empty; async_read times out after 30 ms, throwing
    // ECANCELED.  run() maps that to set_stopped; upon_stopped supplies the
    // fallback.
    // -------------------------------------------------------------------------
    {
        std::array<int, 2> pipefd{};
        if (::pipe(pipefd.data()) != 0) {
            return 1;
        }
        auto [rfd, wfd] = pipefd;

        auto reader = fiberexec::run(sched,
                                     [rfd] {
                                         std::array<char, 64> buf{};
                                         auto n =
                                             fiberexec::async_read(rfd, std::as_writable_bytes(std::span{buf}), 30ms);
                                         return std::string(buf.data(), static_cast<std::size_t>(n));
                                     }) |
                      stdexec::upon_stopped([] { return std::string{"(timed out)"}; });

        auto [message] = *stdexec::sync_wait(reader);
        std::cout << "scenario 2 (timeout fires): " << message << '\n';

        ::close(rfd);
        ::close(wfd);
    }

    return 0;
}
