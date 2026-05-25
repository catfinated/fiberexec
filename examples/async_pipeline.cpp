#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

#include <unistd.h>

#include <array>
#include <chrono>
#include <iostream>
#include <string_view>

// Two fibers share a pipe. The writer sends three messages, pausing between
// each with async_sleep_for. The reader consumes them as they arrive using
// async_read. Both fibers run concurrently on the pool — the OS thread is
// never blocked during the sleeps or while waiting for pipe data.

using namespace std::chrono_literals;

namespace {

constexpr std::array k_messages{
    std::string_view{"ping\n"},
    std::string_view{"pong\n"},
    std::string_view{"done\n"},
};

} // namespace

int main() {
    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    std::array<int, 2> pipe_fds{};
    ::pipe(pipe_fds.data());
    auto const [read_fd, write_fd] = pipe_fds;

    auto writer = stdexec::schedule(sched) | stdexec::then([write_fd] {
                      for (auto msg : k_messages) {
                          fiberexec::async_write(write_fd, msg.data(), msg.size());
                          fiberexec::async_sleep_for(100ms);
                      }
                      ::close(write_fd);
                  });

    auto reader = stdexec::schedule(sched) | stdexec::then([read_fd] {
                      std::array<char, 256> buf{};
                      while (true) {
                          auto const n = fiberexec::async_read(read_fd, buf.data(), buf.size());
                          if (n == 0) {
                              break; // EOF — writer closed its end
                          }
                          std::cout << std::string_view(buf.data(), static_cast<std::size_t>(n));
                      }
                      ::close(read_fd);
                  });

    stdexec::sync_wait(stdexec::when_all(writer, reader));
}
