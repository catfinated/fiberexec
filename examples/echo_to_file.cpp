#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cstdio>
#include <span>
#include <string>
#include <system_error>

// Echo-to-file server: each client connection is handled by a worker fiber
// that asynchronously opens a dedicated log file, writes everything the client
// sends, then asynchronously closes the file.  No thread ever blocks — the
// fiber suspends on async_openat, async_write, and async_close exactly as it
// does on async_recv.  The number of clients is not known upfront; connections
// arrive through an accept loop into a bounded channel, and a fixed worker pool
// drains it.
//
//   accept loop  ──async_accept──▶  push(fd)
//                                      │
//                              channel<int>
//                                      │
//                                   pop(fd)
//                           ┌──────────┴──────────┐
//                        worker 0  ...  worker N-1
//               (async_openat → recv loop → async_write → async_close)
//
// Shutdown sequence: after the last connection is fully written and closed,
// the worker calls ::shutdown(server_fd, SHUT_RDWR), which causes async_accept
// to throw; the accept loop catches the error and closes the channel; remaining
// workers drain, see the channel closed, and exit.  Stop is triggered
// server-side (after all data is on disk) rather than client-side, so the
// accept loop cannot exit before every client's data has been received.

namespace {

constexpr int kWorkers = 4;
constexpr int kClients = 6; // intentionally > kWorkers to exercise queuing

int make_server_socket() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    ::listen(fd, 32);
    return fd;
}

sockaddr_in bound_addr(int fd) {
    sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
    return addr;
}

std::atomic<int> g_next_id{0}; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// Handle one client connection: asynchronously open a per-connection log file,
// write all received data to it, then asynchronously close it.  Runs entirely
// inside a worker fiber — async_openat, async_write, and async_close each
// suspend the fiber without blocking the OS thread.  From the fiber's
// perspective this is straight-line sequential code.
//
// Once all kClients connections have been processed, shuts down the listening
// socket so async_accept returns an error and the accept loop exits.
void handle_connection(int client_fd, int server_fd) {
    int const id = g_next_id.fetch_add(1, std::memory_order_relaxed);
    std::string const path = "connection_" + std::to_string(id) + ".log";

    // Suspend the fiber until the kernel opens the file and returns a fd.
    int const file_fd = fiberexec::async_openat(AT_FDCWD, path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);

    std::array<char, 256> buf{};
    try {
        ssize_t n{};
        while ((n = fiberexec::async_recv(client_fd, std::as_writable_bytes(std::span{buf}))) > 0) {
            fiberexec::async_write(file_fd, std::as_bytes(std::span{buf.data(), static_cast<std::size_t>(n)}));
        }
    } catch (std::system_error const&) {
    }

    // Suspend the fiber until the kernel flushes and closes each fd.
    fiberexec::async_close(file_fd);
    fiberexec::async_close(client_fd);

    std::printf("[connection %d] wrote %s\n", id, path.c_str());

    if (id == kClients - 1) {
        ::shutdown(server_fd, SHUT_RDWR); // unblock async_accept so the accept loop exits
    }
}

} // namespace

int main() {
    int const server_fd = make_server_socket();
    sockaddr_in const addr = bound_addr(server_fd);
    std::printf("listening on 127.0.0.1:%d  (%d workers, %d clients)\n", ntohs(addr.sin_port), kWorkers, kClients);

    fiberexec::context ctx{kWorkers};
    auto sched = ctx.get_scheduler();

    fiberexec::channel<int> conn_ch{8};

    auto worker = [&] {
        int fd{};
        while (conn_ch.pop(fd) == fiberexec::channel_op_status::success) {
            handle_connection(fd, server_fd);
        }
    };

    auto make_client = [&](int id) {
        return fiberexec::run(sched, [&, id] {
            int fd = ::socket(AF_INET, SOCK_STREAM, 0);
            fiberexec::async_connect(fd, reinterpret_cast<sockaddr const*>(&addr), sizeof(addr));
            std::string const msg = "hello from client " + std::to_string(id) + "\n";
            fiberexec::async_send(fd, std::as_bytes(std::span<char const>{msg.data(), msg.size()}));
            fiberexec::async_close(fd);
        });
    };

    stdexec::sync_wait(
        stdexec::when_all(fiberexec::run(sched,
                                         [&] {
                                             try {
                                                 while (true) {
                                                     int fd = fiberexec::async_accept(server_fd, nullptr, nullptr);
                                                     if (conn_ch.push(fd) != fiberexec::channel_op_status::success) {
                                                         ::close(fd);
                                                         break;
                                                     }
                                                 }
                                             } catch (std::system_error const&) {
                                             }
                                             conn_ch.close();
                                         }),
                          fiberexec::run(sched, worker), fiberexec::run(sched, worker), fiberexec::run(sched, worker),
                          fiberexec::run(sched, worker), make_client(0), make_client(1), make_client(2), make_client(3),
                          make_client(4), make_client(5)));

    ::close(server_fd);
    std::printf("done — check connection_*.log files.\n");
}
