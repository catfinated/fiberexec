#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <stop_token>
#include <string>
#include <system_error>

// Realistic TCP echo server: bounded worker pool via channel.
//
// The accept loop and worker pool are decoupled by a channel<int> that
// carries accepted file descriptors.  The channel capacity bounds how many
// unhandled connections can queue up — if all workers are busy and the queue
// is full, the accept loop suspends cooperatively on push() until a worker
// pops one (backpressure).  No OS thread ever blocks.
//
//   accept loop  ──async_accept──▶  push(fd)
//                                      │
//                              channel<int>   ← bounded queue
//                                      │
//                                   pop(fd)
//                           ┌──────────┴──────────┐
//                        worker 0  ...  worker N-1
//                    (recv loop → echo → close → next fd)
//
// Shutdown sequence:
//   1. The last test client calls ss.request_stop().
//   2. async_accept observes the stop token and throws ECANCELED.
//   3. The accept loop closes the channel and exits.
//   4. Workers drain remaining connections, see the channel closed, and exit.
//   5. when_all completes.

namespace {

constexpr int kWorkers = 4;
constexpr int kClients = 8; // intentionally > kWorkers to exercise queuing

int make_server_socket() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::perror("socket");
        return -1;
    }
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

// Echo all bytes received on fd back to the sender until the client closes.
// Called from a worker fiber — async_recv/send suspend the fiber, not the thread.
void handle_connection(int fd) {
    std::array<char, 256> buf{};
    try {
        ssize_t n{};
        while ((n = fiberexec::async_recv(fd, buf.data(), buf.size())) > 0) {
            fiberexec::async_send(fd, buf.data(), static_cast<std::size_t>(n));
        }
    } catch (std::system_error const&) {
    }
    ::close(fd);
}

} // namespace

int main() {
    int server_fd = make_server_socket();
    if (server_fd < 0) {
        return 1;
    }
    sockaddr_in const addr = bound_addr(server_fd);
    std::printf("listening on 127.0.0.1:%d  (%d workers, %d clients)\n", ntohs(addr.sin_port), kWorkers, kClients);

    fiberexec::context ctx{4};
    auto sched = ctx.get_scheduler();

    // Bounded connection queue: capacity 8 → 7 usable slots.
    // With kWorkers=4 handling connections and kClients=8 arriving, up to
    // 3 connections will queue while all workers are busy.
    fiberexec::channel<int> conn_ch{8};
    std::stop_source ss;
    std::atomic<int> clients_done{0};

    // Worker body: pop and handle connections until the channel is closed.
    auto worker = [&] {
        int fd{};
        while (conn_ch.pop(fd) == fiberexec::channel_op_status::success) {
            handle_connection(fd);
        }
    };

    // Test client: connect, send a message, receive the echo, disconnect.
    // The last client to finish signals the server to shut down.
    auto make_client = [&](int id) {
        return fiberexec::run(sched, [&, id] {
            int fd = ::socket(AF_INET, SOCK_STREAM, 0);
            fiberexec::async_connect(fd, reinterpret_cast<sockaddr const*>(&addr), sizeof(addr));

            std::string const msg = "hello from client " + std::to_string(id);
            fiberexec::async_send(fd, msg.data(), msg.size());

            std::array<char, 256> buf{};
            ssize_t n = fiberexec::async_recv(fd, buf.data(), buf.size());
            std::printf("[client %d] echo: \"%.*s\"\n", id, static_cast<int>(n), buf.data());
            ::close(fd);

            if (clients_done.fetch_add(1, std::memory_order_acq_rel) == kClients - 1) {
                ss.request_stop();
            }
        });
    };

    stdexec::sync_wait(stdexec::when_all(
        // Accept loop: push each accepted fd into the channel.
        // Suspends on push() if the channel is full (backpressure).
        // Exits cleanly when the stop token fires.
        fiberexec::run(sched,
                       [&] {
                           try {
                               while (true) {
                                   int fd = fiberexec::async_accept(server_fd, nullptr, nullptr, ss.get_token());
                                   if (conn_ch.push(fd) != fiberexec::channel_op_status::success) {
                                       ::close(fd);
                                       break;
                                   }
                               }
                           } catch (std::system_error const& e) {
                               if (e.code().value() != ECANCELED) {
                                   throw;
                               }
                           }
                           conn_ch.close();
                       }),
        // Worker pool.
        fiberexec::run(sched, worker), fiberexec::run(sched, worker), fiberexec::run(sched, worker),
        fiberexec::run(sched, worker),
        // Test clients.
        make_client(0), make_client(1), make_client(2), make_client(3), make_client(4), make_client(5), make_client(6),
        make_client(7)));

    ::close(server_fd);
    std::printf("done.\n");
}
