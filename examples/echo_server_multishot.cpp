#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

#include <common/tcp_helpers.hpp>

#include <unistd.h>

#include <array>
#include <atomic>
#include <cstdio>
#include <span>
#include <string>

// Echo server using IORING_ACCEPT_MULTISHOT.
//
// Identical to echo_server_pool in behaviour, but the accept loop uses
// fiberexec::multishot_acceptor instead of calling async_accept in a loop.
// One SQE stays armed in the ring for the lifetime of the acceptor; the
// scheduler resubmits it automatically when the kernel consumes it without
// error.  For accept-heavy workloads this halves the number of SQEs submitted
// compared to the single-shot approach.
//
// The shutdown sequence is unchanged: the last test client calls
// ::shutdown(server_fd, SHUT_RDWR), which terminates the multishot SQE with
// an error, next() throws, the accept loop closes the channel, and workers
// drain and exit.

namespace {

constexpr int kWorkers = 4;
constexpr int kClients = 8;

void handle_connection(int fd) {
    std::array<char, 256> buf{};
    try {
        ssize_t n{};
        while ((n = fiberexec::async_recv(fd, std::as_writable_bytes(std::span{buf}))) > 0) {
            fiberexec::async_send(fd, std::as_bytes(std::span{buf.data(), static_cast<std::size_t>(n)}));
        }
    } catch (std::system_error const&) {
    }
    fiberexec::async_close(fd);
}

} // namespace

int main() {
    int server_fd = make_server_socket();
    if (server_fd < 0) {
        return 1;
    }
    sockaddr_in const addr = bound_addr(server_fd);
    std::printf("listening on 127.0.0.1:%d  (%d workers, %d clients)\n", ntohs(addr.sin_port), kWorkers, kClients);

    fiberexec::context ctx{kWorkers};
    auto sched = ctx.get_scheduler();

    fiberexec::channel<int> conn_ch{8};
    std::atomic<int> clients_done{0};

    auto worker = [&] {
        int fd{};
        while (conn_ch.pop(fd) == fiberexec::channel_op_status::success) {
            handle_connection(fd);
        }
    };

    auto make_client = [&](int id) {
        return fiberexec::run(sched, [&, id] {
            int fd = ::socket(AF_INET, SOCK_STREAM, 0);
            fiberexec::async_connect(fd, reinterpret_cast<sockaddr const*>(&addr), sizeof(addr));

            std::string const msg = "hello from client " + std::to_string(id);
            fiberexec::async_send(fd, std::as_bytes(std::span<char const>{msg.data(), msg.size()}));

            std::array<char, 256> buf{};
            ssize_t n = fiberexec::async_recv(fd, std::as_writable_bytes(std::span{buf}));
            std::printf("[client %d] echo: \"%.*s\"\n", id, static_cast<int>(n), buf.data());
            ::close(fd);

            if (clients_done.fetch_add(1, std::memory_order_acq_rel) == kClients - 1) {
                ::shutdown(server_fd, SHUT_RDWR);
            }
        });
    };

    stdexec::sync_wait(
        stdexec::when_all(fiberexec::run(sched,
                                         [&] {
                                             fiberexec::multishot_acceptor acc{server_fd, nullptr, nullptr};
                                             while (auto fd = acc.next()) {
                                                 if (conn_ch.push(*fd) != fiberexec::channel_op_status::success) {
                                                     fiberexec::async_close(*fd);
                                                     break;
                                                 }
                                             }
                                             conn_ch.close();
                                         }),
                          fiberexec::run(sched, worker), fiberexec::run(sched, worker), fiberexec::run(sched, worker),
                          fiberexec::run(sched, worker), make_client(0), make_client(1), make_client(2), make_client(3),
                          make_client(4), make_client(5), make_client(6), make_client(7)));

    ::close(server_fd);
    std::printf("done.\n");
}
