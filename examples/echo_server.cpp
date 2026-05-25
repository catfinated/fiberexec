#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cstdio>
#include <string>

// A self-contained concurrent TCP echo server.
//
// One server fiber accepts three connections in sequence and echoes each
// message back. Three client fibers connect concurrently, send a greeting,
// and print the echo they receive. All network I/O goes through io_uring —
// the OS threads never block, and fibers that are waiting for I/O yield the
// thread to other runnable fibers.

namespace {

constexpr int kClients = 3;

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
    addr.sin_port = 0; // let the kernel pick a free port

    ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    ::listen(fd, kClients);
    return fd;
}

sockaddr_in bound_addr(int server_fd) {
    sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    ::getsockname(server_fd, reinterpret_cast<sockaddr*>(&addr), &len);
    return addr;
}

} // namespace

int main() {
    int server_fd = make_server_socket();
    if (server_fd < 0) {
        return 1;
    }
    sockaddr_in const addr = bound_addr(server_fd);

    std::printf("Echo server listening on 127.0.0.1:%d\n", ntohs(addr.sin_port));

    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    // Server fiber: accept kClients connections one at a time, echo each message.
    // async_accept suspends this fiber while waiting — the thread stays free.
    auto server = stdexec::schedule(sched) | stdexec::then([&] {
                      for (int i = 0; i < kClients; ++i) {
                          int conn_fd = fiberexec::async_accept(server_fd, nullptr, nullptr);
                          std::array<char, 128> buf{};
                          ssize_t n = fiberexec::async_recv(conn_fd, buf.data(), buf.size());
                          fiberexec::async_send(conn_fd, buf.data(), static_cast<std::size_t>(n));
                          std::printf("[server] echoed: \"%.*s\"\n", static_cast<int>(n), buf.data());
                          ::close(conn_fd);
                      }
                  });

    // Client factory: connect, send a greeting, receive and print the echo.
    auto make_client = [&](int id) {
        return stdexec::schedule(sched) | stdexec::then([&, id] {
                   int fd = ::socket(AF_INET, SOCK_STREAM, 0);
                   fiberexec::async_connect(fd, reinterpret_cast<sockaddr const*>(&addr), sizeof(addr));

                   std::string const msg = "hello from client " + std::to_string(id);
                   fiberexec::async_send(fd, msg.data(), msg.size());

                   std::array<char, 128> buf{};
                   ssize_t n = fiberexec::async_recv(fd, buf.data(), buf.size());
                   std::printf("[client %d] echo: \"%.*s\"\n", id, static_cast<int>(n), buf.data());
                   ::close(fd);
               });
    };

    stdexec::sync_wait(stdexec::when_all(server, make_client(0), make_client(1), make_client(2)));

    ::close(server_fd);
    std::printf("Done.\n");
}
