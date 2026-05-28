#pragma once

// Shared POSIX TCP helpers used by examples and benchmarks.
// No fiberexec or Asio dependency — plain BSD socket API only.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <cstdio>

// Create a TCP server socket bound to 127.0.0.1 on a kernel-assigned port and
// ready to accept connections.  Returns -1 on failure.
inline int make_server_socket() {
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
    ::listen(fd, 4096);
    return fd;
}

// Return the local address the socket is bound to.
inline sockaddr_in bound_addr(int fd) {
    sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
    return addr;
}
