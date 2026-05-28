#pragma once

// Shared Boost.Asio helpers used by benchmarks.
// Requires Boost.Asio on the include path — do not include from code that
// does not link against asioexec_iface or Boost::asio.

#include <boost/asio/ip/tcp.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>

// Convert an Asio TCP acceptor's bound endpoint to a sockaddr_in so that
// plain blocking connect() calls can reach it.
inline sockaddr_in asio_local_addr(boost::asio::ip::tcp::acceptor const& acc) {
    auto ep = acc.local_endpoint();
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(ep.port());
    return addr;
}
