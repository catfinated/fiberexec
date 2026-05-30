#include <fiberexec/fiberexec.hpp>

#include <stdexec/execution.hpp>

#include <common/tcp_helpers.hpp>

#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdio>
#include <span>

// Stream consumer using IORING_RECV_MULTISHOT and kernel buffer rings.
//
// A producer fiber connects and sends kMessages fixed-size records over a
// single TCP connection, then closes. A consumer fiber accepts the connection
// and reads the entire stream via fiberexec::multishot_recv.
//
// With plain async_recv the consumer would re-arm an SQE for every receive.
// multishot_recv keeps one SQE armed in the ring for the lifetime of the
// stream; the kernel selects a buffer from a pre-registered ring for each
// arriving segment and delivers CQEs without any re-submission from userspace.
// received_buffer::data() is a zero-copy span into the kernel-selected buffer;
// no heap allocation occurs per message. The buffer is returned to the ring
// when the received_buffer handle is destroyed at the end of each loop
// iteration. When the producer closes the connection, next() returns nullopt.
//
// Buffer ring sizing: kBufSize is large relative to kRecordSize so the kernel
// often batches several records into a single CQE, reducing the total number
// of context switches between kernel and userspace.

namespace {

constexpr int kMessages = 10'000;
constexpr std::size_t kRecordSize = 64;
constexpr std::size_t kBufSize = 4096; // kernel buffer ring slot size
constexpr std::size_t kBufCount = 256; // ring depth (rounded to power-of-two)
constexpr std::size_t kExpected = static_cast<std::size_t>(kMessages) * kRecordSize;

} // namespace

int main() {
    int server_fd = make_server_socket();
    if (server_fd < 0) {
        return 1;
    }
    sockaddr_in const addr = bound_addr(server_fd);
    std::printf("stream_recv_multishot: %d records × %zu bytes  (buf ring: %zu × %zu)\n", kMessages, kRecordSize,
                kBufCount, kBufSize);

    fiberexec::context ctx{2};
    auto sched = ctx.get_scheduler();

    std::atomic<std::size_t> total_bytes{0};
    // CQEs < sends because each buffer is filled with however much data is in
    // the socket's receive buffer at the time the kernel selects the slot — so
    // two or more sends that accumulated before the next selection land in a
    // single buffer and produce one CQE. This is receive-side batching, not
    // Nagle-style send coalescing (which would batch before data leaves the
    // sender).
    std::atomic<int> cqe_count{0};

    stdexec::sync_wait(stdexec::when_all(
        // Consumer: accept one connection, drain the stream with multishot_recv.
        fiberexec::run(sched,
                       [&] {
                           int conn_fd = fiberexec::async_accept(server_fd, nullptr, nullptr);

                           fiberexec::multishot_recv mr{conn_fd, kBufSize, kBufCount};
                           while (auto buf = mr.next()) {
                               total_bytes.fetch_add(buf->data().size(), std::memory_order_relaxed);
                               cqe_count.fetch_add(1, std::memory_order_relaxed);
                               // buf destroyed here → buffer returned to the kernel ring
                           }

                           fiberexec::async_close(conn_fd);
                       }),
        // Producer: connect and stream kMessages records, then close.
        fiberexec::run(sched, [&] {
            int fd = ::socket(AF_INET, SOCK_STREAM, 0);
            fiberexec::async_connect(fd, reinterpret_cast<sockaddr const*>(&addr), sizeof(addr));

            std::array<std::byte, kRecordSize> record{};
            for (int i = 0; i < kMessages; ++i) {
                fiberexec::async_send(fd, std::span<std::byte const>{record});
            }
            fiberexec::async_close(fd);
        })));

    ::close(server_fd);

    bool const ok = total_bytes.load() == kExpected;
    std::printf("received %zu / %zu bytes across %d CQEs — %s\n", total_bytes.load(), kExpected, cqe_count.load(),
                ok ? "OK" : "MISMATCH");
    return ok ? 0 : 1;
}
