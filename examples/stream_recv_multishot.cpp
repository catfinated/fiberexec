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

// Full zero-copy stream: fixed_buffer_pool (send) + multishot_recv (recv).
//
// Producer side — fixed_buffer_pool + async_send_zc:
//   Buffers are registered with the kernel once via IORING_REGISTER_BUFFERS.
//   Each async_send_zc references a buffer by its registered index; the kernel
//   reads from the pinned buffer without a copy and delivers two CQEs: one for
//   the send and one notification when the buffer is released. The fixed_buffer
//   destructor returns the slot to the pool so it can be borrowed again.
//
// Consumer side — multishot_recv + kernel buffer ring:
//   One SQE stays armed via IORING_RECV_MULTISHOT. The kernel selects a slot
//   from the IORING_REGISTER_PBUF_RING buffer ring, writes arriving data into
//   it, and delivers a CQE. received_buffer::data() is a zero-copy span into
//   that slot; the slot is returned to the ring when the handle is destroyed.
//   When the producer closes the connection, next() returns nullopt.
//
// Neither side copies data through an intermediate kernel buffer.
//
// CQE count note: recv-side CQEs are often fewer than sends because the kernel
// fills each buffer slot with all data currently in the socket receive buffer
// at the time of selection — multiple sends that have accumulated land in one
// slot and produce one CQE. This is receive-side batching, not Nagle-style
// send coalescing (which batches before data leaves the sender).

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
        // Producer: connect, stream kMessages records via zero-copy send, then close.
        fiberexec::run(sched, [&] {
            int fd = ::socket(AF_INET, SOCK_STREAM, 0);
            fiberexec::async_connect(fd, reinterpret_cast<sockaddr const*>(&addr), sizeof(addr));

            // Register a small pool of fixed send buffers. The kernel pins them
            // once; each async_send_zc sends from the registered buffer by index
            // with no intermediate copy.
            fiberexec::fixed_buffer_pool pool{kRecordSize, 8};
            for (int i = 0; i < kMessages; ++i) {
                auto fb = pool.borrow(); // blocks if all 8 slots are in flight
                // fb.data() is zero-initialised; fill with real payload here.
                fiberexec::async_send_zc(fd, fb, kRecordSize);
                // fb destroyed → slot returned to pool for next iteration
            }
            fiberexec::async_close(fd);
        })));

    ::close(server_fd);

    bool const ok = total_bytes.load() == kExpected;
    std::printf("received %zu / %zu bytes across %d CQEs — %s\n", total_bytes.load(), kExpected, cqe_count.load(),
                ok ? "OK" : "MISMATCH");
    return ok ? 0 : 1;
}
