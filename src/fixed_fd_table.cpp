#include <fiberexec/detail/fiber_ops.hpp>
#include <fiberexec/fixed_fd_table.hpp>

#include <liburing.h>

#include <stdexcept>
#include <system_error>
#include <vector>

namespace fiberexec {

// ---------------------------------------------------------------------------
// fixed_fd_table
// ---------------------------------------------------------------------------

fixed_fd_table::fixed_fd_table(std::size_t capacity)
    : ring_{detail::current_ring()}
    , capacity_{capacity}
    , free_list_{capacity_} {
    if (ring_ == nullptr) {
        throw std::runtime_error("fixed_fd_table constructed outside of a fiberexec worker thread");
    }
    if (capacity_ == 0) {
        throw std::invalid_argument("fixed_fd_table: capacity must be non-zero");
    }

    std::vector<int> slots(capacity_, -1);
    if (int r = io_uring_register_files(ring_, slots.data(), static_cast<unsigned>(capacity_)); r < 0) {
        throw std::system_error(-r, std::system_category(), "io_uring_register_files");
    }

    for (unsigned i = 0; i < static_cast<unsigned>(capacity_); ++i) {
        static_cast<void>(free_list_.try_push(i));
    }
}

fixed_fd_table::~fixed_fd_table() {
    free_list_.close();
    if (ring_ != nullptr) {
        io_uring_unregister_files(ring_);
    }
}

void fixed_fd_table::update(unsigned slot, int fd) {
    if (int r = io_uring_register_files_update(ring_, slot, &fd, 1); r < 0) {
        throw std::system_error(-r, std::system_category(), "io_uring_register_files_update");
    }
}

fixed_fd fixed_fd_table::operator[](unsigned slot) const noexcept { return fixed_fd{static_cast<int>(slot)}; }

unsigned fixed_fd_table::borrow_slot() {
    unsigned slot{};
    auto status = free_list_.pop(slot);
    if (status != channel_op_status::success) {
        throw std::runtime_error("fixed_fd_table: table closed");
    }
    return slot;
}

void fixed_fd_table::release_slot(unsigned slot) noexcept { static_cast<void>(free_list_.push(slot)); }

// ---------------------------------------------------------------------------
// fd_slot
// ---------------------------------------------------------------------------

fd_slot::fd_slot(fixed_fd_table* table, unsigned slot) noexcept
    : table_{table}
    , slot_{slot} {}

fd_slot::fd_slot(fd_slot&& other) noexcept
    : table_{other.table_}
    , slot_{other.slot_} {
    other.table_ = nullptr;
}

fd_slot& fd_slot::operator=(fd_slot&& other) noexcept {
    if (this != &other) {
        if (table_ != nullptr) {
            table_->update(slot_, -1);
            table_->release_slot(slot_);
        }
        table_ = other.table_;
        slot_ = other.slot_;
        other.table_ = nullptr;
    }
    return *this;
}

fd_slot::~fd_slot() {
    if (table_ != nullptr) {
        table_->update(slot_, -1);
        table_->release_slot(slot_);
    }
}

void fd_slot::update(int fd) { table_->update(slot_, fd); }

// ---------------------------------------------------------------------------
// acquire_fd_slot
// ---------------------------------------------------------------------------

fd_slot acquire_fd_slot(int fd) {
    fixed_fd_table* tbl = detail::current_fd_table();
    if (tbl == nullptr) {
        throw std::runtime_error("acquire_fd_slot called without registered_fd_capacity configured in context_options");
    }
    unsigned slot = tbl->borrow_slot();
    tbl->update(slot, fd);
    return fd_slot{tbl, slot};
}

} // namespace fiberexec
