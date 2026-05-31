#pragma once

namespace fiberexec {

/// Opaque handle for a slot in a fixed_fd_table.
///
/// Pass to async ops; the kernel dispatches these with IOSQE_FIXED_FILE,
/// bypassing the per-op fd table lookup.
struct fixed_fd {
    int index;
};

/// Unified fd argument accepted by all async ops.
///
/// Implicitly constructed from a raw `int` (regular fd) or from `fixed_fd`
/// (pre-registered slot).  Passing an `fd_slot` also works via its
/// `operator fd_ref()` conversion.  The async op implementation checks the
/// `fixed` flag and sets IOSQE_FIXED_FILE on the SQE when needed.
struct fd_ref {
    int index;
    bool fixed;

    fd_ref(int fd) noexcept
        : index(fd)
        , fixed(false) {} // NOLINT(google-explicit-constructor)
    fd_ref(fixed_fd fd) noexcept
        : index(fd.index)
        , fixed(true) {} // NOLINT(google-explicit-constructor)
};

} // namespace fiberexec
