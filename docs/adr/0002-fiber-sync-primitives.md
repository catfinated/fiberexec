# ADR-0002: Fiber-aware synchronization primitives

**Status**: Partially implemented (`fiber_sync_wait` done; mutex/condvar dropped; channel deferred)

## Context

`stdexec::sync_wait` drives a sender to completion by blocking the calling OS
thread on a condition variable. Inside a fiberexec fiber, this is wrong:
blocking the OS thread starves every other fiber that shares it. A fiber that
calls `sync_wait` from inside a `then` lambda will hang the entire worker
thread for the duration of the wait.

The same problem applies to any OS-thread-blocking primitive:
`std::mutex::lock`, `std::condition_variable::wait`, or a plain `std::future`.
A fiber that holds or waits on these blocks its thread.

Boost.Fiber ships a set of fiber-aware equivalents that suspend the *fiber*
instead of the thread, yielding it to the scheduler so other fibers can run.
These are the building blocks for all fiber-level synchronization in fiberexec.

## Primitives

### `fiber_sync_wait(sender)` — await a sender from inside a fiber

The primary motivation. Allows a fiber to fan out work as a sender graph and
collect the result without blocking the OS thread:

```cpp
schedule(sched) | then([&] {
    auto reads = stdexec::when_all(
        schedule(sched) | then([&] { return fiberexec::async_recv(a, ...); }),
        schedule(sched) | then([&] { return fiberexec::async_recv(b, ...); })
    );
    auto [x, y] = fiberexec::fiber_sync_wait(std::move(reads));
    // ...
});
```

#### Why `stdexec::sync_wait` cannot be used here

`stdexec::sync_wait` blocks the calling OS thread. On a fiberexec worker,
that suspends the entire thread's fiber scheduler — no other fiber on that
thread can run until the wait completes. If the sender being awaited needs a
fiber on the *same* thread to make progress, the result is deadlock.

#### Design

`fiber_sync_wait` bridges the sender's completion and the calling fiber
using a heap-allocated `fiber_sync_state` struct (mutex, condition variable,
result value, exception pointer, done flag) shared via `shared_ptr` between
the waiter and the `sync_wait_receiver`.

The fiber calls `boost::fibers::condition_variable::wait()` which suspends
the fiber (not the thread), returning control to the scheduler. When the
sender completes — from any thread — the receiver sets the result under the
mutex, marks `done`, and calls `cv.notify_all()`. Boost.Fiber wakes the
suspended fiber, and the scheduler resumes it on its next pass.

Cross-thread wakeup works because our `io_uring_scheduler::awakened()` is
protected by a mutex and `notify()` writes to the per-thread `notify_fd`,
interrupting any blocked `io_uring_wait_cqe`.

Return type mirrors `stdexec::sync_wait`: `std::optional<value_tuple>`,
where `value_tuple` is `std::tuple<value_types...>`. Returns `std::nullopt`
if the sender completed with `set_stopped`.

`set_error` completions rethrow the stored exception into the calling fiber.

#### Why the sync state is heap-allocated

P2300 senders ordinarily make no heap allocations, and `stdexec::sync_wait`
upholds that by putting its sync state (mutex, condvar, result) on the
calling OS thread's stack. It can do this safely because blocking the OS
thread keeps the stack frame alive for the full duration of the wait —
including the `cv.notify_all()` call on the completing thread. The two
threads reach `notify_all-returns` and `stack-unwind` in strict sequence.

`fiber_sync_wait` cannot block the OS thread — that would starve every other
fiber sharing it. Instead it suspends the fiber cooperatively and frees the
thread to run other fibers. This introduces a race that `stdexec::sync_wait`
never faces: when the completing thread calls `cv.notify_all()`, the woken
fiber can resume on a *different* OS thread and destroy the operation state
(including the receiver and any stack-allocated sync state) *before*
`notify_all()` returns.

`std::condition_variable` is immune because its underlying `FUTEX_WAKE`
syscall is atomic: the kernel records the wakeup and returns without further
access to the condvar's memory. Boost.Fiber's `condition_variable` is not —
it traverses a **user-space intrusive linked list** of waiting fiber contexts.
If the list or its nodes are freed mid-traversal (because the woken fiber
already ran and destroyed them), the traversal reads freed memory and crashes.

The fix is to allocate the sync state on the heap and share it via
`shared_ptr`. Each completion method (`set_value`, `set_error`, `set_stopped`)
copies the `shared_ptr` into a local variable at entry, keeping the state
alive through the `notify_all()` call regardless of what the outer fiber
destroys concurrently. The allocation is a one-per-`fiber_sync_wait`-call
overhead, not per-item, and only occurs on the rare call sites where a fiber
needs to fan out and collect results.

#### Constraints

- Must be called from a fiber running on a fiberexec worker thread.
  Calling from a plain OS thread is undefined behaviour (Boost.Fiber
  primitives are not valid outside a fiber context).
- The `Sender` must complete with at most one value type overload (the
  restriction `stdexec::sync_wait` imposes). Multi-valued senders must be
  wrapped with a `then` that packs them into a tuple first.

---

### `fiber_mutex` and `fiber_condition_variable` — dropped

This ADR originally planned thin wrappers over `boost::fibers::mutex` and
`boost::fibers::condition_variable` to keep Boost.Fiber out of public headers.
This rationale no longer holds: `fiber_sync_wait.hpp` includes
`<boost/fiber/mutex.hpp>` and `<boost/fiber/condition_variable.hpp>` in the
public API because the template implementation requires the full types at
instantiation time. The "keep Boost private" goal is already compromised, and
adding wrapper types provides little value to the project.

The remaining arguments for wrapping are:

- namespace consistency 
- theoretical future-proofing against a fiber runtime swap

Neither make the juice worth the squeeze. 
A fiber runtime swap would require rewriting `fiber_sync_wait`, the
scheduler, and essentially everything else; the wrappers would not meaningfully
reduce the cost.

**Decision**: do not implement `fiber_mutex` or `fiber_condition_variable`.
Users should use `boost::fibers::mutex` and `boost::fibers::condition_variable`
directly. This should be documented in the public API docs as the correct
alternative to `std::mutex` and `std::condition_variable` inside fiberexec
fibers.

---

### `fiber_channel<T>` — bounded MPMC channel (deferred)

A bounded queue with fiber-aware blocking semantics: `push` suspends the
producer fiber if the channel is full; `pop` suspends the consumer fiber if
it is empty. Useful for producer/consumer pipelines entirely within the fiber
pool.

Implementation would use `boost::fibers::buffered_channel<T>` or a hand-rolled
variant on top of `fiber_mutex` + `fiber_condition_variable`.

Deferred until mutex/condvar are in place.

---

## Implementation order

1. ✅ `fiber_sync_wait` — highest leverage; unblocks the README example and
   makes sender fan-out composable from inside fibers.
2. ~~`fiber_mutex` + `fiber_condition_variable`~~ — dropped; see above.
3. ⏳ `fiber_channel<T>` — wraps `boost::fibers::buffered_channel<T>`;
   enables structured producer/consumer patterns.

## Consequences

- Calling any OS-thread-blocking primitive (`std::mutex`, `std::future`,
  `stdexec::sync_wait`) from a fiberexec fiber is a latent bug. Users should
  use `boost::fibers::mutex`, `boost::fibers::condition_variable`, and
  `fiberexec::fiber_sync_wait` instead. This should be prominently documented.
- `fiber_sync_wait.hpp` exposes `<boost/fiber/mutex.hpp>` and
  `<boost/fiber/condition_variable.hpp>` as public dependencies. Boost.Fiber
  is therefore a visible part of the fiberexec API, not purely an
  implementation detail.
- `fiber_sync_wait` performs one heap allocation per call (the shared sync
  state). This is unavoidable given Boost.Fiber's user-space condvar
  traversal and the requirement not to block the OS thread; see the design
  section above for the full rationale.
