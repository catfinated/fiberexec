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

`fiber_sync_wait` uses `boost::fibers::promise` / `boost::fibers::future`
as the bridge between the sender's completion and the calling fiber. The
fiber calls `future.get()` which suspends the fiber (not the thread),
returning control to the scheduler. When the sender completes — from any
thread — it sets the promise, Boost.Fiber wakes the suspended fiber, and
the scheduler resumes it on its next pass.

Cross-thread wakeup works because our `io_uring_scheduler::awakened()` is
protected by a mutex and `notify()` writes to the per-thread `notify_fd`,
interrupting any blocked `io_uring_wait_cqe`.

Return type mirrors `stdexec::sync_wait`: `std::optional<value_tuple>`,
where `value_tuple` is `std::tuple<value_types...>`. Returns `std::nullopt`
if the sender completed with `set_stopped`.

`set_error` completions rethrow the stored exception into the calling fiber.

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
This rationale no longer holds: `fiber_sync_wait.hpp` already includes
`<boost/fiber/future.hpp>` in the public API because the template
implementation requires the full `boost::fibers::promise` and
`boost::fibers::future` types at instantiation time. The "keep Boost private"
goal is already compromised, and adding wrapper types provides little
value to the project.

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
- `fiber_sync_wait.hpp` exposes `<boost/fiber/future.hpp>` as a public
  dependency. Boost.Fiber is therefore a visible part of the fiberexec API,
  not purely an implementation detail.
