# ADR-0002: Fiber-aware synchronization primitives

**Status**: Partially implemented (`fiber_sync_wait` done; mutex/condvar/channel deferred)

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

### `fiber_mutex` — cooperative mutex

A mutex that suspends the waiting fiber instead of blocking the OS thread.
Equivalent to `boost::fibers::mutex`.

#### Design options

**Option A — alias `boost::fibers::mutex` directly**

```cpp
using fiber_mutex = boost::fibers::mutex;
```

Zero implementation cost; callers get the full Boost.Fiber mutex API. The
downside is that the Boost.Fiber header becomes a transitive public dependency.

**Option B — thin wrapper**

```cpp
class fiber_mutex {
    boost::fibers::mutex impl_;
public:
    void lock();
    bool try_lock();
    void unlock();
};
```

Hides the Boost.Fiber header from public consumers; the type name is in
the fiberexec namespace. Minimal extra code.

**Recommendation**: Option B. Boost.Fiber should remain an implementation
detail; exposing `boost::fibers::mutex` directly forces all fiberexec users
to depend on Boost headers.

---

### `fiber_condition_variable` — cooperative condition variable

A condition variable that suspends the waiting fiber instead of the thread.
Natural companion to `fiber_mutex`.

#### Design

Same wrapper pattern as `fiber_mutex` over `boost::fibers::condition_variable`.
Exposes `wait`, `wait_for`, `wait_until`, `notify_one`, `notify_all`.

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
2. ⏳ `fiber_mutex` + `fiber_condition_variable` — prerequisite for
   `fiber_channel` and for any fiber code that needs shared mutable state.
3. ⏳ `fiber_channel<T>` — builds on mutex/condvar; enables structured
   producer/consumer patterns.

## Consequences

- Calling any OS-thread-blocking primitive from a fiberexec fiber is a
  latent bug. These primitives are the correct replacements and should be
  documented as such.
- `fiber_sync_wait` introduces a dependency on `boost::fibers::promise` and
  `boost::fibers::future` in the public-facing implementation (though not in
  the public header — Boost.Fiber types live in the `.cpp`).
- `fiber_mutex` and `fiber_condition_variable` wrappers keep Boost.Fiber out
  of public headers, which is consistent with how `fiber_pool` and the
  scheduler are hidden today.
