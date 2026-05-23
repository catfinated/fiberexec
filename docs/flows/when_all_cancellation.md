# Flow: when_all cancellation propagating into a blocked fiber

This document traces what happens when one branch of `stdexec::when_all`
errors while another branch is suspended inside a fiberexec async op.

## Setup

```cpp
stdexec::sync_wait(stdexec::when_all(
    // Fiber A — blocked in async_read, no explicit stop token
    stdexec::schedule(sched) | stdexec::then([&] {
        fiberexec::async_read(read_fd, buf.data(), buf.size());
    }),
    // Fiber B — sleeps briefly, then throws
    stdexec::schedule(sched) | stdexec::then([&] {
        fiberexec::async_sleep_for(10ms);
        throw std::runtime_error("trigger cancel");
    })));
```

## Actors

| Actor | Lives on |
|---|---|
| Main thread | Blocked in `sync_wait` |
| Worker thread T1 | Runs fiber A |
| Worker thread T2 | Runs fiber B |
| `when_all` shared stop source | Shared state, touched from T1 and T2 |
| `operation<Receiver>` for fiber A | Main thread stack (inside `sync_wait`) |

## Phase 1 — startup (main thread)

1. `sync_wait` connects the `when_all` sender to its internal receiver and
   calls `start()` on the resulting operation state.
2. `when_all` calls `start()` on each child operation state in turn.
3. Each `schedule_sender::operation::start()` posts a task to the fiber pool
   and **returns immediately**. No fibers are running yet.
4. The main thread enters `sync_wait`'s wait loop and blocks.

## Phase 2 — fiber A starts (T1)

5. T1 picks up fiber A's task and creates a Boost.Fiber.
6. Before calling `set_value`, `operation::start()`'s lambda installs
   `fiber_stop_.get_token()` into fiber-local storage via
   `install_fiber_stop_token()`. This token is backed by the operation's
   `std::stop_source fiber_stop_`, which is bridged to `when_all`'s shared
   stop token via `stop_cb_`.
7. `stdexec::set_value(rcvr)` is called, triggering `then`'s receiver, which
   calls the user's lambda.
8. The lambda calls `async_read`. Since no explicit token was passed,
   `async_read` falls back to `current_fiber_stop_token()` and gets the
   fiber-local token. A stop callback is registered on this token inside
   `submit_and_wait`; if fired it will call
   `tl_scheduler->request_cancel(&awaitable)` on T1's scheduler.
9. `async_read` submits a read SQE to T1's io_uring ring and calls
   `future.get()`. Fiber A suspends cooperatively. T1 enters
   `io_uring_wait_cqe`.

## Phase 3 — fiber B errors (T2)

10. T2 picks up fiber B's task and creates a Boost.Fiber.
11. `install_fiber_stop_token()` installs B's fiber-local token.
12. `stdexec::set_value(rcvr)` triggers B's `then` receiver, which calls B's
    lambda.
13. `async_sleep_for(10ms)` suspends fiber B. After 10 ms the timeout CQE
    arrives, T2 drains it, resumes fiber B.
14. The lambda throws `std::runtime_error("trigger cancel")`.

**Key point**: the exception does not need to cross a thread boundary. It is
thrown and caught entirely on T2, inside the synchronous call chain that
started with step 12.

```
T2 call stack at the moment of throw:

  user lambda                       ← throw std::runtime_error
  then_receiver::set_value()        ← try/catch wraps the lambda call
    catch(...) {
      stdexec::set_error(           ← converts exception to set_error signal
          when_all_receiver_B,
          std::current_exception())
    }
  when_all_receiver_B::set_error()  ← runs synchronously on T2
    store exception
    shared_stop_source.request_stop()   ── ① triggers stop propagation
    pending_count.fetch_sub(1)          ── now 1 (fiber A still running)
    // can't deliver result yet
  ← set_error returns
  ← then_receiver::set_value() returns
  ← schedule_task lambda returns
  ← fiber B's function returns, fiber B exits
```

In stdexec, completion signals (`set_value`, `set_error`, `set_stopped`)
propagate **synchronously** through the receiver chain from wherever the
completion originates. There is no message passing or thread handoff.

## Phase 4 — stop propagates to fiber A (T2 → T1)

① `shared_stop_source.request_stop()` fires all registered stop callbacks
synchronously, still on T2:

```
shared_stop_source.request_stop()
  → operation<ReceiverA>::stop_cb_.callback()   (stop_forwarder)
      fiber_stop_A.request_stop()
        → submit_and_wait's stop_callback fires
            tl_scheduler_A->request_cancel(&awaitable_A)
              push awaitable_A* to T1's cancel_queue
              write(T1->notify_fd, 1)            ← wakes T1
```

T1 is blocked in `io_uring_wait_cqe`. The write to `notify_fd` generates a
CQE in T1's ring, unblocking it.

## Phase 5 — fiber A is cancelled (T1)

15. T1 wakes, drains CQEs. Sees the notify CQE.
16. `drain_cqes` calls `flush_cancel_queue()`, which gets an SQE and preps
    `IORING_OP_ASYNC_CANCEL` targeting `&awaitable_A`, then calls
    `arm_notify()` which submits both SQEs together.
17. The kernel cancels the pending read SQE. Two CQEs arrive:
    - Cancel op CQE (`k_cancel_tag`, res = 0): ignored.
    - Original read CQE (`&awaitable_A`, res = -ECANCELED): routes to
      `awaitable_A.promise.set_value(-ECANCELED)`.
18. Fiber A resumes from `future.get()`. `async_read` sees `res < 0` and
    throws `std::system_error(ECANCELED, ...)`.
19. The test's `try/catch` inside the lambda catches it and sets
    `auto_cancelled = true`. The lambda returns normally.
20. `then_receiver_A::set_value()` sees no exception and calls
    `set_value(when_all_receiver_A)`.
21. `when_all_receiver_A::set_value()` decrements `pending_count` to 0.
    All children are done. `when_all` delivers the stored error from fiber B
    to `sync_wait`'s receiver — synchronously on T1.
22. `sync_wait` stores the error and wakes the main thread.

## Phase 6 — main thread resumes

23. The main thread unblocks, `sync_wait` rethrows the stored exception from
    fiber B. The test catches it and checks `auto_cancelled`.

## Summary

```
Main thread          Worker T1 (fiber A)          Worker T2 (fiber B)
─────────────────    ────────────────────          ──────────────────
sync_wait blocks
                     install fiber-local token
                     async_read → SQE submitted
                     future.get() → suspended
                     T1 in io_uring_wait_cqe
                                                   install fiber-local token
                                                   async_sleep_for(10ms)
                                                   fiber B suspended
                                                   ── 10 ms ──
                                                   fiber B resumes
                                                   throw runtime_error
                                                   then catches →
                                                     set_error(when_all_rcvr_B)
                                                       request_stop() ──────┐
                                                       pending: 2→1         │
                                                   fiber B exits            │
                     ←─────── notify_fd write ◄───────────────────────────-┘
                     wake from io_uring_wait_cqe
                     flush_cancel_queue →
                       IORING_OP_ASYNC_CANCEL
                     read SQE cancelled (ECANCELED)
                     future.get() returns -ECANCELED
                     fiber A: catches ECANCELED
                       auto_cancelled = true
                     set_value(when_all_rcvr_A)
                       pending: 1→0
                       deliver error to sync_wait
sync_wait unblocks
rethrow / test checks
auto_cancelled == true ✓
```
