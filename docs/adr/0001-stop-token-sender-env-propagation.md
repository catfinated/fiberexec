# ADR-0001: Stop-token propagation through the sender environment

**Status**: Pending

## Context

`async_read`, `async_write`, and `async_sleep_for` accept an explicit
`std::stop_token` that, when cancelled, submits an `IORING_OP_ASYNC_CANCEL`
SQE and causes the suspended fiber to throw `std::system_error(ECANCELED)`.
This works correctly in isolation.

The gap is that nothing connects the explicit stop token to the stop token
that stdexec propagates through the receiver environment. When a caller uses:

```cpp
stdexec::sync_wait(stdexec::schedule(sched) | stdexec::then([] {
    fiberexec::async_read(fd, buf, len);  // no stop token
}));
```

`sync_wait` creates a `stop_source` internally and makes its token available
via `get_stop_token(get_env(rcvr))`. That token propagates down through the
receiver chain so algorithms like `when_all` can cancel remaining branches
when one branch errors. But `stdexec::then` calls the lambda as `callable()`
— it does not thread the receiver environment into the callable's arguments.
So the stop token from `sync_wait` never reaches our async calls. The fiber
can be left blocked in io_uring while stdexec considers the operation done.

## Problem

1. `when_all` cancellation does not propagate into fiber async ops. If one
   branch errors, `when_all` requests stop on the others, but fibers still
   blocking in `async_read` etc. have no way to learn this.

2. Callers who want cancellation today must construct their own
   `std::stop_source` out-of-band and pass the token by hand to every async
   call, which is error-prone and disconnected from stdexec's model.

## Proposed solution

### 1. Per-fiber stop-token storage

Use `boost::fibers::fiber_specific_ptr<std::stop_token>` to store a stop
token that is scoped to the running fiber, not the OS thread. This is the
fiber-level analogue of `tl_ring` (thread-local ring pointer).

A thread-local is wrong here: multiple fibers share a thread and context-
switch cooperatively, so a single `tl_stop_token` would be overwritten on
each switch.

Add to `src/fiber_context.cpp`:

```cpp
namespace detail {
// Per-fiber token set by the scheduler when a fiber is launched.
// fiber_specific_ptr destructs the pointee when the fiber exits.
boost::fibers::fiber_specific_ptr<std::stop_token> fiber_stop_token;

std::stop_token current_fiber_stop_token() {
    auto* p = fiber_stop_token.get();
    return p ? *p : std::stop_token{};
}
} // namespace detail
```

### 2. Automatic fallback in async ops

Update `async_read` etc. to fall back to the fiber-local token when the
caller passes an empty one:

```cpp
ssize_t async_read(int fd, void* buf, std::size_t len, std::stop_token st) {
    if (!st.stop_possible()) {
        st = detail::current_fiber_stop_token();
    }
    // ... existing implementation
}
```

This keeps the explicit-token path working unchanged; callers who pass their
own token continue to get exactly that behavior.

### 3. Custom `fiber_schedule` sender adapter

The receiver environment carries the stop token, but `stdexec::then` does
not expose it to the callable. We need a custom operation state that extracts
the token before launching the fiber.

Replace (or supplement) the inner `operation::start()` in
`fiber_context::scheduler::schedule_sender`:

```cpp
void start() noexcept {
    // Extract the stop token from the connected receiver's environment.
    auto st = stdexec::get_stop_token(stdexec::get_env(rcvr));

    detail::schedule_task(ctx->pool(), [this, st = std::move(st)]() mutable {
        // Install the token as the fiber-local stop token before running
        // the receiver. It is removed automatically when the fiber exits
        // because fiber_specific_ptr owns the pointee.
        detail::fiber_stop_token.reset(new std::stop_token(std::move(st)));
        stdexec::set_value(std::move(this->rcvr));
    });
}
```

With this change, any fiber launched via `schedule(sched)` automatically
inherits the stop token from its sender chain without any change to user code.

### 4. Completion signatures

The sender must also advertise `set_stopped()` as a completion signature so
stdexec algorithms know the fiber can be cancelled:

```cpp
using completion_signatures = stdexec::completion_signatures<
    stdexec::set_value_t(),
    stdexec::set_stopped_t()>;
```

And the fiber body should catch `std::system_error(ECANCELED)` and call
`stdexec::set_stopped(std::move(rcvr))` instead of letting the exception
propagate — or wrap the whole fiber body in a try/catch that maps ECANCELED
to `set_stopped`.

## Implementation order

1. Add `fiber_stop_token` (fiber_specific_ptr) and `current_fiber_stop_token()`.
2. Update `async_read`, `async_write`, `async_sleep_for` to fall back to the
   fiber-local token — this is purely additive and does not break callers.
3. Update `operation::start()` to install the receiver's stop token before
   invoking `set_value`.
4. Add `set_stopped_t()` to completion signatures and decide on the
   ECANCELED → set_stopped mapping strategy.
5. Write tests that exercise the full chain: `sync_wait` with a stop request,
   `when_all` error propagation cancelling a blocked `async_read`.

## Consequences

- User code that does not use cancellation is unaffected (fallback to empty
  token is a no-op).
- Fibers launched via `schedule(sched) | then(lambda)` automatically cancel
  their in-flight I/O when the sender graph is cancelled — no explicit stop
  token threading required.
- The `set_stopped_t` completion signature change may affect downstream
  `when_all` / `sync_wait` behavior: callers must be prepared for a stopped
  outcome, not just value/error.
- `fiber_specific_ptr` involves a heap allocation per fiber that uses a stop
  token. For fibers that never call async ops this is wasted; evaluate whether
  lazy initialisation (only install if the token is stoppable) is worth it.
