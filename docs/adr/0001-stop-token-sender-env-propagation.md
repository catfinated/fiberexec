# ADR-0001: Stop-token propagation through the sender environment

**Status**: Implemented (step 4 — set_stopped_t mapping — deferred)

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

The receiver's stop token type is not always `std::stop_token` — `when_all`
uses `stdexec::inplace_stop_token` internally, and the two are not
convertible. The solution is a bridge: `operation` holds a
`std::stop_source fiber_stop_` and registers a stop callback on the
receiver's token (any type) that forwards to `fiber_stop_.request_stop()`.
The fiber receives `fiber_stop_.get_token()` (a `std::stop_token`), which
our async ops already accept. The stop_source starts with `std::nostopstate`
(no heap allocation) and gets a real shared state only if the receiver's
token is stoppable.

```cpp
template <class Receiver> struct operation {
    using stop_token_t = stdexec::stop_token_of_t<stdexec::env_of_t<Receiver>>;

    struct stop_forwarder {
        std::stop_source* src{nullptr};
        void operator()() const noexcept { src->request_stop(); }
    };
    using stop_cb_t = stdexec::stop_callback_for_t<stop_token_t, stop_forwarder>;

    Receiver rcvr;
    fiber_context* ctx{nullptr};
    std::stop_source fiber_stop_{std::nostopstate};
    std::optional<stop_cb_t> stop_cb_;

    void start() noexcept {
        auto env_tok = stdexec::get_stop_token(stdexec::get_env(rcvr));
        if (env_tok.stop_possible()) {
            fiber_stop_ = std::stop_source{};
            stop_cb_.emplace(env_tok, stop_forwarder{&fiber_stop_});
        }
        detail::schedule_task(ctx->pool(),
            [this, tok = fiber_stop_.get_token()]() mutable {
                detail::install_fiber_stop_token(std::move(tok));
                stdexec::set_value(std::move(this->rcvr));
            });
    }
};
```

With this change, any fiber launched via `schedule(sched)` automatically
inherits the stop token from its sender chain without any change to user code.

### 4. ECANCELED → set_stopped mapping (deferred)

The sender must advertise `set_stopped_t()` as a completion signature and
actually deliver it when a fiber's I/O is cancelled. This step is deferred
because the mapping strategy is non-trivial.

#### Who actually needs this today

Most users are **not blocked** by this gap. The common `when_all`-style
cancellation pattern already works end-to-end: the fibre wakes on ECANCELED,
the lambda catches it and returns normally, `when_all` receives `set_value`
from that branch and completes. No `set_stopped` signal is required.

The gap only matters when a downstream algorithm specifically branches on the
stopped completion — e.g. `let_stopped`, or a `sync_wait` caller that needs
to distinguish a cancelled outcome from a normal one. The deferred step 4
work should be prioritised when that use case arises.

#### How `then` propagates `set_stopped`

`stdexec::then` propagates a stop signal from upstream by **bypassing its lambda entirely**. When the upstream sender calls `set_stopped(then_receiver)`, `then` forwards `set_stopped` to its outer receiver without ever invoking the callable. The lambda never runs.

This is the key constraint: once `set_value(then_receiver)` has been called — meaning the lambda is already executing — `then` can only produce two outcomes:

- **`set_value`** — the lambda returns normally.
- **`set_error`** — the lambda throws an exception, which `then` catches and converts.

There is no third path. `set_stopped` cannot emerge from inside a running lambda regardless of how the fiber reports cancellation.

#### Exception vs. return value is orthogonal to the mapping problem

A natural instinct is to avoid exceptions and report cancellation via a return value instead — for example, having `async_read` return `std::expected<ssize_t, std::error_code>` and checking for `ECANCELED` in the lambda. This does not solve the `set_stopped` mapping problem.

The root cause is **timing**: by the time `async_read` (or any async op) detects that cancellation occurred, `then`'s `set_value` path is already active. Whether the lambda then throws or returns normally, `then` sees either `set_error` or `set_value` — the stopped signal is lost either way.

Switching from exceptions to return values converts `set_error` into `set_value`; it does not produce `set_stopped`. A custom adapter that controls the execution boundary above the lambda is required regardless of the reporting style chosen for individual async ops.

The two questions are independent:

| Question | Scope | Affects |
|---|---|---|
| Exception vs. return value | Async op API ergonomics | Calling code style, composability |
| `set_error` vs. `set_stopped` | Sender completion signal | stdexec algorithm behaviour |

Changing the async op API does not substitute for a custom adapter.

#### Why the obvious approach does not work

The natural instinct is to inject a receiver wrapper inside
`operation::start()` that intercepts `set_error` and remaps ECANCELED to
`set_stopped`. This fails because once `set_value(then_receiver)` is called,
`then` owns the error-routing path: it catches exceptions from the user's
lambda and calls `set_error` on its *outer* receiver directly, bypassing
any wrapper we placed between ourselves and `then_receiver`. The ECANCELED
exception never surfaces back to our code.

#### Option A — `fiberexec::fiber_then(fn)` adapter

A custom drop-in replacement for `stdexec::then` that controls the full
execution and error-routing path:

```cpp
stdexec::sync_wait(stdexec::schedule(sched) | fiberexec::fiber_then([] {
    fiberexec::async_read(fd, buf, 4);  // ECANCELED → set_stopped, not set_error
}));
```

Internally `fiber_then` wraps the callable in a try/catch:

```cpp
try {
    fn();
    stdexec::set_value(std::move(rcvr));
} catch (std::system_error const& e) {
    if (e.code().value() == ECANCELED) {
        stdexec::set_stopped(std::move(rcvr));
    } else {
        stdexec::set_error(std::move(rcvr), std::current_exception());
    }
} catch (...) {
    stdexec::set_error(std::move(rcvr), std::current_exception());
}
```

Pros: composes with all existing stdexec algorithms; transparent except for
the adapter name.
Cons: users must remember to use `fiber_then` instead of `then`; easy to
accidentally use `then` and lose the mapping.

#### Option B — `fiberexec::run(sched, fn)` compound sender

A single sender that combines schedule, execute, and error mapping in one
call — the canonical fiber entry point:

```cpp
stdexec::sync_wait(fiberexec::run(sched, [] {
    fiberexec::async_read(fd, buf, 4);
}));
```

`run` controls the full lifecycle: schedules on the pool, installs the
fiber-local stop token, runs `fn`, and maps ECANCELED to `set_stopped`.
It is essentially Option A with the `schedule` step inlined, presented as
a single composable unit.

Pros: single canonical entry point, no confusion between `then` and
`fiber_then`; lifecycle is explicit and correct by construction.
Cons: less composable with existing `schedule | then` pipelines (though
piping support could be added); requires API migration.

#### Option C — `fiberexec::map_cancelled_to_stopped(sender)` outer wrapper

A sender adapter applied at the composition site that intercepts
`set_error(exception_ptr)` calls containing an ECANCELED `system_error`
and remaps them to `set_stopped`:

```cpp
stdexec::sync_wait(
    fiberexec::map_cancelled_to_stopped(
        stdexec::schedule(sched) | stdexec::then(fn)));
```

This wraps the *output* of the full sender chain, so it sees `then`'s
`set_error` call and can remap it. Standard `then` can be used unchanged
inside.

Unlike Options A and B, this does not require a custom sender type —
`stdexec::let_error` is the standard-algorithm equivalent:

```cpp
stdexec::sync_wait(
    stdexec::schedule(sched)
    | stdexec::then(fn)
    | stdexec::let_error([](std::exception_ptr ex) {
          try { std::rethrow_exception(ex); }
          catch (std::system_error const& e) {
              if (e.code().value() == ECANCELED)
                  return /* just_stopped-typed sender */;
          }
          return /* error-typed sender wrapping ex */;
      }));
```

The practical difficulty is that both branches of the `let_error` lambda
must return senders with compatible completion signatures. `just_stopped()`
and "re-wrap as error" have different types, so the two paths need a variant
sender or type erasure to unify them. A thin `map_cancelled_to_stopped`
wrapper handles that without exposing the complexity at each call site.

Pros: works with unmodified `then` pipelines; purely additive.
Cons: verbose and easy to forget to apply; must be added at every
call site; does not update completion signatures automatically.

#### Recommendation

Option B (`run`) is the cleanest long-term API because the fiber lifecycle
and cancellation semantics are correct by default. Option A (`fiber_then`)
is a lower-friction migration path if composability with existing `then`
pipelines matters. Option C is a stop-gap that avoids API changes but
produces brittle call sites.

These options are not mutually exclusive: `run` can be implemented on top
of the same machinery as `fiber_then`.

## Implementation order

1. ✅ Add `fiber_stop_token` (fiber_specific_ptr) and `current_fiber_stop_token()`.
2. ✅ Update `async_read`, `async_write`, `async_sleep_for` to fall back to the
   fiber-local token — purely additive, does not break callers.
3. ✅ Update `operation::start()` to install the bridged stop token before
   invoking `set_value`.
4. ⏳ Add `set_stopped_t()` to completion signatures and decide on the
   ECANCELED → set_stopped mapping strategy (deferred).
5. ✅ Write tests that exercise the full chain: `when_all` error propagation
   cancelling a blocked `async_read` with no explicit stop token.

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
