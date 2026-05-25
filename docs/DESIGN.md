# fiberexec — Architecture

fiberexec is a C++20 fiber execution context built on Boost.Fiber and io_uring,
exposed as a `stdexec::scheduler`. Each unit of work runs as a cooperative fiber
on a pool of OS threads. Fibers that issue I/O suspend themselves without blocking
their OS thread; the thread resumes them when the kernel delivers the completion.

## Components

```
context          — public handle; owns the fiber_pool
  fiber_pool           — shared work queue + eventfd; spawns worker threads
    io_uring_scheduler — per-thread; owns the io_uring ring and drives the
                         Boost.Fiber scheduler loop via suspend_until()
```

### `fiber_pool`

Owns:
- A shared `std::queue<task>` protected by a `std::mutex`.
- A single shared `EFD_SEMAPHORE` eventfd used to wake worker threads when
  work is posted.
- A `boost::fibers::condition_variable` + `boost::fibers::mutex` used to
  signal shutdown to each thread's parked main fiber.

### `io_uring_scheduler`

One instance per worker thread, installed via
`boost::fibers::use_scheduling_algorithm<io_uring_scheduler>(...)`.

Owns:
- An `io_uring` ring (the thread's only ring; all I/O for fibers on this
  thread goes through it).
- A per-thread `EFD_SEMAPHORE` notify eventfd used exclusively to interrupt
  `io_uring_wait_cqe` from another OS thread.
- A `std::queue<boost::fibers::context*>` ready queue (mutex-protected, since
  `awakened()` can be called cross-thread during shutdown).

The thread-local pointer `tl_ring` is set to this ring on construction and
cleared on destruction, giving `async_read`/`async_write` access to the
current thread's ring without exposing the scheduler.

## Key data flows

### Work dispatch (`post` → fiber running)

```
caller: pool.post(task)
  → push task to shared queue
  → write(shared_eventfd, 1)          // EFD_SEMAPHORE: wakes exactly one thread

worker thread: io_uring_wait_cqe unblocks
  → drain_cqes() sees k_work_tag CQE
  → dequeue task from shared queue
  → boost::fibers::fiber(task).detach()
      → awakened() adds new fiber to ready_
  → arm_work_efd()                    // re-arm for next post()
  → suspend_until() returns
  → pick_next() returns new fiber
  → fiber runs
```

### Fiber I/O suspension (`async_read` / `async_write`)

```
fiber: async_read(fd, buf, len)
  → io_uring_get_sqe(tl_ring)
  → io_uring_prep_read(sqe, ...)
  → detail::submit_and_wait(sqe)
      → allocate io_awaitable on fiber stack
      → sqe->user_data = &awaitable     // pointer as CQE tag
      → io_uring_submit(tl_ring)
      → future.get()                    // suspends this fiber

  (fiber is now suspended; OS thread returns to scheduler)

kernel: I/O completes → CQE arrives in ring

worker thread: io_uring_wait_cqe unblocks
  → drain_cqes() sees non-sentinel tag
  → reinterpret_cast<io_awaitable*>(tag)->promise.set_value(res)
      → awakened() adds suspended fiber back to ready_
  → suspend_until() returns
  → pick_next() returns the resumed fiber
  → fiber continues from future.get(), gets res
```

Two sentinel tag values (`k_work_tag = 0`, `k_notify_tag = 1`) are reserved.
All `io_awaitable` pointers are heap-aligned and therefore ≥ 8, so they never
collide with the sentinels.

### Shutdown

```
~fiber_pool()
  → stop()
      → running_.store(false)
      → shutdown_cv_.notify_all()
          → for each thread's parked main fiber:
              awakened()              // adds main fiber to that thread's ready_
              notify()               // writes 1 to that thread's notify eventfd

  worker thread: io_uring_wait_cqe unblocks (notify eventfd CQE)
    → drain_cqes() sees k_notify_tag; re-arms notify eventfd
    → suspend_until() returns
    → pick_next() returns main fiber
    → main fiber: shutdown_cv_.wait() sees !running_, returns
    → worker() returns
    → scheduler destructor: io_uring_queue_exit(), close(notify_fd_)
    → OS thread exits

  → thr.join() returns for each thread
  → close(shared_eventfd)
```

The notify eventfd is separate from the shared work eventfd to keep the two
concerns independent. `notify()` must be able to interrupt `io_uring_wait_cqe`
without consuming a work dispatch token or interfering with the semaphore count.

## Thread affinity

A fiber is assigned to whichever thread first reads the shared work eventfd
token after `post()` — a kernel scheduling decision with no userspace control.
Once assigned, a fiber never migrates: its I/O SQEs go to that thread's ring,
and completions return to the same ring, so the fiber is always resumed on the
same thread.

## Known limitations

- **No graceful drain on shutdown.** Fibers sitting in the ready queue when
  `worker()` returns are abandoned. Fibers suspended on in-flight I/O are also
  abandoned: `io_uring_queue_exit` cancels their SQEs but the resulting
  `-ECANCELED` CQEs are never processed and their promises are never fulfilled.
- **No fiber migration.** There is no work-stealing; an idle thread cannot take
  fibers from a busy one.
- **Single fiber per `post()`.** One `post()` call wakes one thread and creates
  one fiber. There is no batching.
