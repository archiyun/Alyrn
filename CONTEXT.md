# CoroPact Context

CoroPact is a C++23 coroutine networking runtime. It owns coroutine
execution, backend-neutral transport contracts, an epoll Reactor backend, and
an io_uring backend. HTTP parsing, routing, proxying, and gateway policy belong
to [CoroGateway](https://github.com/archiyun/CoroGateway).

`docs/SUBSYSTEMS.md` is the normative dependency policy. Read it before
changing module boundaries.

## Public seams

- `coro`: `Task`, `DetachedTask`, `Scheduler`, and `Work` define coroutine
  ownership and scheduling. This module does not own an event loop or an I/O
  queue.
- `net`: `Socket`, `Endpoint`, and buffers are backend-shared network values.
- `io` and `backend`: `AsyncStream`, `AsyncListener`, and `AsyncConnector`
  define application-visible transport contracts. They must not require an
  application protocol.
- `reactor` and `luring` are parallel backend adapters. Do not make either
  backend depend on the `io` facade or on CoroGateway.

## Ownership and thread affinity

- An `EventLoop` and all of its Channels, fds, timers, and stream state belong
  to one thread. Cross-thread Reactor work uses documented posting methods.
- An `LUringLoop` owns one ring and is bound to its creating worker thread.
  Ring submission, CQE dispatch, connection state, and timer mutation occur on
  that thread.
- A `Work*` is non-owning. Its owner must keep it alive until the work runs or
  the owner-side cancellation protocol makes it inert. Do not queue the same
  work twice.
- Continuations resume through their recorded scheduler. Cross-worker code
  must enqueue or post; it must not directly resume a foreign-loop coroutine.

## Coroutine frames

`Scheduler::Run` installs both `Scheduler::Current()` and the scheduler's
frame memory resource for the duration of a work item. Coroutine frames created
while a worker resumes code therefore use that worker's resource. Frame pools
are worker-local unless an implementation explicitly supplies synchronization.
Do not move a suspended frame to a worker whose allocator or scheduler
ownership has not been established.

## io_uring mailbox and MSG_RING

Cross-ring messages enter the target loop's bounded MPSC mailbox. A producer
that receives `kQueuedNeedsNotification` must submit the corresponding
`MSG_RING` notification; the target loop drains the mailbox only on its owner
thread. Notification coalescing is part of the mailbox protocol: do not clear
or re-arm its state outside that protocol, and handle failed notification
submission through the retry path.

## Adding a module or feature

Before adding a module, document its owned resource, interface, lifetime and
thread-affinity rules, permitted dependencies, and tests. Features involving
HTTP messages, routing, upstream selection, retry policy, caching policy, or
response generation belong in CoroGateway. Backend-specific capabilities belong
behind an extension contract or capability profile and must not change the
meaning of the core stream operations.

## Validation

Run the default Reactor build for every change. Changes that touch shared
contracts, coroutine scheduling, or `luring` also require an io_uring-enabled
build and focused lifecycle tests. Network tests may require an environment
that permits socket creation and binding.
