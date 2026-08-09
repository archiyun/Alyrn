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
- `operation/detail` is an internal logical-completion core. It may express
  once-only completion and scheduler-bound continuation resumption, but it
  must not own a result, fd, buffer, Channel, SQE, CQE, or application-visible
  operation type. A logical completion gate is never reopened; reusable
  physical backend slots install a fresh gate only after their release point.
  Composite families record each physical member before authorizing one logical
  result; split-release families separately authorize result, release, and
  continuation.
- `net`: `Socket`, `Endpoint`, and buffers are backend-shared network values.
- `io` and `backend`: `AsyncStream`, `AsyncListener`, `AsyncConnector`, and
  `ManagedLoop` define application-visible transport and dispatcher-control
  contracts. `RequestStop` begins backend-specific cancellation and drain; it
  is not stream `Close` and does not by itself destroy application-owned
  resources. They must not require an application protocol. The `coropact/io.h` umbrella exports the
  backend-neutral stream, listener, buffer, receive-source, and algorithm
  contracts; connector and concrete backend headers are included explicitly by
  composition roots.
- `reactor` and `luring` are parallel backend adapters. Do not make either
  backend depend on the `io` facade or on CoroGateway.

The Reactor public interface is `EventLoop` plus its stream, listener,
connector, receive-source, and option adapters. `reactor/detail` contains the
epoll poller, channel registration, timer queue, and multi-worker bootstrap
machinery; applications must not depend on those types.

The luring public interface is `LUringLoop` plus its stream, listener,
connector, receive-source, timer, and option adapters. `luring/detail` owns
raw SQE/CQE operations, ring and mailbox transport, timer queue, and
multi-worker/server bootstrap machinery; applications must not depend on
those types.

## Ownership and thread affinity

- An `EventLoop` and all of its Channels, fds, timers, and stream state belong
  to one thread. Reactor callback, timer, and coroutine scheduling APIs are
  owner-thread-only; cross-thread delivery is a separate mailbox concern.
- An `LUringLoop` owns one ring and is bound to its creating worker thread.
  Ring submission, CQE dispatch, connection state, and timer mutation occur on
  that thread.
- A `Work*` is non-owning. Its owner must keep it alive until the work runs or
  the owner-side cancellation protocol makes it inert. Do not queue the same
  work twice.
- Continuations resume through their recorded scheduler. Cross-worker code
  must enqueue or post; it must not directly resume a foreign-loop coroutine.
- `RequestStop` is thread-safe and idempotent. `Run` is owner-thread-only;
  `Stopped` means the loop has drained its registered/pending backend
  operations, not that application-owned descriptors, leases, or objects have
  been destroyed. Object release remains a separate owner protocol.

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
behind an extension contract or backend-owned capability check and must not change the
meaning of the core stream operations.

## Validation

Run the default Reactor build for every change. Changes that touch shared
contracts, coroutine scheduling, or `luring` also require an io_uring-enabled
build and focused lifecycle tests. Network tests may require an environment
that permits socket creation and binding.
