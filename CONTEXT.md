# CoroPact Context

CoroPact is a C++23 coroutine networking runtime. It owns coroutine
execution, backend-neutral transport contracts, a Linux epoll Reactor backend,
and a Linux io_uring backend. HTTP parsing, routing, proxying, and gateway
policy belong to [CoroGateway](https://github.com/archiyun/CoroGateway).

`docs/SUBSYSTEMS.md` is the normative dependency policy. Read it before
changing module boundaries.

## Language

**Lifecycle-Refined Coroutine I/O (LRCI)**:
The CoroPact model in which backend executions refine a shared logical I/O lifecycle before result readiness, continuation resumption, and resource release are observed.
_Avoid_: unified event model, CQE-as-completion

**Logical Operation**:
One application-observable asynchronous action, independent of how many backend attempts, kernel requests, or events implement it.
_Avoid_: request, CQE

**Backend Execution**:
The backend-specific process that implements one Logical Operation, including syscall attempts, readiness registration, kernel requests, and event interpretation.
_Avoid_: Logical Operation

**Physical Request**:
A request submitted to the kernel with its own physical terminal condition; a Logical Operation may use zero, one, or multiple Physical Requests.
_Avoid_: Logical Operation, Backend Event

**Backend Event**:
Backend evidence such as a syscall result, readiness notification, CQE, or cancellation acknowledgement.
_Avoid_: completion

**Result Readiness**:
The logical boundary after which the application-visible result or event is fixed.
_Avoid_: release, resume

**Continuation Authorization**:
The once-only boundary that permits the waiting coroutine to be scheduled for resumption.
_Avoid_: Result Readiness

**Physical Terminal**:
The boundary after which a Physical Request can produce no further event and can no longer access its borrowed resources.
_Avoid_: Result Readiness

**Close Preparation**:
An owner-local provisional exclusion of new operations before Close commits a physical drain. It may abort with a local submission error; a committed Close is irreversible.
_Avoid_: Close, cancellation

**Release Authorization**:
The once-only boundary that permits operation state, buffers, descriptors, or leases governed by the protocol to be released or reused.
_Avoid_: continuation completion

**Backend Refinement**:
The trace-preserving mapping from a backend-specific execution to the shared logical I/O specification; backend-internal transitions may project to stuttering steps.
_Avoid_: wrapper, event conversion

## Public seams

- `coro`: `Task`, `DetachedTask`, `Scheduler`, and `Work` define coroutine
  ownership and scheduling. This module does not own an event loop or an I/O
  queue.
- `operation/detail` is an internal logical-completion core. It may express
  once-only completion and scheduler-bound continuation resumption, but it
  must not own a result, fd, buffer, Channel, SQE, CQE, or application-visible
  operation type. `SingleResultLifecycle` is the 1-byte ordered protocol for
  coupled single-result paths: result readiness, release authorization, then
  continuation authorization. A logical completion gate is never reopened;
  reusable physical backend slots install a fresh gate only after their release
  point. Composite families record each physical member before authorizing one
  logical result; split-release families separately authorize result, release,
  and continuation.
- `backend/detail` holds backend-neutral awaiter result storage. It may retain
  a `Result<T>` across suspension, but it must not authorize result
  readiness, continuation resumption, release, or any transport protocol.
- `net`: header-only POSIX `Socket`, `Endpoint`, and buffers are
  backend-shared network values. It may use portable POSIX socket and `fcntl`
  operations, but must not depend on Linux-only facilities or a concrete
  readiness/completion backend. `net/detail` holds backend-neutral
  stream/source lifecycle accounting and is not an application seam.
- `Runtime`: the backend-neutral application composition root. Applications
  select a backend with `Runtime::Builder<runtime::Reactor>` or
  `Runtime::Builder<runtime::LUring>`; `Runtime::Create<Backend>` is the
  default-path shorthand. Runtime type-erases only cold start/stop control.
  It must not erase streams, awaiters, operations, or worker-local resources.
- `io` and `backend`: `AsyncStream`, `AsyncListener`, `AsyncConnector`, and
  `ManagedLoop` define application-visible transport and dispatcher-control
  contracts. `RequestStop` begins backend-specific cancellation and drain; it
  is not stream `Close` and does not by itself destroy application-owned
  resources. They must not require an application protocol. The `coropact/io.h` umbrella exports the
  backend-neutral stream, listener, buffer, receive-source, and algorithm
  contracts; connector and concrete backend headers are included explicitly by
  composition roots.
- `reactor` and `luring` are parallel Linux backend adapters. Do not make
  either backend depend on the `io` facade or on CoroGateway. A BSD kqueue
  backend is a third parallel adapter: it must refine the same contracts, not
  become a preprocessor branch inside `reactor`.

The Reactor public interface is `EventLoop` plus stream, listener, connector,
receive-source, and option adapters. Its `Runtime::Builder<runtime::Reactor>`
binding configures the common composition root; `reactor/detail` contains the
Linux epoll poller, channel registration, timer queue, and multi-worker
bootstrap machinery; applications must not depend on those types.

The luring public interface is `LUringLoop` plus stream, listener, connector,
receive-source, timer, and option adapters. Its
`Runtime::Builder<runtime::LUring>` binding configures the common composition
root; `luring/detail` owns raw SQE/CQE operations, ring and mailbox transport,
timer queue, and multi-worker server bootstrap machinery; applications must
not depend on those types.

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
