# ADR-0008: Coroutine Core and Runtime Adapter Layering

## Status

Accepted for the initial coroutine implementation.

## Context

CoroPact must allow the same business coroutine to run on the epoll/Reactor backend
and the io_uring backend. The coroutine layer therefore cannot own network
descriptors, submit I/O, or depend on either concrete runtime.

The public coroutine API has three distinct roles:

- `Task<T>` is a lazy, move-only child coroutine. It produces a value and is
  consumed exactly once by `co_await` or by `Spawn`.
- `DetachedTask` is a lazy, resultless coroutine intended for fire-and-forget
  execution. `SpawnDetach` schedules it and owns no join state.
- `JoinHandle<T>` represents the externally running, joinable work created by
  `Spawn`. Its root frame contains the join state and resume work.

The distinction is based on ownership and execution lifetime, not on a separate
`Coro<T>` type. `Task<T>` remains backend-neutral and knows nothing about a
scheduler or I/O backend.

## Decision

- `coro::Task<T>` is the lazy, backend-neutral coroutine function return type.
  It is move-only, can only be awaited as an rvalue, and transfers ownership of
  its frame to the awaiter.
- `Task<T>` transports exactly `T`; it does not automatically wrap values in
  `Result`.
- Fallible operations explicitly return `Task<Result<T>>`. The runtime
  uses the value-based error model rather than exceptions for ordinary business
  and I/O failures.
- `coro::Spawn` accepts a scheduler and a `Task<T>`, returning a
  `JoinHandle<T>`. `JoinHandle` supports synchronous `Wait`, asynchronous
  `co_await`, and `Detach`.
- `coro::DetachedTask` is the resultless fire-and-forget coroutine type.
  `coro::SpawnDetach` schedules it directly; the detached coroutine frame
  embeds its `ResumeWork` and has no join state.
- Promise and awaiter implementation details live under `coro/detail/`.
- Reactor awaiters remain in the network adapter layer and may depend on
  `EventLoop` and Reactor-specific operation state.
- io_uring awaiters remain in the luring adapter layer and may depend on the
  ring runtime.
- The dependency direction is one-way from lower primitives to concrete
  backends: `coro/foundation -> net -> Reactor/luring`. The `io` module is a
  higher-level contract/facade layer and may depend on `net` or a selected
  backend, but concrete backends must not include `io` headers or link the
  `coropact_io` target.
- `net::Buffer` owns backend-neutral segmented byte storage and accept-source
  admission state remains a network primitive. `io::Buffer` is the public
  zero-cost spelling for `net::Buffer`. The `io` facade may compose these lower
  modules but does not own their implementations.
- Business and protocol code may depend on `Task` and abstract asynchronous
  stream operations, but not on either concrete I/O backend.

## File Responsibilities

| File | Responsibility |
| --- | --- |
| `coro.h` | Public umbrella header for backend-neutral coroutine primitives |
| `coro/awaitable.h` | Awaiter and awaitable concepts used by coroutine adapters |
| `coro/task.h` | Lazy, result-producing child coroutine and symmetric transfer |
| `coro/detached_task.h` | Lazy, resultless fire-and-forget coroutine |
| `coro/spawn.h` | `Spawn`, `JoinHandle`, `SpawnDetach`, and internal root frames |
| `coro/sync_wait.h` | Synchronous no-I/O test helper for `Task<T>` |
| `coro/scheduler.h` | Backend-neutral scheduling boundary |
| `coro/work.h` | Schedulable work and coroutine resume adapter |
| `coro/frame_allocator.h` | Coroutine-frame allocation through PMR resources |
| `coro/detail/promise_base.h` | Shared lazy coroutine promise and continuation protocol |
| `coro/detail/spawn_state.h` | Joinable root result, detach, waiter, and ownership state embedded in the root frame |
| `coro/detail/spawn_root.h` | Joinable root coroutine, promise, final-suspend, and root-frame destruction protocol |
| `operation/detail/` | Backend-neutral completion gates, composite/split-release lifecycle helpers, and scheduler-bound continuations; never fd, buffer, or CQE ownership |

## Remaining Boundaries

The following are deliberate limits or require separate design work:

- `JoinHandle` does not expose cancellation. Detaching or destroying it only
  relinquishes observation and result ownership; it never cancels the root
  task or a pending I/O operation.
- A parked `JoinHandle` waiter resumes through its captured `Scheduler`, and
  backend awaiters use scheduler-bound continuations. General task migration
  across workers is intentionally not a `Task<T>` feature; a backend must use
  its explicit owner-transfer protocol such as a mailbox.
- `RequestStop()` begins backend cancellation and completion drain. A stopped
  loop is not proof that application-owned streams, leases, or coroutine
  frames have been destroyed; their owner protocols remain responsible for
  final release.
- io_uring extensions now include composite close/timeout convergence,
  multishot accept and receive sources, provided-buffer leases, and split
  zero-copy send completion. Future extensions must refine the same logical
  completion/lifetime protocol without changing `Task<T>` ownership semantics.
