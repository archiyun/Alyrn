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
  `base::Result`.
- Fallible operations explicitly return `Task<base::Result<T>>`. The runtime
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
- `net::Buffer` owns backend-neutral byte storage and accept-source admission
  state remains a network primitive. `io::Buffer` is
  the public zero-cost spelling for `net::Buffer`. The `io` facade may compose
  these lower modules but does not own their implementations.
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
| `coro/detail/spawn_state.h` | Join, detach, waiter, and root-frame ownership state |

## Deferred Work

The following remain deferred or require separate design work:

- cancellation and propagation through `JoinHandle` and pending I/O;
- scheduler affinity when a task and its waiter run on different workers;
- runtime shutdown behavior for queued and suspended coroutine handles;
- completion-family extensions such as multishot operations, provided buffers,
  and split send/notification completions. These belong to the backend adapter
  layer and must not change the ownership semantics of `Task<T>`.
