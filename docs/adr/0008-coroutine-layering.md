# ADR-0008: Coroutine Core and Runtime Adapter Layering

## Status

Accepted for the initial coroutine implementation.

## Context

Alyrn must allow the same business coroutine to run on the epoll backend
and the io_uring backend. The coroutine layer therefore cannot own network
descriptors, submit I/O, or depend on a concrete runtime.

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

- `alyrn::Task<T>` is the lazy, backend-neutral coroutine function return type.
  It is move-only, can only be awaited as an rvalue, and transfers ownership of
  its frame to the awaiter.
- `Task<T>` transports exactly `T`; it does not automatically wrap values in
  `Result`.
- Fallible operations explicitly return `Task<Result<T>>`. The runtime
  uses the value-based error model rather than exceptions for ordinary business
  and I/O failures.
- `alyrn::Spawn` accepts a scheduler and a `Task<T>`, returning a
  `JoinHandle<T>`. `JoinHandle` supports synchronous `Wait`, asynchronous
  `co_await`, and `Detach`.
- `alyrn::DetachedTask` is the resultless fire-and-forget coroutine type.
  `alyrn::SpawnDetach` schedules it directly; the detached coroutine frame
  embeds its `ResumeWork` and has no join state.
- Promise and awaiter implementation details live under
  `include/alyrn/coro/detail/`.
- Epoll awaiters remain in the network adapter layer and may depend on
  `Loop` and epoll-specific operation state.
- io_uring awaiters remain in the luring adapter layer and may depend on the
  ring runtime.
- The dependency direction is one-way from lower primitives to concrete
  backends: `coro/foundation -> net -> backend -> epoll/uring`.
  `include/alyrn/backend` holds the adapter-contract concepts (`alyrn::backend`);
  CMake exposes them as `alyrn_io_contract`. The `io` module is the
  application facade of those concepts (`alyrn::io` aliases). Concrete
  backends link `alyrn_io_contract` and include `alyrn/backend/*.h`; they must
  not include `io` headers or link the `alyrn_io` target.
- `net::Buffer` owns backend-neutral segmented byte storage and accept-source
  admission state remains a network primitive. `io::Buffer` is the public
  zero-cost spelling for `net::Buffer`. The `io` facade may compose these lower
  modules but does not own their implementations.
- Business and protocol code may depend on `Task` and abstract asynchronous
  stream operations, but not on a concrete I/O backend.

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
| `detail/completion_gate.h`, `detail/*_lifecycle.h`, `detail/scheduler_continuation.h` | Backend-neutral completion gates, composite/split-release lifecycle helpers, and scheduler-bound continuations; never fd, buffer, or CQE ownership |
| `backend/*.h` | Adapter-contract concepts and awaiter result storage (`alyrn::backend`); no `alyrn/backend.h` umbrella |

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

## 修订（2026-08）

Promise 与 spawn 实现位于 `include/alyrn/coro/detail/`（公开模块下的
`detail/`，与 `epoll/detail`、`condy/detail` 同一布局）。完成协议仍在
`include/alyrn/detail/`，因为没有公开的 `operation` 模块。
adapter 契约在 `include/alyrn/backend`（`alyrn_io_contract`）；`io` 只是应用侧别名。
具体后端链接 `alyrn_io_contract`、不链接 `alyrn_io`。
