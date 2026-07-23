# ADR-008: Coroutine Core and Runtime Adapter Layering

## Status

Accepted for the initial coroutine implementation.

## Context

CoroPact must allow the same business coroutine to run on the epoll/Reactor backend
and a future io_uring backend. The coroutine return type therefore cannot own
network descriptors, submit I/O, or depend on either runtime.

Condy separates a lazy structured coroutine (`Coro<T>`) from a coroutine that
has already been submitted to a runtime (`Task<T>`). CoroPact adopts that semantic
boundary while retaining its value-based error model.

## Decision

- `coro::Coro<T>` is the lazy, backend-neutral coroutine function return type.
- `Coro<T>` transports exactly `T`; it does not automatically wrap values in
  `base::Result`.
- Fallible operations explicitly return `Coro<base::Result<T>>`.
- `coro::Spawn` accepts a scheduler and a `Coro<void>`. Restricting detached
  roots to `void` prevents returned values or errors from being discarded
  accidentally.
- Promise and awaiter implementation details live under `coro/detail/`.
- Reactor awaiters remain in the network adapter layer and may depend on
  `Channel` and `EventLoop`.
- io_uring awaiters remain in the luring adapter layer and may depend on the
  ring runtime.
- Business and protocol code may depend on `Coro` and abstract asynchronous
  stream operations, but not on either concrete I/O backend.

## File Responsibilities

| File | Responsibility |
| --- | --- |
| `coro/concepts.h` | Backend-neutral coroutine and scheduler concepts |
| `coro/coro.h` | Public lazy coroutine owner |
| `coro/detail/coro.h` | Promise, symmetric transfer, result storage |
| `coro/spawn.h` | Detached root submission through a scheduler |
| `coro/sync_wait.h` | Synchronous no-I/O test helper |
| `coro/task.h` | Temporary compatibility name; reserved for joinable tasks |

## Deferred Work

A real `Task<T>` will represent concurrently running, joinable work. It must
not be implemented as another name for `Coro<T>`. Its design must first define:

- join and detach ownership states;
- completion racing with join or detach;
- cancellation of pending I/O;
- scheduler affinity when a task and its waiter run on different runtimes;
- runtime shutdown behavior for queued and suspended coroutine handles.

