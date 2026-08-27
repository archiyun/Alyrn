# Runtime Subsystem Boundaries

This document is the normative dependency policy for Alyrn. It is a
networking runtime: HTTP parsing, routing, proxying, and gateway policy belong
to the separate [CoroGateway](https://github.com/archiyun/CoroGateway) project.

## Layer Model

```text
L3  applications and validation
    examples / tools / benchmarks / tests
                         |
                         v
L2  coroutine execution and network transport
    coro / net / io / epoll / uring / kqueue
                         |
                         v
L1  process services and value utilities
    time
                         |
                         v
L0  dependency-free foundations
    detail/base / detail/ds / detail/memory
```

Higher layers may depend on lower layers. A lower layer must never inspect an
application protocol, route, peer, proxy, or gateway policy.

## Directory Mapping

| Directory | Layer | Ownership |
|---|---:|---|
| `include/alyrn/detail/base`, `detail/ds`, `detail/memory` | L0 | Primitive values, intrusive structures, and pools used by Alyrn; not application seams. |
| `include/alyrn/time` | L1 | Public time values and timer identifiers; timer indexes and queues are implementation support. |
| `include/alyrn/coro` | L2 | Coroutine ownership, scheduling, frame allocation, and continuation rules. |
| `include/alyrn/detail/coro` | L2 | Promise storage, root-coroutine, and frame-allocation implementation; not an application seam. |
| `include/alyrn/detail/operation` | L2 | Internal completion and lifecycle authorization shared by backend adapters: one-shot, composite, and split-release protocols plus scheduler-bound continuations; no transport resource ownership. |
| `include/alyrn/detail/backend` | L2 | Internal backend-neutral awaiter result storage and loop state concepts; retains `Result<T>` but owns no transport-lifecycle protocol. |
| `include/alyrn/net` | L2 | Header-only POSIX socket, address, and buffer values shared by backends; no concrete backend or Linux-only dependency. |
| `include/alyrn/detail/net` | L2 | Internal stream/source lifecycle, admission, pause/drain, and lease-accounting state machines shared by backend adapters. |
| `include/alyrn/io` | L2 | User-facing backend-neutral I/O concepts and contracts. |
| `include/alyrn/runtime.h` | L2 | Backend-neutral application lifecycle facade. It type-erases only cold start/stop control; backend tags select a Builder specialization at compile time. |
| `include/alyrn/epoll`, `src/epoll` | L2 | Linux epoll readiness adapter. `Loop`, the `Runtime::Builder<runtime::Epoll>` binding, and transport adapters are public; `detail/epoll` owns channel registration, epoll polling, timers, and worker bootstrap implementation. |
| `include/alyrn/uring`, `src/uring` | L2 | Linux io_uring completion adapter. `Loop`, the `Runtime::Builder<runtime::Uring>` binding, and transport adapters are public; `detail/uring` owns raw SQE/CQE operations, ring/mailbox transport, timer queue, and worker/server bootstrap implementation. |
| `include/alyrn/kqueue`, `src/kqueue` | L2 | BSD/Darwin kqueue readiness adapter. `Loop`, the `Runtime::Builder<runtime::Kqueue>` binding, and transport adapters are public; `detail/kqueue` owns channel registration, kevent polling, timers, and master-slave worker bootstrap. |
| `examples`, `benchmarks`, `tests` | L3 | Consumers and validation; never runtime dependencies. |

## Hard Dependency Rules

- `detail/base`, `detail/ds`, and `detail/memory` must not depend on networking or any
  application-layer library.
- `detail/operation` may depend on backend-neutral runtime primitives, but not
  on `net`, `io`, Epoll, uring, or CoroGateway.
- `detail/backend` may retain backend-adapter result values, but it must not
  encode completion authorization or depend on `net`, `io`, Epoll, uring,
  or CoroGateway.
- `net` may depend on portable POSIX socket facilities, but must not depend on
  Linux-only facilities, `io`, Epoll, uring, or CoroGateway.
- Epoll, uring, and kqueue may depend on `net` and coroutine contracts, but
  none of them may include the application-level `io` facade.
- A kqueue backend is a parallel adapter. Do not add BSD conditionals to
  Epoll's epoll implementation; share only a genuinely backend-neutral
  contract or value module.
- `detail/kqueue` is not a supported application interface. Callers outside the
  kqueue implementation and validation code must use `Loop` and the
  public transport adapters instead.
- `io_contract` remains backend-neutral. The `io` facade may assemble adapters
  only at a composition root; backend cores must not link it.
- CoroGateway may depend on Alyrn public interfaces. Alyrn must not
  depend on CoroGateway.
- A backend extension belongs behind a separate contract or capability profile;
  it must not change the meaning of `ReadSome`, `WriteAll`, or `Close`.
- `ManagedLoop::RequestStop` is a thread-safe dispatcher-control request that
  begins backend cancellation/drain; it is not a resource `Close`. A backend
  must not treat `Stopped` as proof of fd, buffer, or coroutine-frame release.
- `detail/epoll` is not a supported application interface. It may depend on
  public Epoll adapter types, while callers outside the Epoll
  implementation, validation, and benchmark code must use `Loop` and the
  public transport adapters instead.
- `detail/uring` is not a supported application interface. It may depend on
  public uring adapter types, while callers outside the uring implementation
  and validation code must use `Loop` and the public transport adapters
  instead.
- `Runtime` is the sole application composition root. Backend selection stays
  compile-time through Builder tags; do not add a runtime backend enum,
  type-erased stream handler, implementation-tuning knobs, or a main macro
  without superseding ADR-0010.

## New Module Decision

Before adding a module, state its owned resource or invariant, its public
interface, and its permitted dependencies. If it needs HTTP messages, routing,
upstream peers, retries, or response policy, it belongs in CoroGateway rather
than Alyrn.
