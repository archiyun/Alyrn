# Runtime Subsystem Boundaries

This document is the normative dependency policy for CoroPact. It is a
networking runtime: HTTP parsing, routing, proxying, and gateway policy belong
to the separate [CoroGateway](https://github.com/archiyun/CoroGateway) project.

## Layer Model

```text
L3  applications and validation
    examples / tools / benchmarks / tests
                         |
                         v
L2  coroutine execution and network transport
    coro / net / io / reactor / luring / kqueue
                         |
                         v
L1  process services and value utilities
    time
                         |
                         v
L0  dependency-free foundations
    base / ds / memory
```

Higher layers may depend on lower layers. A lower layer must never inspect an
application protocol, route, peer, proxy, or gateway policy.

## Directory Mapping

| Directory | Layer | Ownership |
|---|---:|---|
| `include/coropact/base`, `ds`, `memory` | L0 | Primitive values, intrusive structures, and pools. |
| `include/coropact/time` | L1 | Time values and timer indexes; no fd or event-loop ownership. |
| `include/coropact/coro` | L2 | Coroutine ownership, scheduling, frame allocation, and continuation rules. |
| `include/coropact/coro/detail` | L2 | Promise storage, root-coroutine, and frame-allocation implementation; not an application seam. |
| `include/coropact/operation/detail` | L2 | Internal completion and lifecycle authorization shared by backend adapters: one-shot, composite, and split-release protocols plus scheduler-bound continuations; no transport resource ownership. |
| `include/coropact/backend/detail` | L2 | Internal backend-neutral awaiter result storage; retains `Result<T>` but owns no authorization or transport-lifecycle protocol. |
| `include/coropact/net` | L2 | Header-only POSIX socket, address, and buffer values shared by backends; no concrete backend or Linux-only dependency. |
| `include/coropact/net/detail` | L2 | Internal stream/source lifecycle, admission, pause/drain, and lease-accounting state machines shared by backend adapters. |
| `include/coropact/io`, `include/coropact/backend` | L2 | Backend-neutral I/O and dispatcher lifecycle contracts and algorithms. |
| `include/coropact/runtime.h` | L2 | Backend-neutral application lifecycle facade. It type-erases only cold start/stop control; backend tags select a Builder specialization at compile time. |
| `include/coropact/reactor`, `src/reactor` | L2 | Linux epoll readiness adapter. `Loop`, the `Runtime::Builder<runtime::Reactor>` binding, and transport adapters are public; `reactor/detail` owns channel registration, epoll polling, timers, and worker bootstrap implementation. |
| `include/coropact/luring`, `src/luring` | L2 | Linux io_uring completion adapter. `Loop`, the `Runtime::Builder<runtime::LUring>` binding, and transport adapters are public; `luring/detail` owns raw SQE/CQE operations, ring/mailbox transport, timer queue, and worker/server bootstrap implementation. |
| `include/coropact/kqueue`, `src/kqueue` | L2 | BSD/Darwin kqueue readiness adapter. `Loop`, the `Runtime::Builder<runtime::Kqueue>` binding, and transport adapters are public; `kqueue/detail` owns channel registration, kevent polling, timers, and master-slave worker bootstrap. |
| `examples`, `benchmarks`, `tests` | L3 | Consumers and validation; never runtime dependencies. |

## Hard Dependency Rules

- `base`, `ds`, and `memory` must not depend on networking or any
  application-layer library.
- `operation/detail` may depend on backend-neutral runtime primitives, but not
  on `net`, `io`, Reactor, luring, or CoroGateway.
- `backend/detail` may retain backend-adapter result values, but it must not
  encode completion authorization or depend on `net`, `io`, Reactor, luring,
  or CoroGateway.
- `net` may depend on portable POSIX socket facilities, but must not depend on
  Linux-only facilities, `io`, Reactor, luring, or CoroGateway.
- Reactor, luring, and kqueue may depend on `net` and coroutine contracts, but
  none of them may include the application-level `io` facade.
- A kqueue backend is a parallel adapter. Do not add BSD conditionals to
  Reactor's epoll implementation; share only a genuinely backend-neutral
  contract or value module.
- `kqueue/detail` is not a supported application interface. Callers outside the
  kqueue implementation and validation code must use `Loop` and the
  public transport adapters instead.
- `io_contract` remains backend-neutral. The `io` facade may assemble adapters
  only at a composition root; backend cores must not link it.
- CoroGateway may depend on CoroPact public interfaces. CoroPact must not
  depend on CoroGateway.
- A backend extension belongs behind a separate contract or capability profile;
  it must not change the meaning of `ReadSome`, `WriteAll`, or `Close`.
- `ManagedLoop::RequestStop` is a thread-safe dispatcher-control request that
  begins backend cancellation/drain; it is not a resource `Close`. A backend
  must not treat `Stopped` as proof of fd, buffer, or coroutine-frame release.
- `reactor/detail` is not a supported application interface. It may depend on
  public Reactor adapter types, while callers outside the Reactor
  implementation, validation, and benchmark code must use `Loop` and the
  public transport adapters instead.
- `luring/detail` is not a supported application interface. It may depend on
  public luring adapter types, while callers outside the luring implementation
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
than CoroPact.
