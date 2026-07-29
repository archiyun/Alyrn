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
    coro / net / io / reactor / luring
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
| `include/coropact/net`, `src/net` | L2 | Socket and address values shared by backends. |
| `include/coropact/io`, `include/coropact/backend` | L2 | Backend-neutral I/O contracts and algorithms. |
| `include/coropact/reactor`, `src/reactor` | L2 | epoll readiness backend. |
| `include/coropact/luring`, `src/luring` | L2 | io_uring completion backend. |
| `examples`, `benchmarks`, `tests` | L3 | Consumers and validation; never runtime dependencies. |

## Hard Dependency Rules

- `base`, `ds`, and `memory` must not depend on networking or any
  application-layer library.
- `net` must not depend on `io`, Reactor, luring, or CoroGateway.
- Reactor and luring may depend on `net` and coroutine contracts, but neither
  may include the application-level `io` facade.
- `io_contract` remains backend-neutral. The `io` facade may assemble adapters
  only at a composition root; backend cores must not link it.
- CoroGateway may depend on CoroPact public interfaces. CoroPact must not
  depend on CoroGateway.
- A backend extension belongs behind a separate contract or capability profile;
  it must not change the meaning of `ReadSome`, `WriteSome`, or `Close`.

## New Module Decision

Before adding a module, state its owned resource or invariant, its public
interface, and its permitted dependencies. If it needs HTTP messages, routing,
upstream peers, retries, or response policy, it belongs in CoroGateway rather
than CoroPact.
