# Runtime Subsystem Boundaries

This document is the normative dependency and maintenance policy for the
runtime. It describes where code belongs, which direction dependencies may
flow, and what an AI-generated patch must preserve.

## Layer Model

```text
L4  applications
    examples / tools / benchmarks / tests
                         |
                         v
L3  gateway policy and orchestration
    gateway/server
    gateway/proxy
    gateway/health
    gateway/rate_limit
    gateway/circuit_breaker
    gateway/upstream
    gateway/load_balance
                         |
                         v
L2  execution, transport, and protocol
    task / coro / net / uring / http
                         |
                         v
L1  process services and value utilities
    time / log / metrics / config
                         |
                         v
L0  dependency-free foundations
    base / ds / memory-core
```

The layer number is an ownership rule, not a statement about performance or
importance. Higher layers may use lower layers. Lower layers must never know
about higher-layer policy.

## Current Directory Mapping

| Current directory | Layer | Notes |
|---|---:|---|
| `include/runtime/base` | L0 | Noncopyable and thread identity primitives. |
| `include/runtime/ds` | L0 | Header-only intrusive containers and hashes. |
| `include/runtime/memory` | L0/L1 | Pools are L0. TTL/LRU cache code currently depends on `time` and should be split into an L1 cache module. |
| `include/runtime/time`, `src/time` | L1 | Time values, timers, and timer indexes. No fd or EventLoop ownership. |
| `include/runtime/log`, `src/log` | L1 | Process logging; may depend on base and time. |
| `include/runtime/metrics` | L1 plus one L3 leak | Generic metrics are L1. `gateway_metrics.h` belongs under gateway. |
| `include/runtime/task`, `src/task` | L2 | Blocking/thread-pool execution. Must remain independent of net. |
| `include/runtime/coro` | L2 | Coroutine execution utilities. |
| `include/runtime/net`, `src/net` | L2 | Reactor, sockets, channels, timers bound to EventLoop, TCP lifecycle. |
| `include/runtime/uring`, `src/uring` | L2 | Optional I/O backend. It may reuse net value types but must not expose gateway policy. |
| `include/runtime/http`, `src/http` | L2 | HTTP parser, router, request/response, and server adapter over net/task. |
| `include/runtime/gateway`, `src/gateway` | L3 | Currently flat; should be split by policy responsibility. |
| `examples`, `docs/benchmark`, `tests` | L4 | Consumers and validation only. |

## Allowed Dependencies

### L0

- May depend on the C++ standard library and operating-system headers needed by
  the primitive itself.
- `ds` may depend on `base`.
- `memory-core` may depend on `base`.
- L0 modules must not depend on time, logging, networking, HTTP, or gateway.

### L1

- May depend on L0.
- `log` may depend on `time`.
- Generic metrics must remain independent of HTTP and gateway.
- Configuration data types should not construct sockets, start threads, or
  schedule callbacks.

### L2

- May depend on L0 and L1.
- `task` and `net` are peers and must not depend on each other.
- `http` may depend on both `net` and `task`.
- `net` may use `time`, `log`, `ds`, and `memory-core`.
- `uring` may implement a net-owned backend interface. It must not require
  gateway types.

### L3

- May depend on public APIs from L0-L2.
- Gateway submodules may depend on the gateway core types listed in the
  submodule graph below.
- Gateway policy must not be moved into net, time, ds, memory, or generic
  metrics to avoid an apparent dependency.

### L4

- May depend on any public runtime layer.
- Tests may include internal headers when testing an invariant that cannot be
  observed through the public API, but production code must not copy that
  dependency.

## Forbidden Dependencies

The following are hard failures:

- `base`, `ds`, or memory pools including `time`, `net`, `http`, or `gateway`.
- `time` including `net`, `http`, or `gateway`.
- `log` or generic metrics including `http` or `gateway`.
- `net` including `http` or `gateway`, or inspecting gateway-specific names.
- `task` including `net`, and `net` including `task`.
- `http` including `gateway`.
- A gateway policy type being placed in `runtime::metrics`, `runtime::net`, or
  another lower namespace merely to make it reusable.
- Tests or benchmarks becoming required runtime link dependencies.

Semantic dependency inversion counts even without an include. For example,
`TcpConnection` recognizing a `"proxy->"` name prefix is a forbidden net to
gateway dependency.

## Gateway Submodule Graph

```text
gateway/server
  -> proxy, health, rate_limit, circuit_breaker
  -> upstream, load_balance
  -> http, net, metrics

gateway/proxy
  -> upstream, load_balance, circuit_breaker
  -> http, net, time

gateway/health
  -> upstream
  -> net, time, log

gateway/load_balance
  -> upstream
  -> ds

gateway/rate_limit
  -> base/time utilities only

gateway/circuit_breaker
  -> base/time utilities only

gateway/upstream
  -> circuit_breaker configuration/state
  -> net only in the connection-pool adapter
```

### Gateway Responsibility Boundaries

- **server** owns route registration, request admission order, response
  selection, metrics wiring, and lifecycle orchestration. It does not implement
  load-balancing algorithms or TCP connection state.
- **proxy** owns one downstream-to-upstream request state machine, header
  rewriting, response framing, timeout handling, retry decisions, and
  backpressure coordination.
- **health** owns active probes and the binary active-health state. It does not
  own passive request failures or circuit-breaker state.
- **rate_limit** owns token accounting and bounded identity storage. It does not
  parse HTTP or choose fallback bodies.
- **circuit_breaker** owns per-upstream admission state and outcome transitions.
  Local queue saturation and rate limiting are not upstream failures.
- **upstream** owns immutable topology/configuration plus narrowly defined
  atomic runtime counters. Mutation after publication requires a separate
  snapshot/publication design.
- **load_balance** selects an eligible peer. It does not open sockets, retry
  requests, mutate routes, or generate responses.

## New Module Decision Rules

Before adding a module, answer these questions in order:

1. What resource or invariant does the module own?
2. Is it policy, protocol, transport, process service, or a primitive?
3. Can it be tested without constructing a higher layer?
4. Does its proposed dependency point downward?
5. Is a callback, value type, or narrow interface enough to avoid a reverse
   dependency?
6. Does it require shared mutable state across EventLoops? If yes, justify why
   per-loop ownership or immutable snapshot publication is insufficient.
7. Is it truly reusable, or is it gateway-specific code being misplaced?

If ownership cannot be stated in one sentence, the module boundary is not
ready.

## Threading and Lifetime Rules

- One `EventLoop` is constructed, run, and destroyed on one owning thread.
- `Channel`, Poller registration, and connection state transitions run only on
  the owning loop thread.
- Cross-thread work is posted with `RunInLoop` or `QueueInLoop`; posting does
  not make captured raw pointers safe.
- Every asynchronous callback must use one of:
  - value capture;
  - `shared_ptr` when the callback intentionally owns the operation;
  - `weak_ptr` or generation token when cancellation must make it inert;
  - raw pointer/reference only when a documented owner cancels and drains all
    callbacks before destruction.
- Timer cancellation is asynchronous when called off-loop. Object lifetime
  must not rely on cancellation alone.
- Repeating timers owned by a component must have stored `TimerId`s and be
  cancelled before the referenced state is destroyed.
- Per-loop state should use values or `unique_ptr`. `shared_ptr` is reserved for
  real shared async ownership, not as a general lifetime patch.

## AI Patch Rules

An AI-generated patch must:

1. Read this file and the nearest module `SKILL.md` before editing.
2. State the owning thread, resource owner, and terminal state for every new
   asynchronous object.
3. Preserve public APIs unless the patch explains why the existing contract is
   unsafe or impossible to maintain.
4. Keep lower layers unaware of gateway names, routes, peers, retries, health,
   rate limits, and circuit-breaker policy.
5. Avoid raw `this` in delayed/asynchronous callbacks unless teardown proves
   cancellation and drain ordering.
6. Avoid new global locks. Prefer per-loop ownership, sharding, or immutable
   snapshots.
7. Bound externally controlled cardinality, queue length, buffered bytes,
   retries, active requests, and timeout duration.
8. Treat timeout, cancellation, disconnect, and normal completion as competing
   terminal events and make cleanup idempotent.
9. Add or update tests for state transitions, late callbacks, wrong-thread
   calls, capacity exhaustion, and teardown.
10. Run the module's required tests plus upper-layer integration tests when a
    lower-layer contract changes.

Generated patches must not introduce broad refactors, new abstraction layers,
or ownership changes solely for stylistic consistency.
