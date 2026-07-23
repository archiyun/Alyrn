# Maintainer Review

This review describes the current tree as inspected on 2026-06-15. It treats
the working tree as the source of truth, including its uncommitted gateway/net
changes. It proposes documentation and boundaries, not a bulk rewrite.

## A. Current Subsystem Diagnosis

The explicit include direction is mostly sound: no base/ds/memory/time/net
header directly includes gateway. The main problems are target granularity,
semantic leaks, and insufficiently explicit lifetime contracts.

| Area | Current layer | Diagnosis |
|---|---:|---|
| `base`, intrusive `ds`, pool allocators | L0 | Correct direction. Intrusive containers need stronger hook-membership and owner-lifetime rules. |
| TTL/LRU cache under `memory` | L0/L1 mixed | It depends on wall-clock `Timestamp`; move cache policy to L1 or inject a clock. |
| `time` | L1 | Correctly independent of net. Scheduling still uses wall-clock timestamps while timerfd uses a monotonic clock. |
| `log` | L1 | Correctly placed as a process-level service. |
| `task`, `net`, `http` | L2 | Direction is mostly correct. `task` and `net` are peers; `http` is their integration layer. |
| `gateway` | L3 | All gateway responsibilities are flattened into one directory and several header-only types. |
| examples/tests/benchmarks | L4 | Correct consumers, but the human test map is already stale in places. |

Observed dependency edges:

```text
time -> base, ds
memory -> base, time              # TTL cache causes the time edge
log -> base, time
task -> base
net -> base, ds, memory, time, log
http -> base, memory, time, task, net, log
gateway -> base, ds, time, log, net, http
```

Boundary violations or warning signs:

- The former callback-oriented TCP layer mixed transport lifetime with gateway
  policy. It is now removed; the Reactor stream and gateway connector contracts
  keep those responsibilities separate.
- The single `coropact_foundation` target combines L0 memory/ds concerns with L1
  time/log concerns, so CMake cannot enforce the intended boundaries.
- README claims upstreams can be added at runtime, while `UpstreamRegistry` and
  `Upstream` are implemented as startup-built, read-only objects.

## B. Recommended Directory Structure

This is an incremental destination. Keep compatibility forwarding headers when
moving public APIs.

```text
include/coropact/
  base/
  ds/
  memory/                 # arena, fixed block pool, object pool only
  cache/                  # TTL/LRU policy; L1
  time/
  log/
  task/
  net/
    backend/
  http/
  gateway/
    server/
    health/
    rate_limit/
    circuit_breaker/
    proxy/
    upstream/
    load_balance/

src/                       # mirrors include/coropact
examples/
tools/
benchmarks/                # move executable benchmark drivers here over time
tests/{unit,integration,adversarial}
docs/
```

Recommended target split:

```text
runtime_base
runtime_ds
runtime_memory
runtime_time       -> base, ds
runtime_log        -> base, time
coropact_task       -> base
coropact_net        -> base, ds, memory, time, log
coropact_http_core  -> foundation
coropact_gateway    -> http, net, time, log, ds
```

Do not perform all moves in one commit. First add dependency checks and
forwarding headers, then move one subsystem at a time.

## C. Dependency Direction Rules

The normative rules are in [SUBSYSTEMS.md](SUBSYSTEMS.md). The short form is:

- Dependencies flow from L4 to L0 only.
- L0 has no process services or business policy.
- `task` and `net` remain independent peers.
- `http` may integrate task and net; gateway may integrate HTTP and net.
- Sibling gateway dependencies follow the graph in `SUBSYSTEMS.md`.
- Semantic knowledge counts as a dependency. Name prefixes and test hooks can
  violate boundaries without an include.

## D. `docs/SUBSYSTEMS.md` Draft

The draft is stored as [SUBSYSTEMS.md](SUBSYSTEMS.md). It contains the layer
diagram, allowed and forbidden dependencies, gateway responsibility boundaries,
new-module rules, threading/lifetime rules, and AI patch requirements.

## E. Module `SKILL.md` Drafts

The version-controlled drafts are under `docs/skills/coropact`:

- [net](skills/coropact/net/SKILL.md)
- [time](skills/coropact/time/SKILL.md)
- [ds](skills/coropact/ds/SKILL.md)
- [memory](skills/coropact/memory/SKILL.md)
- [gateway](skills/coropact/gateway/SKILL.md)
- [gateway/health](skills/coropact/gateway/health/SKILL.md)
- [gateway/rate_limit](skills/coropact/gateway/rate_limit/SKILL.md)
- [gateway/circuit_breaker](skills/coropact/gateway/circuit_breaker/SKILL.md)
- [gateway/proxy](skills/coropact/gateway/proxy/SKILL.md)
- [gateway/upstream](skills/coropact/gateway/upstream/SKILL.md)
- [gateway/load_balance](skills/coropact/gateway/load_balance/SKILL.md)

These are repository maintenance instructions. They can later be mirrored into
an agent-specific skill directory without making `.claude/` the canonical copy.

## F. Header Maintenance Comment Drafts

### EventLoop

```cpp
// Threading: constructed, Loop()'d, and destroyed on one owning thread.
// RunInLoop/QueueInLoop/Quit are cross-thread entry points; channel mutation is not.
// Lifetime: every posted functor and timer must become inert before captured
// non-owning state is destroyed. The EventLoop outlives all registered Channels.
// State: created -> looping -> quit requested -> stopped -> destroyed.
// Invariants: one EventLoop per thread; callbacks run serially; Poller and
// TimerQueue are touched only by the owner thread.
// Common failure modes: raw-this capture outliving its owner, teardown with
// queued work, reentrant RunInLoop assumptions, and wrong-thread destruction.
```

### Channel

```cpp
// Threading: all event-interest changes, dispatch, and removal occur on OwnerLoop().
// Lifetime: Channel does not own fd or callback target. The fd owner must disable
// and remove the Channel before either fd or Channel storage is destroyed.
// State: new/unregistered -> registered -> disabled -> removed.
// Invariants: at most one Poller registration per fd; Remove requires no events;
// Tie(), when used, is established before events can be dispatched.
// Common failure modes: stale Poller pointer, fd reuse before removal, callback
// destroying the owner mid-dispatch, and callbacks firing in an unexpected order.
```

### TimerQueue / TimerId

```cpp
// Threading: tree/hash/pool/timerfd state belongs to one EventLoop thread.
// Off-loop add/cancel is a posted request, not a synchronous state transition.
// Lifetime: TimerQueue owns Timer objects; TimerId owns nothing and may be stale.
// A callback's captures must remain valid even if Cancel races with dispatch.
// State: allocated -> pending insertion -> active -> executing -> rescheduled/released.
// Invariants: each active Timer is in both indexes exactly once; sequence IDs are
// never reused; one terminal path releases each Timer exactly once.
// Common failure modes: wall-clock jumps, raw-this captures, allocation outside
// the loop racing teardown, cancellation assumed synchronous, and uncancelled
// repeating timers retaining component state.
```

### CircuitBreaker

```cpp
// Threading: all methods are concurrent; atomics define the transition order.
// Lifetime: the breaker outlives every request admitted by AllowRequest().
// State: Closed -> Open -> HalfOpen -> Closed/Open.
// Invariants: each admitted request reports exactly one outcome; HalfOpen probe
// capacity is bounded; configuration permits enough probes to reach recovery.
// Common failure modes: calling local overload an upstream failure, losing an
// outcome, contradictory "consecutive failure" semantics, invalid thresholds,
// and success_threshold greater than half_open_max_requests.
```

### RateLimiter

```cpp
// Threading: token updates are serialized per bucket; identity-map ownership and
// lock scope must be explicit because calls run on EventLoop hot paths.
// Lifetime: buckets are owned by the limiter and no bucket reference escapes.
// State: tokens refill lazily from steady time and are consumed atomically.
// Invariants: tokens stay in [0, burst]; external identity cardinality is bounded;
// eviction never resets an actively limited identity.
// Common failure modes: identity spray, O(n) eviction under a global hot-path lock,
// invalid rate/burst conversion, clock arithmetic overflow, and lock convoying.
```

### UpstreamRegistry

```cpp
// Threading: construction and Add occur before publication; Find/all are read-only
// after publication. Runtime mutation requires immutable snapshot replacement.
// Lifetime: registry outlives GatewayServer, the active health loop, routes,
// and requests.
// State: building -> frozen/published.
// Invariants: upstream names are unique; peer vectors/configuration do not mutate
// after publication; returned Upstream objects remain alive for active requests.
// Common failure modes: Add racing Find/iteration, silent duplicate insertion,
// documentation claiming dynamic mutation, and registry destruction before callbacks.
```

### LoadBalancer

```cpp
// Threading: Select may run concurrently on many EventLoop threads.
// Lifetime: strategy and Upstream outlive every Select call; cached peer pointers
// require immutable peer topology.
// State: stateless, atomic cursor, mutex-protected weights, or immutable RCU table.
// Invariants: return only currently eligible peers; bound hot-path allocation and
// work; configured weights cannot overflow selection arithmetic.
// Common failure modes: null strategy from invalid configuration, hidden O(N)
// allocation, stale raw-pointer cache after topology mutation, global mutex
// contention, and hash retries selecting the same failed peer.
```

### Proxy / GatewayServer

```cpp
// Threading: connection/request state is owned by the downstream connection's loop.
// Shared topology is immutable or atomic; per-loop pools are never touched off-loop.
// Lifetime: GatewayServer outlives server callbacks and recurring timers.
// Proxy requests have one idempotent terminal cleanup path and do not close or
// return an upstream stream while its terminal operation is still on the stack.
// State: request admission -> connect -> send -> read headers -> stream body -> done.
// Invariants: one response per request; no retry of unsafe on-wire methods; active,
// peer, breaker, timer, and pool accounting are released exactly once; all queues,
// retries, buffered bytes, and time are bounded.
// Common failure modes: pipelined request overwrites, partial response followed by
// 502 on timeout, retry amplification, no downstream backpressure, invalid/malformed
// upstream response counted successful, and recurring timer UAF during teardown.
```

## G. Priority Boundary and Lifetime Problems

### P0: fix before adding gateway features

1. **Recurring pool timer has no owner-safe teardown.** `GetOrCreatePool`
   captures `&pool_ref` in `RunEvery` and stores no `TimerId`; destroying
   `GatewayServer` while loops continue leaves a dangling callback.

### P1: resilience contracts are incomplete

1. Proxy has one total request deadline and one upstream active-request limit,
   but no downstream pending-request limit, per-peer connect budget, global
   retry budget, response-header byte limit, or streaming backpressure.
2. A timeout after response headers are forwarded sends a new 502 after a
   partial response. Timeout handling must distinguish uncommitted and
   committed downstream responses.
3. One `ConnCtx::upstream_req` is overwritten by HTTP pipelining. Either allow
   one active proxy request per downstream connection or maintain a bounded,
   ordered queue.
4. Local bulkhead rejection is reported to the circuit breaker as an upstream
   failure. Admission overload and remote failure must remain separate signals.
5. CircuitBreaker documentation says "consecutive failures", but Closed-state
   implementation preserves failures across isolated successes until a success
   threshold is reached. Choose and test one semantic.
6. `success_threshold > half_open_max_requests` makes recovery impossible in a
   HalfOpen cycle. Configuration requires validation.
7. RateLimiter bounds identity count, but all per-IP traffic and a possible
   65k-entry eviction sweep run under one mutex on I/O threads.

### P2: structural debt

1. Split TTL caches from memory-core.
2. Split load balancers out of one large header and move nontrivial algorithms
   to `.cc` files.
3. Enforce target-level boundaries in CMake and CI.
4. Reconcile README runtime-mutation claims with startup-only registry design.
5. Move timer scheduling to an explicit monotonic time domain.

## H. Suggested Commit Plan

1. `docs: define subsystem layering and maintenance contracts`
   - Add `SUBSYSTEMS.md`, this review, and module skills only.
2. `net: harden timer ownership and monotonic scheduling`
   - Loop-own allocation/insertion, explicit cancellation/drain rules, and
     monotonic deadlines.
3. `gateway: make server and pool timers teardown-safe`
   - Store timer IDs, cancel on owner loop, and define destructor ordering.
4. `gateway: bound proxy request lifecycle`
   - Pending limit, committed-response timeout behavior, response-header limit,
     downstream backpressure, and idempotent cleanup.
5. `gateway: separate retry and overload budgets from breaker outcomes`
   - Configurable per-request retry count plus shared retry budget.
6. `gateway: validate resilience configuration`
   - Circuit-breaker semantics, rate/burst ranges, health thresholds, and
     globally unique peer identity.
7. `gateway: split subdirectories with compatibility headers`
   - Move one submodule per commit; preserve existing external includes.
8. `ci: add dependency and sanitizer gates`
    - Include graph checks, required module tests, ASan lifetime tests, and TSan
      tests for cross-loop APIs.
