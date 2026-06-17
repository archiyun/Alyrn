---
name: runtime-gateway-upstream-maintenance
description: Maintain Upstream, UpstreamPeer, UpstreamRegistry, and per-loop upstream connection pools. Use for topology publication, peer state ownership, request accounting, and pool lifetime.
---

# runtime/gateway/upstream Maintenance

## Purpose

Represent named upstream groups and peers, publish immutable topology, own
narrow atomic runtime counters, and cache reusable outbound clients per
EventLoop.

## Non-goals

- Selecting peers.
- Driving active health probes.
- Parsing/forwarding HTTP.
- Deciding retries, fallbacks, or rate limits.

## Owned resources

- Upstream/peer configuration and identity.
- Published peer vector and registry map.
- Peer active/request/failure/effective-weight atomics.
- Upstream active request slots.
- Per-loop idle TcpClient queues.

## Public API / entry points

- `UpstreamRegistry::{Add,Find,all}`
- `Upstream::{AddPeer,peers,TryAcquireRequestSlot,ReleaseRequestSlot}`
- `UpstreamPeer::{AvailableAt,OnFailure,OnSuccess}`
- `UpstreamConnPool::{Acquire,Release,EvictStale}`

## Thread model

- Build registry/upstreams before publication.
- Runtime topology reads are lock-free because topology is immutable.
- Peer and upstream counters are atomic.
- One UpstreamConnPool belongs to one EventLoop and has no internal lock.

## Lifetime rules

- Registry outlives gateway, health checker, routes, and requests.
- Upstream outlives route strategies and active requests.
- Peer shared ownership remains stable after publication.
- Pool owns whole TcpClient objects, not orphan TcpConnection pointers.
- Pool maintenance timer is cancelled before pool destruction.

## State machine

```text
registry/upstream: building -> frozen/published
peer passive state: eligible -> cooldown-ineligible -> eligible after timeout/success
peer active state: up <-> down (health-owned)
pool client: active -> reusable idle -> acquired | stale/dropped
request slot: available -> acquired -> released
```

## Invariants

- Upstream and peer names are unique in their documented scope.
- Published peer topology and static config do not mutate.
- Active-health `down` and passive failure cooldown have distinct owners.
- Active request counters never underflow and are released exactly once.
- Pool access occurs only on its owner loop.
- Idle limits and timeout are bounded.

## Common bugs

- `Add` racing with `Find` or health iteration.
- Silent duplicate names.
- Passive proxy path writing active-health hard-down state.
- Counter underflow on duplicate cleanup.
- Pool retaining only TcpConnection while TcpClient dies.
- Recurring eviction callback outliving the pool.
- Returning an idle connection after its timeout without validation.

## Required tests

- `load_balancer_smoke_test`
- `health_checker_smoke_test`
- `proxy_e2e_smoke_test`
- `gateway_adversarial_test`
- Tests for duplicate names, publication freeze, passive cooldown, request-slot
  exhaustion/release, pool reuse, stale eviction, and teardown
- TSan for topology publication or counter changes

## Forbidden dependencies

- `gateway/server`
- `gateway/rate_limit`
- HTTP response/fallback policy
- Load-balancer implementation inside upstream data types

## Patch rules

- Keep static configuration immutable after publication.
- Runtime mutation requires a complete immutable-snapshot publication design.
- Do not merge active and passive health ownership.
- Keep pools per-loop; do not add a global pool lock.
- Store/cancel any pool timer owned by a higher component.
- Add underflow and duplicate-terminal-path tests for accounting changes.
