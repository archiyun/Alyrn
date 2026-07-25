---
name: runtime-gateway-maintenance
description: Maintain coropact/gateway session orchestration and cross-subsystem policy. Use for GatewaySessionService, routing admission, resilience integration, and gateway boundary reviews.
---

# coropact/gateway Maintenance

Read `docs/SUBSYSTEMS.md` and the narrower gateway submodule skill before
editing.

## Purpose

Integrate HTTP, net, upstream topology, proxying, health, rate limiting,
circuit breaking, load balancing, and fallback into one request-serving
product.

## Non-goals

- Reimplementing EventLoop, TCP, timers, HTTP parsing, allocators, or intrusive
  containers.
- Moving gateway policy into lower layers.
- Providing unbounded queues or implicit cross-loop shared state.

## Owned resources

- GatewaySessionService configuration and route table.
- Component lifecycle and startup/teardown ordering.
- Per-connection gateway context.
- Per-loop upstream connection pools and their maintenance timers.
- Gateway-specific fallback responses.

## Public API / entry points

- `GatewaySessionService`
- Route registration and `Serve`
- Rate-limit enablement
- Public upstream, proxy, load-balancer, and resilience configuration types

## Thread model

- Configuration is built before Start.
- Request and connection context is owned by the connection's EventLoop.
- Topology is immutable after publication or replaced as an immutable snapshot.
- Per-loop pools are accessed only by their owning loop.
- Cross-loop global state must be atomic, sharded, or snapshot-based.

## Lifetime rules

- The network server owns EventLoops, listeners, accepts, workers, and shutdown.
- The network server and UpstreamRegistry outlive GatewaySessionService sessions.
- GatewaySessionService outlives callbacks installed in its routes and handlers.
- One request object owns one upstream attempt lifecycle and idempotent cleanup.

## State machine

```text
GatewaySessionService: configuring -> serving -> destroyed
Connection: accepted -> parsing/dispatching -> closing
Proxy request: admitted -> active -> terminal cleanup
```

## Invariants

- Dependencies point only to lower layers or allowed gateway siblings.
- One downstream request produces at most one response.
- Route/load-balancer configuration is valid before Start.
- Admission order is explicit: rate limit, route, breaker, budget, peer.
- Local overload is not reported as remote upstream failure.
- Every externally controlled queue, map, buffer, retry count, and timeout is
  bounded.

## Common bugs

- Raw `this` callbacks surviving GatewaySessionService or the network server.
- Recurring pool timer UAF.
- Pipelined requests overwriting one active request slot.
- Backend-specific downcast leaking into policy.
- Rejection and fallback paths reporting the wrong reason.
- Destruction order invalidating state before server callbacks stop.

## Required tests

- `proxy_e2e_smoke_test`
- `resilience_integration_smoke_test`
- `gateway_adversarial_test`
- `gateway_core_smoke_test`
- All narrower submodule smoke tests affected by the patch
- ASan/UBSan for lifecycle changes; TSan for shared gateway state

## Forbidden dependencies

- Tests/examples/benchmarks from production code
- New gateway knowledge in `base`, `ds`, `memory`, `time`, `task`, or `net`
- Global locks protecting per-loop state

## Patch rules

- Prefer a narrow submodule patch over adding network lifecycle to GatewaySessionService.
- Keep recurring health or maintenance work in an owner with an explicit shutdown contract.
- Keep route/config mutation startup-only unless implementing snapshot
  publication as a complete feature.
- Preserve existing external includes with forwarding headers during moves.
- Add rejection-reason and terminal-cleanup tests for lifecycle changes.
