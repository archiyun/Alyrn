---
name: runtime-gateway-health-maintenance
description: Design or maintain a standalone active upstream health component.
---

# coropact/gateway/health Maintenance

Active health probing is deliberately outside `GatewaySessionService`. The
session layer owns request parsing and proxy policy; it must not own a timer,
connector, accept loop, or recurring task whose shutdown depends on a network
backend.

## Current status

- `HealthCheckConfig` remains a configuration data type.
- The gateway YAML loader rejects `health_check` because no standalone checker
  is attached by `GatewaySessionService`.
- A future checker must define its scheduler, probe concurrency, generation
  gate, and shutdown/join contract before it is exposed as a runtime feature.

## Design requirements for a future checker

- Probe each peer with an explicit byte and timeout budget.
- Serialize consecutive success/failure state per peer.
- Attribute exactly one terminal outcome to each probe.
- Make late completions inert after shutdown or generation change.
- Keep active-health writes separate from passive failure cooldown and circuit
  breaker state.
- Do not make the checker depend on Reactor-specific APIs; bind it to the
  common async connector/stream contract or let the owning network service
  provide a backend-neutral scheduling boundary.

## Non-goals

- Proxy retries, load balancing, circuit breakers, or rate limiting.
- Reintroducing `GatewayServer` as a compatibility owner.
- Hiding recurring work inside `GatewaySessionService`.
