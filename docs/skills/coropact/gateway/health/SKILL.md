---
name: runtime-gateway-health-maintenance
description: Maintain GatewayServer's active upstream health loop, bounded probes, thresholds, late completions, and health-state ownership.
---

# coropact/gateway/health Maintenance

## Purpose

Probe upstream peers through the configured `UpstreamConnector`, classify
complete bounded responses, maintain consecutive outcomes, and exclusively own
the active-health hard-down flag.

## Non-goals

- Passive request failure cooldowns.
- Circuit-breaker transitions.
- Load-balancer algorithms or proxy retries.
- Generic transport implementation.

## Owned resources

- GatewayServer-owned periodic health loop and probe tasks.
- Per-peer in-flight marker and bounded probe stream.
- Consecutive success/failure counters.
- Active-health writes to `UpstreamPeerState::down`.

## Public API / entry points

- `HealthCheckConfig`
- `GatewayServer::EnableHealthCheck`
- `GatewayServer::HealthLoop`, `ProbePeer`, and `CompleteHealthProbe`

## Thread model

- The health loop runs on the GatewayServer scheduler.
- Probe state and threshold updates are serialized by that loop.
- Connector and stream completions return to the same scheduling domain.
- Stop/generation gates make late completions harmless.

## Lifetime rules

- GatewayServer, registry, and scheduler outlive the health loop.
- Shutdown invalidates the generation before resources are released.
- Late completions may release stream resources but must not mutate peer health.
- Probe ownership is bounded and released promptly after completion.

## State machine

```text
health loop: disabled -> running(generation N) -> stopped
peer probe: idle -> in-flight -> success | failure | timeout -> idle
peer health: up --N failures--> down --M successes--> up
```

## Invariants

- At most one in-flight probe exists per peer identity per generation.
- Exactly one outcome is attributed to each probe.
- A successful prefix is not a successful HTTP response; headers must complete
  within the configured byte limit.
- Failures reset success streaks and successes reset failure streaks.
- Active health alone owns `down`; passive proxy failures do not write it.

## Common bugs

- Raw owner captures in recurring probe tasks.
- Completion or cancellation treated as synchronous when it is not.
- Duplicate peer names colliding in the in-flight map.
- Probe overlap when interval is shorter than the timeout.
- A late stream completion changing state after shutdown.
- Registry mutation during peer iteration.

## Required tests

- `gateway_adversarial_test`
- `gateway_core_smoke_test`
- `proxy_e2e_smoke_test` when probe transport changes
- `luring_gateway_smoke_test` for the io_uring backend
- ASan/UBSan for destroy-with-probe-in-flight
- Tests for malformed/truncated status, oversized headers, timeout, shutdown,
  generation changes, and duplicate peer identity

## Forbidden dependencies

- `gateway/proxy`
- `gateway/rate_limit`
- Route/fallback policy
- Writes to passive cooldown or circuit-breaker state

## Patch rules

- Preserve generation-based invalidation for late completions.
- Key single-flight state by a truly unique peer identity.
- Never permit overlapping probes as an accidental side effect.
- Add a lifetime test before changing callback or task captures.
