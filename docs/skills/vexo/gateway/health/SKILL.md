---
name: runtime-gateway-health-maintenance
description: Maintain active upstream health checking. Use for HealthChecker probes, thresholds, late callbacks, Stop semantics, overlap prevention, and health-state ownership.
---

# vexo/gateway/health Maintenance

## Purpose

Actively probe upstream peers, classify complete bounded responses, maintain
consecutive probe outcomes, and exclusively own the active-health hard-down
flag.

## Non-goals

- Passive request failure cooldowns.
- Circuit-breaker transitions.
- Load-balancer algorithms or proxy retries.
- Generic TCP connection management.

## Owned resources

- Periodic scan timer and per-probe timeout.
- Probe TcpClient lifetime.
- Per-peer in-flight marker.
- Consecutive success/failure counters.
- Active-health writes to `UpstreamPeerState::down`.

## Public API / entry points

- `HealthCheckConfig`
- `HealthChecker::{Start,Stop}`

## Thread model

- Probe maps and threshold counters run on one configured EventLoop.
- `running`, `active`, and generation are atomic cancellation gates.
- TcpClient callbacks and timeout callbacks complete on the loop.

## Lifetime rules

- EventLoop and registry outlive the checker.
- Stop invalidates generation before timer cancellation.
- Late callbacks may release transport resources but must not mutate peer health.
- Per-probe ownership is bounded and released promptly after completion.

## State machine

```text
checker: stopped -> running(generation N) -> stopped(generation N+1)
peer probe: idle -> in-flight -> success | failure | timeout -> idle
peer health: up --N failures--> down --M successes--> up
```

## Invariants

- At most one in-flight probe per peer identity per generation.
- Exactly one outcome is attributed to each probe.
- A successful prefix is not a successful HTTP response; headers must complete
  within the byte limit.
- Failures reset success streaks and successes reset failure streaks.
- Stop makes all prior-generation callbacks inert.
- Active health alone owns `down`.

## Common bugs

- Raw `this` in recurring or timeout callbacks.
- Timer cancellation treated as synchronous.
- Duplicate peer names colliding in the in-flight map.
- Probe overlap when interval is shorter than timeout.
- Timeout callback retaining TcpClient until the full timeout after early
  completion.
- Registry mutation during iteration.

## Required tests

- `health_checker_smoke_test`
- `gateway_adversarial_test`
- `health_checker_lifetime_reproducer`
- ASan/UBSan for destroy-with-probe-in-flight
- Tests for malformed/truncated status, oversized headers, timeout, Stop/Start
  generation changes, and duplicate peer identity

## Forbidden dependencies

- `gateway/proxy`
- `gateway/rate_limit`
- Route/fallback/HTTP-server policy
- Writes to passive cooldown or circuit-breaker state

## Patch rules

- Preserve generation-based late-callback invalidation.
- Store/cancel per-probe timers if a patch changes probe ownership.
- Key single-flight state by a truly unique peer identity.
- Never permit overlapping probes as an accidental side effect.
- Add a lifetime test before changing callback captures.
