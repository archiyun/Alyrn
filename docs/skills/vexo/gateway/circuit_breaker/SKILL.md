---
name: runtime-gateway-circuit-breaker-maintenance
description: Maintain the per-upstream CircuitBreaker state machine. Use for concurrent transitions, consecutive outcome semantics, HalfOpen probe budgets, timeout recovery, and config validation.
---

# vexo/gateway/circuit_breaker Maintenance

## Purpose

Reject requests quickly when an upstream is failing and admit a bounded set of
recovery probes after a monotonic open timeout.

## Non-goals

- Per-peer health, load balancing, rate limiting, retries, or queue admission.
- HTTP status parsing.
- Owning request lifetime.

## Owned resources

- Closed/Open/HalfOpen state.
- Failure/success streak or score, as explicitly defined.
- Open timestamp, HalfOpen probe quota, unresolved-probe timeout.
- Transition metrics counters.

## Public API / entry points

- `CircuitBreakerConfig`
- `CircuitBreaker::{AllowRequest,OnSuccess,OnFailure,state,failure_count}`

## Thread model

- All methods may run concurrently on multiple EventLoop threads.
- Atomic operations define a linearized transition order.
- No mutex or blocking operation belongs on `AllowRequest`.

## Lifetime rules

- Breaker outlives every request admitted by it.
- Each admitted request reports exactly one terminal outcome.
- Lost outcomes must have a bounded recovery path, not permanent HalfOpen
  exhaustion.

## State machine

```text
Closed --trip condition--> Open
Open --open_timeout--> HalfOpen
HalfOpen --required successful probes--> Closed
HalfOpen --any failed probe or unresolved timeout--> Open
```

## Invariants

- The meaning of "consecutive failures" is consistent in code, comments, tests,
  and metrics.
- A success/failure cannot be counted twice for one admitted request.
- `success_threshold <= half_open_max_requests`, unless recovery is explicitly
  designed across multiple cycles.
- Thresholds and timeout are positive and bounded.
- Local rate/bulkhead rejection is not an upstream failure.

## Common bugs

- `AllowRequest` without a matching outcome.
- Non-consecutive results treated as consecutive by accident.
- Concurrent stale counter reset.
- HalfOpen quota impossible to satisfy.
- Negative config cast to a large unsigned value.
- Counting malformed/partial responses as success.

## Required tests

- `circuit_breaker_smoke_test`
- `resilience_integration_smoke_test`
- `gateway_adversarial_test`
- Concurrent transition tests
- Full Closed/Open/HalfOpen cycles
- Lost-outcome recovery
- Alternating success/failure semantic tests
- Invalid configuration tests

## Forbidden dependencies

- `vexo/net`
- `vexo/http`
- `gateway/proxy`
- `gateway/health`
- `gateway/rate_limit`

## Patch rules

- Write the state transition table before changing atomics.
- Name counters according to their real semantics: streak, score, or cumulative.
- Keep `AllowRequest` allocation-free and nonblocking.
- Do not weaken exactly-once outcome reporting requirements.
- Validate config once rather than scattering clamps through hot paths.
- Add a concurrency regression for every transition algorithm change.
