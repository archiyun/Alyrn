---
name: runtime-gateway-rate-limit-maintenance
description: Maintain gateway token buckets and bounded identity rate limiting. Use for rate math, cardinality caps, eviction, concurrency, and hot-path lock reviews.
---

# vexo/gateway/rate_limit Maintenance

## Purpose

Provide global and identity-scoped request admission with token buckets,
monotonic refill, bounded identity storage, and explicit overload behavior.

## Non-goals

- HTTP response rendering.
- Client identity extraction/trust policy.
- Circuit breaking, retries, health, or upstream selection.
- General-purpose cache implementation.

## Owned resources

- Global token bucket.
- Per-identity bucket shards/map.
- Bucket count limit and eviction metadata.
- Token/refill state.

## Public API / entry points

- `RateLimiterConfig`
- `TokenBucket::TryConsume`
- `RateLimiter::{AllowGlobal,AllowPerIP,per_ip_bucket_count}`

## Thread model

- Calls may arrive concurrently from all I/O loops.
- Each bucket update is serialized or atomic as one refill/consume transaction.
- Identity map synchronization must not create a gateway-wide hot-path convoy.

## Lifetime rules

- RateLimiter owns all buckets.
- Bucket references do not escape unless ownership keeps them stable.
- Configuration is immutable after construction.

## State machine

```text
bucket: full/partial/empty --elapsed time--> lazily refilled --request--> consumed/rejected
identity: absent -> admitted bucket -> idle/full -> evicted
```

## Invariants

- `0 <= tokens <= burst`.
- Rate, burst, and scaled arithmetic are validated and finite.
- Per-identity storage never exceeds its hard cap.
- Active limited identities are not evicted in a way that resets their budget.
- Unknown identity behavior at capacity is explicit and tested.

## Common bugs

- Identity spray causing unbounded map growth.
- O(n) eviction while holding a global lock on an EventLoop.
- Negative/NaN/huge configuration converting to invalid integers.
- Lock nesting map mutex then bucket mutex.
- Resetting abusive clients by eviction.
- Trusting spoofable headers as the identity key.

## Required tests

- `rate_limiter_smoke_test`
- `resilience_integration_smoke_test`
- `gateway_adversarial_test`
- Concurrent same-identity and different-identity tests
- Hard-cap, idle-eviction, invalid-config, and sustained spray tests
- TSan for synchronization changes
- Benchmark/latency check for lock or eviction changes

## Forbidden dependencies

- `vexo/http`
- `vexo/net`
- `gateway/proxy`
- `gateway/health`
- Fallback body presentation policy

## Patch rules

- Keep identity cardinality bounded by default.
- Do not add a single global lock around all per-client hot-path work.
- Separate local admission rejection from upstream failure signals.
- Validate configuration at construction/startup.
- Preserve steady-clock refill semantics.
- Add adversarial high-cardinality tests for map changes.
