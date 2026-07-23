---
name: runtime-gateway-proxy-maintenance
description: Maintain proxy request forwarding, UpstreamStreamPool usage, retries, deadlines, response framing, and backpressure.
---

# coropact/gateway/proxy Maintenance

## Purpose

Forward one parsed downstream HTTP request to an upstream peer, relay one
response, manage attempts and stream reuse, and terminate within explicit time,
retry, queue, and byte budgets.

## Non-goals

- Reactor or io_uring transport implementation.
- Active upstream health probing.
- Load-balancer algorithm internals.
- Token-bucket or circuit-breaker state-machine implementation.

## Owned resources

- `ProxyPass` request state and serialized request bytes.
- The active `AsyncStream` obtained from `UpstreamConnector` or
  `UpstreamStreamPool`.
- Downstream stream ownership and request deadline.
- Retry budget, peer/upstream accounting, and pool release decision.

## Public API / entry points

- `ProxyPass::Forward`
- `ProxyPass::BuildRequest`
- `UpstreamConnector`
- `UpstreamStreamPool`
- GatewayServer proxy-route dispatch integration

## Thread model

- One request is owned by the downstream stream's scheduling domain.
- Its upstream connector, stream, pool operations, timers, and cleanup run in
  that domain.
- Shared upstream/peer counters are atomic; topology is immutable.
- Do not close or return an upstream stream while an operation owns it.

## Lifetime rules

- The serving task retains request state until its terminal operation completes.
- GatewayServer, route load balancer, Upstream, pool, and breaker outlive requests.
- Cancellation makes later completions inert before stream or owner release.
- Cleanup releases timer, peer count, upstream slot, breaker outcome, and stream
  exactly once.

## State machine

```text
admitted -> connecting -> sending -> reading_headers -> forwarding_body -> done
             |              |             |                 |
             +--------------+-------------+------> retry or terminal failure
any active state --deadline/stream close--> terminal cleanup
```

## Invariants

- One downstream request yields at most one response.
- No unsafe on-wire method is retried.
- Retry selects an eligible alternate peer or terminates.
- Total retry attempts and shared retry amplification are bounded.
- A committed downstream response is never followed by a new 502 response.
- Header bytes, pending requests, active requests, and output buffering are bounded.
- Pool reuse occurs only after exact response completion.
- Accounting and breaker reporting are exactly once.

## Common bugs

- Replacing or closing an upstream stream before its terminal operation unwinds.
- One request slot overwritten by HTTP pipelining.
- Timeout after partial response causing protocol corruption.
- Chunked keep-alive response never reaching completion.
- Unbounded upstream headers or downstream output buffer.
- Retrying the same hash-selected peer.
- Local bulkhead rejection reported as breaker failure.
- Owner capture outliving a pool maintenance task.

## Required tests

- `proxy_e2e_smoke_test`
- `gateway_adversarial_test`
- `gateway_core_smoke_test`
- Tests for connect failure, retry method safety, alternate-peer selection,
  total deadline, partial-response timeout, chunked framing, header limit,
  pipelining limit, client disconnect, and backpressure
- ASan/UBSan stream-lifetime tests

## Forbidden dependencies

- Net internals beyond public transport APIs
- Active health-loop implementation
- RateLimiter internals
- Tests/benchmarks in production code

## Patch rules

- Model each new terminal event as a race with normal completion.
- Use one idempotent cleanup function or equivalent guard set.
- Never add an unbounded queue, buffer, or retry path.
- Separate per-request retry count from a shared retry budget.
- Defer stream replacement or destruction until its operation unwinds.
- Preserve existing public proxy APIs unless an unsafe contract is documented.
