---
name: runtime-gateway-proxy-maintenance
description: Maintain proxy request forwarding and GatewayServer proxy orchestration. Use for UpstreamRequest state, retries, deadlines, response framing, connection pooling, budgets, and backpressure.
---

# vexo/gateway/proxy Maintenance

## Purpose

Forward one parsed downstream HTTP request to an upstream peer, relay one
response, manage attempts and connection reuse, and terminate within explicit
time, retry, queue, and byte budgets.

## Non-goals

- TCP reactor implementation.
- Peer health probing.
- Load-balancer algorithm internals.
- Token-bucket or circuit-breaker state-machine implementation.

## Owned resources

- UpstreamRequest state and serialized request bytes.
- Outbound TcpClient for the active attempt.
- Downstream weak reference.
- Request deadline and retry counters/budget tokens.
- Peer/upstream active accounting.
- Response framing and pool release decision.

## Public API / entry points

- `ProxyPass::Forward`
- `ProxyPass::BuildRequest`
- `UpstreamRequest::Start`
- GatewayServer proxy-route dispatch integration

## Thread model

- One request is owned by the downstream connection's EventLoop.
- Its TcpClient, pool, timers, state transitions, and cleanup run on that loop.
- Shared upstream/peer counters are atomic; topology is immutable.
- Do not destroy a TcpClient while one of its callbacks is on the stack.

## Lifetime rules

- Caller retains UpstreamRequest until its terminal state.
- GatewayServer, route LoadBalancer, Upstream, pool, and breaker outlive active
  requests.
- Timer callbacks use weak request ownership.
- Cleanup releases timer, peer count, upstream slot, breaker outcome, and
  connection exactly once.

## State machine

```text
admitted -> connecting -> sending -> reading_headers -> forwarding_body -> done
             |              |             |                 |
             +--------------+-------------+------> retry or terminal failure
any active state --deadline/client close--> terminal cleanup
```

## Invariants

- One downstream request yields at most one response.
- No unsafe on-wire method is retried.
- Retry selects an eligible alternate peer or terminates.
- Total retry attempts and shared retry amplification are bounded.
- A committed downstream response is never followed by a new 502 response.
- Header bytes, pending requests, active requests, and output buffering are
  bounded.
- Pool reuse occurs only after exact response completion.
- Accounting and breaker reporting are exactly once.

## Common bugs

- Replacing/destroying TcpClient inside its disconnect callback.
- One `ConnCtx::upstream_req` overwritten by HTTP pipelining.
- Timeout after partial response causing protocol corruption.
- Chunked keep-alive response never reaching completion.
- Unbounded upstream headers or downstream output buffer.
- Retrying the same hash-selected peer.
- Local bulkhead rejection reported as breaker failure.
- Raw reference captured by pool maintenance timer.

## Required tests

- `proxy_e2e_smoke_test`
- `resilience_integration_smoke_test`
- `gateway_adversarial_test`
- `rst_storm_smoke_test` for close/reset path changes
- Tests for connect failure, retry method safety, alternate-peer selection,
  total deadline, partial-response timeout, chunked framing, header limit,
  pipelining limit, client disconnect, and backpressure
- ASan/UBSan callback-stack lifetime tests

## Forbidden dependencies

- Net internals beyond public transport APIs
- Active HealthChecker implementation
- RateLimiter internals
- Tests/benchmarks in production code

## Patch rules

- Model each new terminal event as a race with normal completion.
- Use one idempotent cleanup function or equivalent guard set.
- Never add an unbounded queue, buffer, or retry path.
- Separate per-request retry count from a shared retry budget.
- Defer owner replacement/destruction until callback dispatch unwinds.
- Preserve existing public proxy APIs unless an unsafe contract is documented.
