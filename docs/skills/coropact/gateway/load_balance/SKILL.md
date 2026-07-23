---
name: runtime-gateway-load-balance-maintenance
description: Maintain gateway load-balancing strategies and factory selection. Use for peer eligibility, weighted algorithms, hash rings, Maglev tables, concurrency, and hot-path cost.
---

# coropact/gateway/load_balance Maintenance

## Purpose

Select one eligible UpstreamPeer according to a documented strategy while
remaining thread-safe and predictable at large peer counts.

## Non-goals

- Opening connections, retrying, reporting health, changing topology, or
  generating HTTP responses.
- Owning upstream peers.

## Owned resources

- Strategy-local cursor/RNG state.
- Smooth weighted current weights.
- Immutable hash rings or lookup tables and rebuild synchronization.
- Algorithm factory mapping.

## Public API / entry points

- `RequestContext`
- `LoadBalancer::Select`
- Concrete strategy classes
- `CreateLoadBalancer`

## Thread model

- `Select` may be called concurrently by all I/O loops.
- Stateless/atomic fast paths should not block.
- Stateful tables are immutable after atomic publication.
- Rebuild locks protect only rare topology/eligibility changes.

## Lifetime rules

- Strategy outlives active requests using it.
- Upstream and peers outlive Select and any cached table.
- Raw peer pointer keys are valid only under immutable topology.
- Published table/ring ownership keeps referenced peers alive.

## State machine

```text
simple strategy: ready -> Select*
cached strategy: empty/stale -> rebuilding -> published/current -> stale
factory: algorithm name -> valid strategy | configuration error
```

## Invariants

- Return `nullptr` or an eligible peer, never an ineligible peer by design.
- Unknown algorithm is rejected before request handling; no null dereference.
- Hot-path complexity/allocation matches the documented claim.
- Weight arithmetic cannot overflow and invalid weights are rejected or
  normalized consistently.
- Hash output is deterministic for the same key and published peer snapshot.
- Rebuild publication is race-free.

## Common bugs

- Fingerprint/string allocation on every supposedly allocation-free Select.
- O(N) scans used where a 10k-peer design claims O(1).
- One mutex serializing all requests.
- Pointer cache invalid after runtime topology mutation.
- Integer overflow in total weights or cross-products.
- Availability changing between multi-pass count and pick.
- Retry using the same affinity-selected failed peer.
- `CreateLoadBalancer` returning null and GatewayServer dereferencing it.

## Required tests

- `load_balancer_smoke_test`
- `proxy_e2e_smoke_test` for factory/request-context changes
- Concurrent Select tests
- Distribution and weighted fairness tests
- Peer up/down and passive cooldown changes
- Consistent-hash/Maglev determinism and bounded remapping tests
- 10k-peer benchmark for algorithmic or allocation changes
- TSan for table publication/rebuild changes

## Forbidden dependencies

- `coropact/net`
- `coropact/http`
- `gateway/proxy`
- `gateway/health`
- `gateway/rate_limit`
- Response, retry, or connection-pool policy

## Patch rules

- State expected time, allocation, and lock cost for Select.
- Keep topology input read-only.
- Move large non-template implementations to `.cc` files when splitting the
  current monolithic header.
- Validate algorithm names and parameters at route registration.
- Add distribution plus concurrency tests for strategy changes.
- Do not hide retry/failover behavior inside Select.
