---
name: runtime-ds-maintenance
description: Maintain runtime/ds intrusive containers, heaps, trees, hash tables, bloom filters, and hash functions. Use for data-structure invariants and zero-ownership container patches.
---

# runtime/ds Maintenance

## Purpose

Provide low-level, policy-free data structures with explicit ownership,
intrusive hooks, predictable complexity, and debug invariant validation.

## Non-goals

- Object lifetime ownership.
- Thread synchronization.
- Networking, timers, cache eviction policy, or gateway selection policy.

## Owned resources

- Container bookkeeping storage such as bucket arrays and heap vectors.
- Intrusive hook linkage while an element is a member.
- Structural size, root/minimum, and index metadata.

## Public API / entry points

- `IntrusiveList`
- `IntrusiveHashTable`
- `IntrusiveRBTree`
- `IntrusiveSplayTree`
- `IntrusiveQuadHeap`
- `BloomFilter`
- Murmur hash helpers

## Thread model

- Containers are not thread-safe unless a type explicitly says otherwise.
- A container and every linked hook are externally synchronized as one unit.
- Hash/comparison projections must be pure while elements are linked.

## Lifetime rules

- Containers never own elements.
- Element storage outlives membership.
- Hooks are unlinked before element destruction.
- A hook instance belongs to at most one container; use distinct tags/hooks for
  multiple memberships.

## State machine

```text
hook: unlinked -> linked in exactly one container -> unlinked
container: empty <-> populated -> Clear/destruction unlinks all hooks
```

## Invariants

- Size equals reachable linked elements.
- Parent/child, prev/next, pprev, and heap index links are reciprocal.
- Tree comparator is irreflexive, transitive, and stable while linked.
- Hash key does not change while linked unless erased/reinserted.
- Container destruction leaves hooks reusable.
- No gateway or net type appears in a generic template contract.

## Common bugs

- Cross-container erase through a hook that only says "linked somewhere".
- Moving or destroying linked elements.
- Mutating key/order fields while linked.
- Vector reallocation invalidating intrusive back-pointers.
- Pointer-tagging alignment assumptions.
- Header/file mismatch for hash implementations.

## Required tests

- `intrusive_list_smoke_test`
- `intrusive_hash_table_smoke_test`
- `rbtree_validator`
- `splaytree_validator`
- `quad_heap_test`
- `timer_tree_smoke_test` when tree behavior changes
- `test_bloom_filter` for BloomFilter/hash changes
- Add randomized differential and invariant checks for structural changes

## Forbidden dependencies

- `runtime/time`
- `runtime/log`
- `runtime/net`
- `runtime/http`
- `runtime/gateway`
- Allocator or scheduling policy

## Patch rules

- State hook ownership and cross-container preconditions.
- Keep ownership external and allocation optional.
- Add or update `CheckInvariants` for new metadata.
- Use randomized operation sequences for tree/hash changes.
- Do not weaken assertions that detect structural corruption.
- Avoid API abstraction that obscures O(1)/O(log n) ownership mechanics.
