---
name: runtime-memory-maintenance
description: Maintain vexo/memory arenas, fixed block pools, object pools, and PMR adapters. Use for allocator ownership, alignment, reset, cleanup, and pool lifetime patches.
---

# vexo/memory Maintenance

## Purpose

Own allocation mechanisms: request arenas, fixed-size block pools, typed object
pools, alignment, cleanup registration, reset, and PMR adaptation.

## Non-goals

- TTL/LRU cache policy that depends on clocks.
- General object ownership across async callbacks.
- Networking, scheduling, logging, or gateway policy.

## Owned resources

- Arena chunks and large allocations.
- Fixed-block backing storage and free lists.
- Pool-owned object slots and overflow allocations.
- Registered arena cleanup nodes.

## Public API / entry points

- `Pool::Create`, allocation/reset/free/cleanup APIs
- `MemoryPool`
- `ObjectPool`
- `PoolResource`
- Cache headers are currently colocated but should be treated as L1 cache code

## Thread model

- `Pool` and `PoolResource` are request/thread confined unless externally
  synchronized.
- `MemoryPool`/`ObjectPool` follow their `MutexPolicy`.
- Cleanup handlers run synchronously during owner destruction.

## Lifetime rules

- Arena pointers never outlive Pool reset/destruction.
- PMR containers using PoolResource are destroyed before the Pool.
- ObjectPool users release each acquired object exactly once to the same pool.
- The pool outlives every ScopedPtr/deleter that references it.
- Cleanup data allocated in the arena remains valid until cleanup execution.

## State machine

```text
Pool: created -> allocations/cleanups -> reset(reusable) -> destroyed
Block: free -> allocated -> free
Object slot: raw free -> constructed -> destroyed/raw free
```

## Invariants

- Alignment is a nonzero power of two and each returned pointer satisfies it.
- Free-list nodes belong to the pool and are not double-freed.
- Cleanup order is LIFO; cleanup runs before backing memory release.
- Reset does not leave live arena-backed objects or PMR containers.
- Overflow allocation and release paths are distinguishable and balanced.

## Common bugs

- Arena pointer escaping into connection/global state.
- Pool destroyed before PMR users or custom deleters.
- Invalid alignment arithmetic.
- Double release or releasing a foreign pointer.
- Constructor exceptions leaking a slot.
- Hiding time-dependent cache policy inside memory-core.

## Required tests

- `memory_pool_smoke_test`
- `object_pool_smoke_test`
- `pool_smoke_test`
- `pmr_pool_resource_smoke_test`
- `MemoryPoolTest` and `ObjectPoolTest` when GTest is available
- `memory_pool_bench` for hot-path layout/performance changes
- ASan/UBSan for reset, cleanup, overflow, and alignment changes
- `timer_tree_smoke_test` if ObjectPool behavior affects TimerQueue

## Forbidden dependencies

- `vexo/net`
- `vexo/http`
- `vexo/gateway`
- `vexo/log`
- `vexo/time` for allocator primitives

## Patch rules

- Keep allocator code independent of business lifetime.
- Document ownership for every returned pointer.
- Do not convert clear single ownership into shared_ptr.
- Preserve bulk-free semantics; do not add per-object deallocation to the arena.
- Add failure-path tests for exhaustion, overflow, reset, and cleanup.
- Move clock-aware cache changes toward a separate cache subsystem.
