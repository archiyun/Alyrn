---
name: runtime-memory-maintenance
description: Maintain alyrn/memory fixed block pools and object pools. Use for allocator ownership, alignment, and pool lifetime patches.
---

# alyrn/memory Maintenance

## Purpose

Own allocation mechanisms: fixed-size block pools, typed object pools, and
alignment.

## Non-goals

- TTL/LRU cache policy that depends on clocks.
- General object ownership across async callbacks.
- Networking, scheduling, logging, or gateway policy.
- Request-scoped arenas or PMR adapters over them.

## Owned resources

- Fixed-block backing storage and free lists.
- Pool-owned object slots and overflow allocations.

## Public API / entry points

- `MemoryPool`
- `ObjectPool`

## Thread model

- `MemoryPool`/`ObjectPool` follow their `MutexPolicy`.

## Lifetime rules

- ObjectPool users release each acquired object exactly once to the same pool;
  destroying an ObjectPool with a live fixed or overflow object terminates.
- The pool outlives every ScopedPtr/deleter that references it.

## State machine

```text
Block: free -> allocated -> free
Object slot: raw free -> constructed -> destroyed/raw free
```

## Invariants

- Alignment is a nonzero power of two and each returned pointer satisfies it.
- Free-list nodes belong to the pool and are not double-freed.
- Overflow allocation and release paths are distinguishable and balanced.

## Common bugs

- Invalid alignment arithmetic.
- Double release or releasing a foreign pointer.
- Constructor exceptions leaking a slot.
- Hiding time-dependent cache policy inside memory-core.

## Required tests

- `memory_pool_smoke_test`
- `object_pool_smoke_test`
- `memory_pool_bench` under `BUILD_BENCHMARKS=ON` for hot-path layout/performance changes
- ASan/UBSan for overflow and alignment changes
- `timer_tree_smoke_test` if ObjectPool behavior affects TimerQueue

## Forbidden dependencies

- `alyrn/net`
- CoroGateway
- `alyrn/time` for allocator primitives

## Patch rules

- Keep allocator code independent of business lifetime.
- Document ownership for every returned pointer.
- Do not convert clear single ownership into shared_ptr.
- Add failure-path tests for exhaustion and overflow.
- Do not add cache policy to this module.
