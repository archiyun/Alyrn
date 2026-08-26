---
name: runtime-time-maintenance
description: Maintain coropact/time Duration, Deadline, Timer, TimerId, and timer indexes. Use for monotonic clock semantics, timer ordering, cancellation handles, and time-layer dependency reviews.
---

# coropact/time Maintenance

## Purpose

Provide time value types, clock conversion helpers, timer callback records,
stable cancellation IDs, and intrusive timer ordering/index types.

## Non-goals

- Owning timerfd, Loop, threads, sockets, or callback dispatch.
- Gateway deadlines, health intervals, retry policy, or cache policy.
- Logging policy.

## Owned resources

- Monotonic `Duration` and `Deadline` value semantics.
- Timer callback, expiration, repeat interval, and sequence.
- TimerId identity semantics.
- TimerIndex ordering over red-black tree and quadheap adapters.

## Public API / entry points

- `Clock`, `Deadline`, `Duration`, `SteadyNow()`
- `Nanoseconds`, `Microseconds`, `Milliseconds`, `Seconds`
- `TimerId`

`Timer`, `TimerTree`, and `TimerIndex` are TimerQueue
implementation types. They are not exported by `coropact/time.h`. Backend
timer queues and index tests include those headers directly.

## Thread model

- Deadline, Duration, and TimerId values are copyable/read-only values.
- Timer mutation is single-scheduler-owned.
- Global timer sequence generation is atomic.
- This module starts no threads and posts no callbacks.

## Lifetime rules

- Timer owns its callback but not the objects captured by that callback.
- TimerIndex never owns Timer storage.
- TimerId owns nothing and remains safe when stale.
- Intrusive hooks must be unlinked before Timer destruction.

## State machine

```text
Timer storage: constructed/unlinked -> indexed -> extracted -> restarted/indexed | destroyed
TimerId: invalid | issued -> stale after cancel/fire
```

## Invariants

- Timer ordering is a strict total order, including equal expirations.
- Sequence values are not reused during process lifetime.
- One Timer is linked into each intended intrusive index at most once.
- Runtime timer APIs accept monotonic deadlines and durations only; wall-clock
  values are not represented in this module.
- Negative/zero intervals have explicit behavior.

## Common bugs

- Using `system_clock` for elapsed-time guarantees.
- Capturing raw objects in callbacks without an owner cancellation protocol.
- Unsigned underflow in time arithmetic.
- Repeating timers drifting or rearming from an inconsistent time base.
- Destroying a Timer while still linked.

## Required tests

- `timer_tree_smoke_test`
- `timer_index_smoke_test`
- `reactor_loop_smoke_test` for scheduling semantic changes
- `rbtree_validator` for TimerTree hook/order changes
- New tests for clock jumps, equal expiration ordering, stale TimerId, cancel
  during callback, and repeating timer drift

## Forbidden dependencies

- `coropact/net`
- CoroGateway
- OS event-dispatch APIs such as epoll/timerfd

## Patch rules

- Keep time representation separate from dispatch.
- Keep runtime deadlines and durations on `time::Clock`; do not reintroduce a
  Unix-epoch timestamp or a floating-point seconds API.
- Do not put Loop pointers in Timer or TimerId.
- Preserve stale-handle safety.
- Test ordering and cancellation edge cases, not only normal firing.
