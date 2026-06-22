---
name: runtime-time-maintenance
description: Maintain vexo/time Timestamp, Timer, TimerId, and timer indexes. Use for clock semantics, timer ordering, cancellation handles, and time-layer dependency reviews.
---

# vexo/time Maintenance

## Purpose

Provide time value types, clock conversion helpers, timer callback records,
stable cancellation IDs, and intrusive timer ordering/index types.

## Non-goals

- Owning timerfd, EventLoop, threads, sockets, or callback dispatch.
- Gateway deadlines, health intervals, retry policy, or cache policy.
- Logging policy.

## Owned resources

- Timestamp representation and formatting semantics.
- Timer callback, expiration, repeat interval, and sequence.
- TimerId identity semantics.
- TimerTree ordering.

## Public API / entry points

- `Timestamp::{Now,Invalid,Valid,ToString,ToFormattedString}`
- `TimeDifference`, `AddTime`
- `Timer::{Run,Restart,expiration,repeat,sequence}`
- `TimerId`
- `TimerTree`

## Thread model

- Timestamp and TimerId values are copyable/read-only values.
- Timer mutation is single-scheduler-owned.
- Global timer sequence generation is atomic.
- This module starts no threads and posts no callbacks.

## Lifetime rules

- Timer owns its callback but not the objects captured by that callback.
- TimerTree never owns Timer storage.
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
- Wall-clock values and monotonic durations are not silently interchangeable.
- Negative/zero intervals have explicit behavior.

## Common bugs

- Using `system_clock` for elapsed-time guarantees.
- Capturing raw objects in callbacks without an owner cancellation protocol.
- Unsigned underflow in time arithmetic.
- Repeating timers drifting or rearming from an inconsistent time base.
- Destroying a Timer while still linked.

## Required tests

- `timer_tree_smoke_test`
- `event_loop_smoke_test` for scheduling semantic changes
- `rbtree_validator` for TimerTree hook/order changes
- New tests for clock jumps, equal expiration ordering, stale TimerId, cancel
  during callback, and repeating timer drift

## Forbidden dependencies

- `vexo/net`
- `vexo/http`
- `vexo/gateway`
- `vexo/log`
- OS event-dispatch APIs such as epoll/timerfd

## Patch rules

- Keep time representation separate from dispatch.
- Introduce a distinct monotonic type or explicit clock parameter rather than
  overloading Timestamp semantics.
- Do not put EventLoop pointers in Timer or TimerId.
- Preserve stale-handle safety.
- Test ordering and cancellation edge cases, not only normal firing.
