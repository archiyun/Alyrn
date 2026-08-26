---
name: runtime-reactor-maintenance
description: Maintain the coropact/reactor epoll Reactor, Loop, Channel, poller, and timers. Use for Reactor-layer patches, reviews, lifetime fixes, and threading changes.
---

# coropact/reactor Maintenance

Read `docs/SUBSYSTEMS.md` before editing.

## Purpose

Own Linux Reactor mechanics: Loop dispatch, Poller registration, Channel
event delivery, Reactor socket ownership, callback scheduling, and timerfd
integration. Shared address and socket primitives remain in `coropact/net`.
The coroutine-facing stream operations are thin adapters over this transport;
the superseded callback-oriented TCP object stack is not part of the public net
layer.

## Non-goals

- HTTP parsing, routing, retries, health policy, rate limiting, or upstream selection.
- Gateway-specific connection names or test hooks.
- Blocking task execution or io_uring implementation details.

## Owned resources

- Loop thread affinity and owner-local work queues.
- Poller registrations and Channel event masks.
- Listening and connected socket fds.
- `Listener`, `Connector`, and `Stream` state.
- TimerQueue timerfd and loop-bound timer indexes.

## Public API / entry points

- `Loop::{Loop,Quit,RunOnOwner,Schedule,RunAt,RunAfter,RunEvery,Cancel}`
- `Channel`, `Poller`, and `EPollPoller`
- `Listener`, `Connector`, and `Stream`

## Thread model

- One Loop is constructed, run, and destroyed on one owning thread.
- Channel, Poller, fd, stream state, and timer mutation belong to that loop.
- `RunOnOwner`, `Schedule`, timers, and `Quit` are owner-thread
  APIs; they are not cross-thread safe.
- Cross-thread delivery belongs to a separate mailbox design. Do not add a
  global lock or wakeup fd to compensate for wrong-thread access.

## Lifetime rules

- Loop outlives every registered Channel and owner-local queued callback/work target.
- Channel does not own its fd or callback owner; remove it before either is destroyed.
- A Reactor stream owns its transport state and is destroyed on its owning loop.
- Awaiting or owner-local deferred operations must complete or become inert before their owner dies.
- Raw `this` callbacks require owner-controlled cancellation and drain ordering.

## State machine

```text
Loop: created -> looping -> quit-requested -> stopped -> destroyed
Channel: unregistered -> registered -> disabled -> removed
Listener: open -> accepting -> closed
Connector: idle -> connecting -> connected | failed
Stream: open -> reading/writing -> half-closed | closed
Timer: pending-insert -> active -> executing -> active(repeat) | released
```

## Invariants

- One Loop per thread.
- A Channel is registered in at most one Poller and removed before fd reuse.
- Socket and Channel always refer to the same live fd.
- Transport callbacks execute on the owning loop.
- ET paths drain until `EAGAIN`; LT paths preserve unread bytes correctly.
- Owner-only operations do not inspect or mutate loop-owned state from another thread.
- Reactor code contains no gateway-specific naming or policy.

## Common bugs

- Raw `this` outliving a listener, connector, stream, or timer callback.
- Destroying a callback owner while its callback stack is active.
- Duplicate Channel removal or fd close.
- Reusing an fd before its Channel is removed.
- Unbounded output buffering and ignored backpressure signals.
- Timer cancellation assumed synchronous.
- Wall-clock deadlines mixed with monotonic timerfd scheduling.

## Required tests

- `epoll_poller_smoke_test`
- `reactor_loop_smoke_test`
- `reactor_stream_smoke_test`
- `reactor_listener_smoke_test`
- `net_move_smoke_test` for detached Channel and Socket ownership transfer
- `timer_tree_smoke_test`
- `timer_index_smoke_test`
- `io_buffer_smoke_test` for the public `coropact::io::Buffer`
- ASan/UBSan lifetime tests and TSan for cross-thread API changes

## Forbidden dependencies

- CoroGateway
- `coropact/task`
- Peer health, retries, route policy, or connection-name conventions

## Patch rules

- State the owning loop for each new resource.
- Keep work on the owning loop; cross-thread delivery requires the separate mailbox seam.
- Every fd path must identify one owner and one close point.
- Every async terminal path must be idempotent.
- Add a teardown/race test for lifetime fixes.
- Keep backend abstraction changes transport-oriented and usable without HTTP.
