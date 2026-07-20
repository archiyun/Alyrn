---
name: runtime-net-maintenance
description: Maintain the vexo/net Reactor transport, EventLoop, Channel, poller, sockets, and timers. Use for net-layer patches, reviews, lifetime fixes, and threading changes.
---

# vexo/net Maintenance

Read `docs/SUBSYSTEMS.md` before editing.

## Purpose

Own Linux Reactor mechanics: EventLoop dispatch, Poller registration, Channel
event delivery, socket ownership, callback scheduling, and timerfd integration.
The coroutine-facing stream operations are thin adapters over this transport;
the superseded callback-oriented TCP object stack is not part of the public net
layer.

## Non-goals

- HTTP parsing, routing, retries, health policy, rate limiting, or upstream selection.
- Gateway-specific connection names or test hooks.
- Blocking task execution or io_uring implementation details.

## Owned resources

- EventLoop thread affinity and wakeup fd.
- Poller registrations and Channel event masks.
- Listening and connected socket fds.
- `ReactorListener`, `ReactorConnector`, and `ReactorStream` state.
- TimerQueue timerfd and loop-bound timer indexes.

## Public API / entry points

- `EventLoop::{Loop,Quit,RunInLoop,QueueInLoop,RunAt,RunAfter,RunEvery,Cancel}`
- `Channel`, `Poller`, and `EPollPoller`
- `Socket`, `InetAddress`, and net utility APIs
- `ReactorListener`, `ReactorConnector`, and `ReactorStream`

## Thread model

- One EventLoop is constructed, run, and destroyed on one owning thread.
- Channel, Poller, fd, stream state, and timer mutation belong to that loop.
- Only explicitly documented posting APIs are cross-thread safe.
- Do not add a global lock to compensate for wrong-thread access.

## Lifetime rules

- EventLoop outlives every registered Channel and queued callback target.
- Channel does not own its fd or callback owner; remove it before either is destroyed.
- A Reactor stream owns its transport state and is destroyed on its owning loop.
- Awaiting or posted operations must complete or become inert before their owner dies.
- Raw `this` callbacks require owner-controlled cancellation and drain ordering.

## State machine

```text
EventLoop: created -> looping -> quit-requested -> stopped -> destroyed
Channel: unregistered -> registered -> disabled -> removed
Listener: open -> accepting -> closed
Connector: idle -> connecting -> connected | failed
Stream: open -> reading/writing -> half-closed | closed
Timer: pending-insert -> active -> executing -> active(repeat) | released
```

## Invariants

- One EventLoop per thread.
- A Channel is registered in at most one Poller and removed before fd reuse.
- Socket and Channel always refer to the same live fd.
- Transport callbacks execute on the owning loop.
- ET paths drain until `EAGAIN`; LT paths preserve unread bytes correctly.
- Cross-thread operations post to the owner and do not inspect loop-owned state first.
- Net code contains no gateway-specific naming or policy.

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
- `event_loop_smoke_test`
- `reactor_stream_smoke_test`
- `reactor_listener_smoke_test`
- `net_move_smoke_test` for detached Channel and Socket ownership transfer
- `timer_tree_smoke_test`
- `io_buffer_smoke_test` for the backend-neutral `vexo::io::Buffer`
- `vexo_integration_tests` EventLoop coverage
- `proxy_e2e_smoke_test` when a public transport contract changes
- ASan/UBSan lifetime tests and TSan for cross-thread API changes

## Forbidden dependencies

- `vexo/http`
- `vexo/gateway`
- `vexo/task`
- Peer health, retries, route policy, or connection-name conventions

## Patch rules

- State the owning loop for each new resource.
- Post work to the loop instead of locking loop-owned state.
- Every fd path must identify one owner and one close point.
- Every async terminal path must be idempotent.
- Add a teardown/race test for lifetime fixes.
- Keep backend abstraction changes transport-oriented and usable without HTTP.
