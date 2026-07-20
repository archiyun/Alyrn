---
name: runtime-net-maintenance
description: Maintain vexo/net reactor, EventLoop, Channel, TCP, poller, timerfd, and backend code. Use for net-layer patches, reviews, lifetime fixes, and threading changes.
---

# vexo/net Maintenance

Read `docs/SUBSYSTEMS.md` before editing.

## Purpose

Own Linux transport mechanics: EventLoop dispatch, Poller registration,
Channel event delivery, sockets, TCP client/server connections, buffers,
timerfd integration, and transport backend adapters.

## Non-goals

- HTTP parsing, routing, retries, health policy, rate limiting, or upstream
  selection.
- Gateway-specific connection names or test hooks.
- Blocking task execution.

## Owned resources

- EventLoop thread affinity and wakeup fd.
- Poller registrations and Channel event masks.
- Listening/connected socket fds.
- TcpConnection input/output buffers and transport state.
- TimerQueue timerfd and loop-bound timer indexes.

## Public API / entry points

- `EventLoop::{Loop,Quit,RunInLoop,QueueInLoop,RunAt,RunAfter,RunEvery,Cancel}`
- `Channel`
- `TcpServer`, `TcpClient`, `TcpConnection`
- `Buffer`, `InetAddress`, socket/net utility APIs

## Thread model

- One EventLoop is constructed, run, and destroyed on one thread.
- Channel, Poller, fd, connection state, and buffer mutation belong to that
  loop.
- Only explicitly documented posting APIs are cross-thread safe.
- Do not add a global lock to compensate for wrong-thread access.

## Lifetime rules

- EventLoop outlives every registered Channel and queued callback target.
- Channel does not own its fd or callback owner.
- TcpConnection is shared only because async callbacks may extend connection
  lifetime; Channel::Tie is a dispatch guard, not the primary owner.
- `ConnectDestroyed` runs once on the connection's loop after all registrations
  are disabled.
- Raw `this` callbacks require owner-controlled cancellation and drain ordering.

## State machine

```text
EventLoop: created -> looping -> quit-requested -> stopped -> destroyed
Channel: unregistered -> registered -> disabled -> removed
TcpConnection: connecting -> connected -> disconnecting -> disconnected
Connector: disconnected -> connecting -> connected | retrying -> disconnected
Timer: pending-insert -> active -> executing -> active(repeat) | released
```

## Invariants

- One EventLoop per thread.
- A Channel is registered in at most one Poller and removed before fd reuse.
- Socket and Channel always refer to the same live fd.
- Connection callbacks execute on the owning loop.
- ET paths drain until `EAGAIN`; LT paths preserve unread bytes correctly.
- Cross-thread Send/Shutdown has no non-atomic access to loop-owned state.
- Net code contains no gateway-specific naming or policy.

## Common bugs

- Raw `this` outliving TcpClient/TcpServer/backend adapters.
- Destroying a callback owner while its callback stack is active.
- Direct sub-loop connection teardown from the base loop.
- Duplicate Channel removal or fd close.
- Unbounded output buffering and ignored high-water signals.
- Timer cancellation assumed synchronous.
- Wall-clock deadlines mixed with monotonic timerfd scheduling.

## Required tests

- `buffer_smoke_test`
- `epoll_poller_smoke_test`
- `event_loop_smoke_test`
- `tcp_server_smoke_test`
- `rst_storm_smoke_test`
- `timer_tree_smoke_test`
- `vexo_integration_tests` filters for EventLoop/TcpServer when available
- `test_trigger_mode` coverage for Channel/TcpConnection ET changes
- ASan/UBSan lifetime tests and TSan for cross-thread API changes
- `proxy_e2e_smoke_test` when a public net contract changes

## Forbidden dependencies

- `vexo/http`
- `vexo/gateway`
- `vexo/task`
- Peer health, retries, route policy, or connection-name conventions

## Patch rules

- State the owning loop for each new resource.
- Post work to the loop instead of locking loop-owned state.
- Do not change external callbacks or ownership types without a lifetime proof.
- Every fd path must identify one owner and one close point.
- Every async terminal path must be idempotent.
- Add a teardown/race test for lifetime fixes.
- Keep backend abstraction changes transport-oriented and usable without HTTP.
