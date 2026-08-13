---
name: runtime-kqueue-maintenance
description: Maintain the coropact/kqueue BSD/Darwin readiness backend, KqueueLoop, Post, Channel, poller, timers, and master-slave worker handoff. Use for kqueue-layer patches, reviews, lifetime fixes, and threading changes.
---

# coropact/kqueue Maintenance

Read `docs/SUBSYSTEMS.md` and `docs/design/zh-CN/network/kqueue/` before editing.

## Purpose

Own BSD/Darwin kqueue mechanics: KqueueLoop dispatch, kevent registration,
Channel event delivery, one-shot stream adapters, user-space timers, and
master-slave worker bootstrap. This is a parallel adapter to the Linux epoll
Reactor, not a preprocessor branch inside `reactor`.

## Non-goals

- HTTP parsing, routing, retries, or gateway policy.
- Implementing kqueue as `#ifdef` inside Reactor or sharing poller/Channel code.
- Using `SO_REUSEPORT` as the kqueue multi-worker path.
- Scheduling coroutine `Work*` from a foreign thread.

## Owned resources

- `KqueueLoop` thread affinity, wakeup pipe, `posted_` queue, and owner-local work.
- Poller registrations and Channel event masks.
- Listening and connected socket fds; `Socket::Release` / `KqueueStream::Release`.
- `KqueueListener`, `KqueueConnector`, and `KqueueStream` state.
- TimerQueue: one `EVFILT_TIMER` plus a user-space timer tree.
- Master-slave `KqueueWorkerGroup`: acceptor on worker 0, I/O workers `1..n-1`.

## Public API / entry points

- `KqueueLoop::{Run,RequestStop,RunOnOwner,Post,Schedule,RunAt,RunAfter,RunEvery,Cancel}`
- `KqueueStream`, `KqueueListener`, `KqueueConnector`, `KqueueRecvSource`
- `Runtime::Builder<runtime::Kqueue>`
- `kqueue/detail` is not an application seam

## Thread model

Owner-thread only for Channel mutation, stream construction/move, `Schedule`,
and `Run`. `RequestStop` and `Post` are the cross-thread loop APIs. `Post`
holds `posted_mutex_` across `Wakeup()` so a 0-timeout poll cannot drain the
callback and destroy the wakeup pipe before the write returns.

`KqueueStream` cannot move across loops. Handoff is: `PeerAddress()`,
`Release()` the fd, `Post` a callback that reconstructs the stream on the
owner thread, then `SpawnDetach` the connection handler there.

I/O workers start before the acceptor so `Post` never observes a null loop.
`reuse_port` stays false. `Workers(n)` means n threads, not n listeners.

## Validation

- Native: `-DCOROPACT_ENABLE_KQUEUE=ON` on FreeBSD/NetBSD/OpenBSD/Darwin
  (worker-group and Runtime smoke).
- Linux shim: `-DCOROPACT_ENABLE_KQUEUE_SHIM_TESTS=ON` (oneshot state machine
  and `Post`; fake `kevent` does not watch real sockets).
