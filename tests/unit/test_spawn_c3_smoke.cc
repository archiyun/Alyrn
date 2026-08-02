// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// C3 acceptance: a detached connection coroutine (Spawn + Detach) must be driven
// to completion by RESUMING it -- never destroyed from outside while suspended --
// and the spawn machinery must self-destruct with no leak and no use-after-free.
// SpawnState is embedded in the SpawnRoot frame. It makes Detach safe: once the
// consumer has detached, the producer destroys its own root frame on completion.
//
// No real Channel: FakeConn stands in for the Channel read slot. Read() parks the
// coroutine handle; Deliver() (run on the loop thread) hands back a result and
// defers the resume. Two exit paths are covered:
//   conn A: Deliver(0)            -> EOF, graceful co_return
//   conn B: Deliver(unexpected)   -> teardown via resume-with-error
// EventLoop implements the coro Scheduler directly: the initial root submission
// and subsequent IO resumes run through its owner-local Work queue. Run under
// ASan (leak check proves self-destruct) and TSan.

#include <atomic>
#include <cerrno>
#include <chrono>
#include <coroutine>
#include <expected>
#include <iostream>
#include <thread>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/spawn.h"
#include "coropact/coro/task.h"
#include "coropact/coro/work.h"
#include "coropact/reactor/event_loop.h"

using coropact::base::MakeErrno;
using coropact::base::Result;
using coropact::coro::Spawn;
using coropact::coro::Task;
using coropact::coro::Work;
using coropact::reactor::EventLoop;

namespace {

// Stand-in for a Channel read slot: parks one coroutine handle, hands back a
// scripted Result<int> when resumed. All access happens on the loop thread.
struct FakeConn {
  std::coroutine_handle<> parked{};
  Result<int> next{0};

  struct ReadAwaiter {
    FakeConn* c;
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) noexcept { c->parked = h; }
    Result<int> await_resume() noexcept { return std::move(c->next); }
  };
  ReadAwaiter Read() noexcept { return ReadAwaiter{this}; }

  // Owner-thread side: deliver `r`, then defer the parked coroutine's resume
  // to the next EventLoop turn.
  void Deliver(Result<int> r, EventLoop* loop) {
    next = std::move(r);
    if (auto h = std::exchange(parked, {})) {
      loop->DeferOnOwner([h] {
        if (!h.done()) h.resume();
      });
    }
  }
};

constexpr int kConns = 2;
std::atomic<int> g_completed{0};
std::atomic<int> g_on_loop{0};
EventLoop* g_loop = nullptr;
std::thread::id g_loop_tid;

// Top-level connection coroutine: read until EOF or read error, then exit.
// Errors are consumed here (the real one would close the connection); the
// detached root therefore has an explicit Task<void> boundary.
Task<void> Serve(FakeConn* c) {
  for (;;) {
    Result<int> r = co_await c->Read();
    if (!r) break;       // teardown: read failed -> clean up and exit
    if (*r == 0) break;  // EOF
  }
  if (std::this_thread::get_id() == g_loop_tid) g_on_loop.fetch_add(1);
  if (g_completed.fetch_add(1) + 1 == kConns) g_loop->Quit();
  co_return;
}

}  // namespace

int main() {
  EventLoop loop;
  g_loop = &loop;
  g_loop_tid = std::this_thread::get_id();

  // FakeConns must outlive their coroutines (they finish before join below).
  FakeConn conn_a;
  FakeConn conn_b;

  // Spawn both detached coroutines on the owning scheduler, then defer the
  // deliveries for the following loop iteration, after both roots have parked on
  // their first Read.
  Spawn(loop, Serve(&conn_a)).Detach();
  Spawn(loop, Serve(&conn_b)).Detach();
  g_loop->DeferOnOwner([&] { conn_a.Deliver(Result<int>{0}, g_loop); });  // EOF
  g_loop->DeferOnOwner(
      [&] { conn_b.Deliver(std::unexpected(MakeErrno(ECONNRESET)), g_loop); });
  loop.Loop();

  const int done = g_completed.load();
  const int on_loop = g_on_loop.load();
  if (done != kConns || on_loop != kConns) {
    std::cout << "FAIL: completed=" << done << " on_loop=" << on_loop << " expected=" << kConns
              << '\n';
    return 1;
  }
  std::cout << "spawn c3 smoke: PASS (" << kConns
            << " detached coroutines resumed & self-destructed)\n";
  return 0;
}
