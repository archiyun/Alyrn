// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// C3 acceptance: a detached connection coroutine (Spawn + Detach) must be driven
// to completion by RESUMING it -- never destroyed from outside while suspended --
// and the spawn machinery must self-destruct with no leak and no use-after-free.
// JoinState is what makes Detach safe: the producer (the spawned root) frees the
// state on completion once the consumer has detached.
//
// No real Channel: FakeConn stands in for the Channel read slot. Read() parks the
// coroutine handle; Deliver() (run on the loop thread) hands back a result and
// Schedules the resume. Two exit paths are covered:
//   conn A: Deliver(0)            -> EOF, graceful co_return
//   conn B: Deliver(unexpected)   -> teardown via resume-with-error
// LoopScheduler adapts the coro Scheduler onto the EventLoop: the initial root
// submission runs on the loop thread; subsequent IO resumes go straight through
// EventLoop::Schedule(handle). Run under ASan (leak check proves self-destruct)
// and TSan (cross-thread Schedule/RunInLoop).

#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <coroutine>
#include <expected>
#include <future>
#include <mutex>
#include <print>
#include <thread>
#include <utility>

#include "vexo/base/error.h"
#include "vexo/coro/scheduler.h"
#include "vexo/coro/spawn.h"
#include "vexo/coro/task.h"
#include "vexo/coro/work.h"
#include "vexo/net/event_loop.h"

using vexo::base::make_errno;
using vexo::base::Result;
using vexo::coro::Spawn;
using vexo::coro::Task;
using vexo::coro::Work;
using vexo::net::EventLoop;

namespace {

// Adapts the coarse-grained coro Scheduler onto an EventLoop: a submitted Work
// is run on the loop thread. std::function lives in this net-side adapter, never
// in the coro module.
class LoopScheduler final : public vexo::coro::Scheduler {
public:
  explicit LoopScheduler(EventLoop* loop) noexcept : loop_(loop) {}
  void Schedule(Work* work) override {
    loop_->RunInLoop([work] { work->run(work); });
  }

private:
  EventLoop* loop_;
};

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

  // Loop-thread side: deliver `r`, then schedule the parked coroutine's resume.
  void Deliver(Result<int> r, EventLoop* loop) {
    next = std::move(r);
    if (auto h = std::exchange(parked, {})) loop->Schedule(h);
  }
};

constexpr int kConns = 2;
std::atomic<int> g_completed{0};
std::atomic<int> g_on_loop{0};
EventLoop* g_loop = nullptr;
std::thread::id g_loop_tid;
std::promise<void> g_all_done;

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
  if (g_completed.fetch_add(1) + 1 == kConns) g_all_done.set_value();
  co_return;
}

}  // namespace

int main() {
  std::mutex m;
  std::condition_variable cv;

  std::jthread worker([&] {
    EventLoop loop;
    {
      std::lock_guard lk{m};
      g_loop = &loop;
      g_loop_tid = std::this_thread::get_id();
    }
    cv.notify_one();
    loop.Loop();
  });

  {
    std::unique_lock lk{m};
    cv.wait(lk, [] { return g_loop != nullptr; });
  }

  // FakeConns must outlive their coroutines (they finish before join below).
  FakeConn conn_a;
  FakeConn conn_b;

  // Spawn both detached coroutines on the owning scheduler, then queue the
  // deliveries for the following loop iteration, after both roots have parked on
  // their first Read.
  g_loop->RunInLoop([&] {
    static LoopScheduler sched(g_loop);
    Spawn(sched, Serve(&conn_a)).Detach();
    Spawn(sched, Serve(&conn_b)).Detach();
    g_loop->QueueInLoop([&] { conn_a.Deliver(Result<int>{0}, g_loop); });  // EOF
    g_loop->QueueInLoop([&] { conn_b.Deliver(std::unexpected(make_errno(ECONNRESET)), g_loop); });
  });

  auto fut = g_all_done.get_future();
  if (fut.wait_for(std::chrono::seconds(3)) != std::future_status::ready) {
    std::println("FAIL: watchdog timed out -- detached coroutines never finished");
    g_loop->Quit();
    return 1;
  }
  g_loop->Quit();
  worker.join();

  const int done = g_completed.load();
  const int on_loop = g_on_loop.load();
  if (done != kConns || on_loop != kConns) {
    std::println("FAIL: completed={} on_loop={} expected={}", done, on_loop, kConns);
    return 1;
  }
  std::println("spawn c3 smoke: PASS ({} detached coroutines resumed & self-destructed)", kConns);
  return 0;
}
