// SPDX-License-Identifier: MIT

/*
 * Owner-thread Loop behaviour, including the user-space timer tree
 * driven by a single EVFILT_TIMER.
 */

#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <exception>
#include <iostream>
#include <memory_resource>
#include <thread>
#include <vector>

#include "alyrn/detail/base/check.h"
#include "alyrn/detail/backend/loop.h"
#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/coro/task.h"
#include "alyrn/coro/work.h"
#include "alyrn/kqueue/connector.h"
#include "alyrn/detail/kqueue/channel.h"
#include "alyrn/kqueue/loop.h"
#include "alyrn/detail/net/socket.h"
#include "alyrn/time/clock.h"
#include "alyrn/time/timer_id.h"

namespace {

bool Expect(bool condition, const char* message) {
  if (!condition) {
    std::cerr << "[FAIL] " << message << '\n';
    return false;
  }
  return true;
}

class NoopWork final : public alyrn::coro::Work {
public:
  NoopWork() noexcept { SetRun(&RunNoop); }

private:
  static void RunNoop(alyrn::coro::Work*) noexcept {}
};

void DestroyLoopWithQueuedWork() {
  NoopWork work;
  alyrn::kqueue::Loop loop;
  loop.Schedule(&work);
}

bool TestLoopRejectsQueuedWorkAtDestruction() {
  const pid_t child = ::fork();
  if (child < 0) {
    return Expect(false, "fork failed for Loop destructor invariant test");
  }
  if (child == 0) {
    (void)::freopen("/dev/null", "w", stderr);
    DestroyLoopWithQueuedWork();
    ::_exit(0);
  }

  int status = 0;
  while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
  }
  return Expect(WIFSIGNALED(status),
                "Loop destruction with queued work must terminate in Release") &&
         Expect(WTERMSIG(status) == SIGABRT,
                "Loop queued-work invariant must terminate with SIGABRT");
}

void MutateChannelFromForeignThread() {
  alyrn::kqueue::Loop loop;
  int fds[2] = {-1, -1};
  ALYRN_CHECK(::pipe(fds) == 0, "pipe creation failed for Channel ownership test");
  ALYRN_CHECK(alyrn::net::SetNonBlocking(fds[0]).has_value(), "SetNonBlocking failed");
  alyrn::kqueue::detail::Channel channel(&loop, fds[0]);

  std::thread foreign([&] { channel.EnableReading(); });
  foreign.join();

  channel.DisableAll();
  channel.Remove();
  (void)::close(fds[0]);
  (void)::close(fds[1]);
}

bool TestLoopRejectsForeignChannelMutation() {
  const pid_t child = ::fork();
  if (child < 0) {
    return Expect(false, "fork failed for Channel ownership test");
  }
  if (child == 0) {
    (void)::freopen("/dev/null", "w", stderr);
    MutateChannelFromForeignThread();
    ::_exit(0);
  }

  int status = 0;
  while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
  }
  return Expect(WIFSIGNALED(status), "foreign Channel mutation must terminate in Release") &&
         Expect(WTERMSIG(status) == SIGABRT,
                "foreign Channel mutation must terminate with SIGABRT");
}

bool TestRunOnOwnerExecutesImmediately() {
  alyrn::kqueue::Loop loop;
  bool called = false;
  std::thread::id callback_thread;

  loop.RunOnOwner([&] {
    called = true;
    callback_thread = std::this_thread::get_id();
  });

  return Expect(called, "RunOnOwner should execute immediately on owner thread") &&
         Expect(callback_thread == std::this_thread::get_id(),
                "RunOnOwner callback should execute on owner thread");
}

class SchedulerProbeWork final : public alyrn::coro::Work {
public:
  SchedulerProbeWork(alyrn::coro::Scheduler* expected_scheduler, bool* ran,
                     bool* scheduler_matched, alyrn::kqueue::Loop* loop) noexcept
      : expected_scheduler_(expected_scheduler),
        ran_(ran),
        scheduler_matched_(scheduler_matched),
        loop_(loop) {
    SetRun(&RunProbe);
  }

private:
  static void RunProbe(alyrn::coro::Work* work) noexcept {
    auto* self = static_cast<SchedulerProbeWork*>(work);
    *self->scheduler_matched_ =
        &alyrn::coro::Scheduler::RequireCurrent() == self->expected_scheduler_;
    *self->ran_ = true;
    self->loop_->RequestStop();
  }

  alyrn::coro::Scheduler* expected_scheduler_;
  bool* ran_;
  bool* scheduler_matched_;
  alyrn::kqueue::Loop* loop_;
};

bool TestSchedulerWorkIsDeferredAndBound() {
  alyrn::kqueue::Loop loop;

  bool ran = false;
  bool scheduler_matched = false;
  SchedulerProbeWork work(&loop, &ran, &scheduler_matched, &loop);

  loop.Schedule(&work);

  bool ok = true;
  ok &= Expect(!ran, "scheduler work must not run inline");

  loop.Run();

  ok &= Expect(ran, "scheduler work should run through Loop");
  ok &= Expect(scheduler_matched, "scheduler work should run with its Scheduler::Current affinity");
  ok &= Expect(alyrn::coro::Scheduler::TryCurrent() == nullptr,
               "scheduler work should restore the previous Scheduler::Current value");
  return ok;
}

bool TestLoopOwnsFrameResource() {
  std::pmr::monotonic_buffer_resource first_resource;
  alyrn::kqueue::Loop loop(&first_resource);
  return Expect(loop.FrameResource() == &first_resource,
                "Loop should retain its configured frame resource");
}

class ScheduleNextWork final : public alyrn::coro::Work {
public:
  ScheduleNextWork(alyrn::kqueue::Loop* scheduler, alyrn::coro::Work* next,
                   bool* next_ran, bool* next_was_deferred) noexcept
      : scheduler_(scheduler),
        next_(next),
        next_ran_(next_ran),
        next_was_deferred_(next_was_deferred) {
    SetRun(&RunScheduleNext);
  }

private:
  static void RunScheduleNext(alyrn::coro::Work* work) noexcept {
    auto* self = static_cast<ScheduleNextWork*>(work);
    self->scheduler_->Schedule(self->next_);
    *self->next_was_deferred_ = !*self->next_ran_;
  }

  alyrn::kqueue::Loop* scheduler_;
  alyrn::coro::Work* next_;
  bool* next_ran_;
  bool* next_was_deferred_;
};

bool TestSchedulerWorkScheduledDuringResumeIsDeferred() {
  alyrn::kqueue::Loop loop;

  bool second_ran = false;
  bool scheduler_matched = false;
  bool second_was_deferred = false;
  SchedulerProbeWork second(&loop, &second_ran, &scheduler_matched, &loop);
  ScheduleNextWork first(&loop, &second, &second_ran, &second_was_deferred);

  loop.Schedule(&first);
  loop.Run();

  return Expect(second_was_deferred,
                "work scheduled during a resumed work item must not run inline") &&
         Expect(second_ran, "deferred work should run on a later Loop turn") &&
         Expect(scheduler_matched, "deferred work should retain scheduler affinity");
}

bool TestCrossThreadRequestStopWakesPoll() {
  alyrn::kqueue::Loop loop;
  std::atomic_bool stop_sent{false};

  std::jthread stopper([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    loop.RequestStop();
    stop_sent.store(true, std::memory_order_release);
  });

  const auto start = std::chrono::steady_clock::now();
  loop.Run();
  stopper.join();
  const auto elapsed = std::chrono::steady_clock::now() - start;

  return Expect(stop_sent.load(std::memory_order_acquire),
                "cross-thread stop request should be delivered") &&
         Expect(loop.State() == ::alyrn::detail::backend::LoopState::kStopped,
                "Loop should reach stopped after RequestStop") &&
         Expect(elapsed < std::chrono::seconds(1),
                "RequestStop should wake kevent instead of waiting for its poll timeout");
}

bool TestRepeatingTimerCanCancelItself() {
  alyrn::kqueue::Loop loop;
  int fire_count = 0;
  alyrn::time::TimerId timer_id;

  timer_id = loop.RunEvery(alyrn::time::Milliseconds(10), [&] {
    ++fire_count;
    if (fire_count == 1) {
      loop.Cancel(timer_id);
      loop.RunAfter(alyrn::time::Milliseconds(50), [&loop] { loop.RequestStop(); });
    }
  });
  loop.Run();

  return Expect(fire_count == 1, "self-cancelling repeating timer should fire exactly once");
}

bool TestSameDeadlineTimersKeepSequenceOrder() {
  alyrn::kqueue::Loop loop;
  std::vector<int> fired;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(10);

  loop.RunAt(deadline, [&] { fired.push_back(1); });
  loop.RunAt(deadline, [&] { fired.push_back(2); });
  loop.RunAt(deadline, [&] {
    fired.push_back(3);
    loop.RequestStop();
  });
  loop.Run();

  return Expect(fired == std::vector<int>({1, 2, 3}),
                "same-deadline timers should follow sequence order");
}

bool TestCancelEarliestKeepsNextTimerScheduled() {
  alyrn::kqueue::Loop loop;
  bool cancelled_timer_fired = false;
  bool next_timer_fired = false;
  bool timed_out = false;

  auto cancelled =
      loop.RunAfter(alyrn::time::Milliseconds(10), [&] { cancelled_timer_fired = true; });
  loop.RunAfter(alyrn::time::Milliseconds(30), [&] {
    next_timer_fired = true;
    loop.RequestStop();
  });
  loop.RunAfter(alyrn::time::Milliseconds(500), [&] {
    timed_out = true;
    loop.RequestStop();
  });
  loop.Cancel(cancelled);
  loop.Run();

  return Expect(!timed_out, "next timer should fire before watchdog") &&
         Expect(!cancelled_timer_fired, "cancelled earliest timer should not fire") &&
         Expect(next_timer_fired, "next timer should remain scheduled after cancellation");
}

bool TestStaleTimerIdCannotCancelReplacement() {
  alyrn::kqueue::Loop loop;
  bool replacement_fired = false;
  bool timed_out = false;

  auto stale = loop.RunAfter(alyrn::time::Seconds(60), [] {});
  loop.Cancel(stale);

  auto replacement = loop.RunAfter(alyrn::time::Milliseconds(10), [&] {
    replacement_fired = true;
    loop.RequestStop();
  });
  loop.RunAfter(alyrn::time::Milliseconds(500), [&] {
    timed_out = true;
    loop.RequestStop();
  });

  loop.Cancel(stale);
  loop.Run();

  return Expect(stale.sequence != replacement.sequence,
                "replacement timer should have a new sequence") &&
         Expect(!timed_out, "replacement timer should fire before watchdog") &&
         Expect(replacement_fired, "stale TimerId should not cancel a replacement timer");
}

bool TestLoopStopDiscardsUnexpiredTimer() {
  alyrn::kqueue::Loop loop;
  bool fired = false;

  loop.RunAfter(alyrn::time::Seconds(60), [&] { fired = true; });
  loop.RequestStop();
  loop.Run();

  return Expect(loop.State() == ::alyrn::detail::backend::LoopState::kStopped,
                "loop with an unexpired timer should stop") &&
         Expect(!fired, "loop shutdown must discard an unexpired timer without running it");
}

alyrn::coro::DetachedTask SleepUntilLoopStops(alyrn::kqueue::Connector* connector,
                                                 bool* resumed) {
  co_await connector->SleepFor(std::chrono::hours(1));
  *resumed = true;
}

bool TestLoopStopCancelsConnectorTimer() {
  alyrn::kqueue::Loop loop;
  alyrn::kqueue::Connector connector(&loop);
  bool resumed = false;

  alyrn::coro::SpawnDetach(loop, SleepUntilLoopStops(&connector, &resumed));
  std::jthread stopper([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    loop.RequestStop();
  });
  loop.Run();
  stopper.join();

  return Expect(resumed, "loop stop should settle Connector::SleepFor") &&
         Expect(loop.State() == ::alyrn::detail::backend::LoopState::kStopped,
                "timer cancellation should leave Loop stopped");
}

}  // namespace

int main() {
  try {
    if (!TestRunOnOwnerExecutesImmediately()) return 1;
    if (!TestLoopRejectsQueuedWorkAtDestruction()) return 1;
    if (!TestLoopRejectsForeignChannelMutation()) return 1;
    if (!TestSchedulerWorkIsDeferredAndBound()) return 1;
    if (!TestLoopOwnsFrameResource()) return 1;
    if (!TestSchedulerWorkScheduledDuringResumeIsDeferred()) return 1;
    if (!TestRepeatingTimerCanCancelItself()) return 1;
    if (!TestSameDeadlineTimersKeepSequenceOrder()) return 1;
    if (!TestCancelEarliestKeepsNextTimerScheduled()) return 1;
    if (!TestStaleTimerIdCannotCancelReplacement()) return 1;
    if (!TestLoopStopDiscardsUnexpiredTimer()) return 1;
    if (!TestCrossThreadRequestStopWakesPoll()) return 1;
    if (!TestLoopStopCancelsConnectorTimer()) return 1;
  } catch (const std::exception& ex) {
    std::cerr << "[FAIL] unexpected exception: " << ex.what() << '\n';
    return 1;
  } catch (...) {
    std::cerr << "[FAIL] unexpected unknown exception\n";
    return 1;
  }

  std::cout << "[PASS] kqueue_loop_smoke_test\n";
  return 0;
}
