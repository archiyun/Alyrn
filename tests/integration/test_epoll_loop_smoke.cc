#include <sys/eventfd.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <exception>
#include <iostream>
#include <memory_resource>
#include <thread>
#include <utility>
#include <vector>

#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/coro/work.h"
#include "alyrn/io/loop.h"
#include "alyrn/epoll/connector.h"
#include "alyrn/detail/epoll/channel.h"
#include "alyrn/epoll/loop.h"

namespace {

bool Expect(bool condition, const char* message) {
  if (!condition) {
    std::cerr << "[FAIL] " << message << '\n';
    return false;
  }
  return true;
}

bool ExpectChildAbort(void (*entry)(), const char* message) {
  const pid_t child = ::fork();
  if (child < 0) {
    return Expect(false, "fork failed for Loop affinity test");
  }
  if (child == 0) {
    (void)::freopen("/dev/null", "w", stderr);
    entry();
    ::_exit(0);
  }

  int status = 0;
  while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
  }
  return Expect(WIFSIGNALED(status), message) &&
         Expect(WTERMSIG(status) == SIGABRT,
                "loop-affinity invariant must terminate with SIGABRT");
}

class NoopWork final : public alyrn::coro::Work {
public:
  NoopWork() noexcept { SetRun(&RunNoop); }

private:
  static void RunNoop(alyrn::coro::Work*) noexcept {}
};

void DestroyLoopWithQueuedWork() {
  NoopWork work;
  alyrn::epoll::Loop loop;
  loop.Schedule(&work);
}

bool TestEpollLoopRejectsQueuedWorkAtDestruction() {
  return ExpectChildAbort(&DestroyLoopWithQueuedWork,
                          "Loop destruction with queued work must terminate in Release");
}

void MutateChannelFromForeignThread() {
  alyrn::epoll::Loop loop;
  const int fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  ALYRN_CHECK(fd >= 0, "eventfd creation failed for Channel ownership test");
  alyrn::epoll::detail::Channel channel(&loop, fd);

  std::thread foreign([&] { channel.EnableReading(); });
  foreign.join();

  channel.DisableAll();
  channel.Remove();
  (void)::close(fd);
}

bool TestEpollLoopRejectsForeignChannelMutation() {
  return ExpectChildAbort(&MutateChannelFromForeignThread,
                          "foreign Channel mutation must terminate in Release");
}

void RunAfterFromForeignThread() {
  alyrn::epoll::Loop loop;
  std::thread foreign([&loop] { (void)loop.RunAfter(alyrn::time::Duration::zero(), [] {}); });
  foreign.join();
}

void DestroyLoopFromForeignThread() {
  auto* loop = new alyrn::epoll::Loop;
  std::thread foreign([loop] { delete loop; });
  foreign.join();
}

bool TestEpollLoopAffinityIsEnforcedInRelease() {
  return ExpectChildAbort(&RunAfterFromForeignThread,
                          "RunAfter from a foreign thread must terminate in Release") &&
         ExpectChildAbort(&DestroyLoopFromForeignThread,
                          "Loop destruction from a foreign thread must terminate in Release");
}

bool TestRunOnOwnerExecutesImmediately() {
  alyrn::epoll::Loop loop;
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
                     bool* scheduler_matched, alyrn::epoll::Loop* loop) noexcept
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
  alyrn::epoll::Loop* loop_;
};

bool TestSchedulerWorkIsDeferredAndBound() {
  alyrn::epoll::Loop loop;

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

bool TestEpollLoopOwnsFrameResource() {
  std::pmr::monotonic_buffer_resource first_resource;
  alyrn::epoll::Loop loop(&first_resource);
  return Expect(loop.FrameResource() == &first_resource,
                "Loop should retain its configured frame resource");
}

class ScheduleNextWork final : public alyrn::coro::Work {
public:
  ScheduleNextWork(alyrn::epoll::Loop* scheduler, alyrn::coro::Work* next,
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

  alyrn::epoll::Loop* scheduler_;
  alyrn::coro::Work* next_;
  bool* next_ran_;
  bool* next_was_deferred_;
};

bool TestSchedulerWorkScheduledDuringResumeIsDeferred() {
  alyrn::epoll::Loop loop;

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

bool TestRepeatingTimerCanCancelItself() {
  alyrn::epoll::Loop loop;
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

bool TestSameDeadlineTimersKeepSequenceOrderOnQuadHeap() {
  alyrn::epoll::Loop loop;
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
                "quadheap same-deadline timers should follow sequence order");
}

bool TestSameDeadlineTimersKeepSequenceOrder() {
  alyrn::epoll::Loop loop;
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
  alyrn::epoll::Loop loop;
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
  alyrn::epoll::Loop loop;
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

  // The pool slot reuse that makes this an ABA hazard is deliberately no
  // longer observable through the handle: TimerId carries only the sequence.
  return Expect(stale.sequence != replacement.sequence,
                "replacement timer should have a new sequence") &&
         Expect(!timed_out, "replacement timer should fire before watchdog") &&
         Expect(replacement_fired, "stale TimerId should not cancel a replacement timer");
}

bool TestLoopStopDiscardsUnexpiredTimer() {
  alyrn::epoll::Loop loop;
  bool fired = false;

  loop.RunAfter(alyrn::time::Seconds(60), [&] { fired = true; });
  loop.RequestStop();
  loop.Run();

  return Expect(loop.State() == alyrn::io::LoopState::kStopped,
                "loop with an unexpired timer should stop") &&
         Expect(!fired, "loop shutdown must discard an unexpired timer without running it");
}

bool TestCrossThreadRequestStopWakesPoll() {
  alyrn::epoll::Loop loop;
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
         Expect(loop.State() == alyrn::io::LoopState::kStopped,
                "Loop should reach stopped after RequestStop") &&
         Expect(elapsed < std::chrono::seconds(1),
                "RequestStop should wake epoll_wait instead of waiting for its poll timeout");
}

alyrn::coro::DetachedTask SleepUntilLoopStops(alyrn::epoll::Connector* connector,
                                                 bool* resumed) {
  co_await connector->SleepFor(std::chrono::hours(1));
  *resumed = true;
}

bool TestLoopStopCancelsConnectorTimer() {
  alyrn::epoll::Loop loop;
  alyrn::epoll::Connector connector(&loop);
  bool resumed = false;

  alyrn::coro::SpawnDetach(loop, SleepUntilLoopStops(&connector, &resumed));
  std::jthread stopper([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    loop.RequestStop();
  });
  loop.Run();
  stopper.join();

  return Expect(resumed, "loop stop should settle Connector::SleepFor") &&
         Expect(loop.State() == alyrn::io::LoopState::kStopped,
                "timer cancellation should leave Loop stopped");
}

}  // namespace

int main() {
  try {
    if (!TestRunOnOwnerExecutesImmediately()) return 1;
    if (!TestEpollLoopRejectsQueuedWorkAtDestruction()) return 1;
    if (!TestEpollLoopRejectsForeignChannelMutation()) return 1;
    if (!TestEpollLoopAffinityIsEnforcedInRelease()) return 1;
    if (!TestSchedulerWorkIsDeferredAndBound()) return 1;
    if (!TestEpollLoopOwnsFrameResource()) return 1;
    if (!TestSchedulerWorkScheduledDuringResumeIsDeferred()) return 1;
    if (!TestRepeatingTimerCanCancelItself()) return 1;
    if (!TestSameDeadlineTimersKeepSequenceOrder()) return 1;
    if (!TestSameDeadlineTimersKeepSequenceOrderOnQuadHeap()) return 1;
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

  std::cout << "[PASS] epoll_loop_smoke_test\n";
  return 0;
}
