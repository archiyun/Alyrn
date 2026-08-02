#include <chrono>
#include <exception>
#include <iostream>
#include <memory_resource>
#include <thread>
#include <utility>
#include <vector>

#include "coropact/coro/scheduler.h"
#include "coropact/coro/work.h"
#include "coropact/reactor/event_loop.h"

namespace {

bool Expect(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "[FAIL] " << message << '\n';
        return false;
    }
    return true;
}

bool TestRunOnOwnerExecutesImmediately() {
    coropact::reactor::EventLoop loop;
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

class SchedulerProbeWork final : public coropact::coro::Work {
public:
  SchedulerProbeWork(coropact::coro::Scheduler* expected_scheduler, bool* ran,
                     bool* scheduler_matched, coropact::reactor::EventLoop* loop) noexcept
      : expected_scheduler_(expected_scheduler),
        ran_(ran),
        scheduler_matched_(scheduler_matched),
        loop_(loop) {
    SetRun(&RunProbe);
  }

private:
  static void RunProbe(coropact::coro::Work* work) noexcept {
    auto* self = static_cast<SchedulerProbeWork*>(work);
    *self->scheduler_matched_ =
        &coropact::coro::Scheduler::RequireCurrent() == self->expected_scheduler_;
    *self->ran_ = true;
    self->loop_->Quit();
  }

  coropact::coro::Scheduler* expected_scheduler_;
  bool* ran_;
  bool* scheduler_matched_;
  coropact::reactor::EventLoop* loop_;
};

bool TestSchedulerWorkIsDeferredAndBound() {
  coropact::reactor::EventLoop loop;

  bool ran = false;
  bool scheduler_matched = false;
  SchedulerProbeWork work(&loop, &ran, &scheduler_matched, &loop);

  loop.Schedule(&work);

  bool ok = true;
  ok &= Expect(!ran, "scheduler work must not run inline");

  loop.Loop();

  ok &= Expect(ran, "scheduler work should run through EventLoop");
  ok &= Expect(scheduler_matched, "scheduler work should run with its Scheduler::Current affinity");
  ok &= Expect(coropact::coro::Scheduler::Current() == nullptr,
               "scheduler work should restore the previous Scheduler::Current value");
  return ok;
}

bool TestEventLoopOwnsFrameResource() {
  std::pmr::monotonic_buffer_resource first_resource;
  coropact::reactor::EventLoop loop(&first_resource);
  return Expect(loop.FrameResource() == &first_resource,
                "EventLoop should retain its configured frame resource");
}

class ScheduleNextWork final : public coropact::coro::Work {
public:
  ScheduleNextWork(coropact::reactor::EventLoop* scheduler, coropact::coro::Work* next,
                   bool* next_ran, bool* next_was_deferred) noexcept
      : scheduler_(scheduler),
        next_(next),
        next_ran_(next_ran),
        next_was_deferred_(next_was_deferred) {
    SetRun(&RunScheduleNext);
  }

private:
  static void RunScheduleNext(coropact::coro::Work* work) noexcept {
    auto* self = static_cast<ScheduleNextWork*>(work);
    self->scheduler_->Schedule(self->next_);
    *self->next_was_deferred_ = !*self->next_ran_;
  }

  coropact::reactor::EventLoop* scheduler_;
  coropact::coro::Work* next_;
  bool* next_ran_;
  bool* next_was_deferred_;
};

bool TestSchedulerWorkScheduledDuringResumeIsDeferred() {
  coropact::reactor::EventLoop loop;

  bool second_ran = false;
  bool scheduler_matched = false;
  bool second_was_deferred = false;
  SchedulerProbeWork second(&loop, &second_ran, &scheduler_matched, &loop);
  ScheduleNextWork first(&loop, &second, &second_ran, &second_was_deferred);

  loop.Schedule(&first);
  loop.Loop();

  return Expect(second_was_deferred,
                "work scheduled during a resumed work item must not run inline") &&
         Expect(second_ran, "deferred work should run on a later EventLoop turn") &&
         Expect(scheduler_matched, "deferred work should retain scheduler affinity");
}

bool TestRepeatingTimerCanCancelItself() {
    coropact::reactor::EventLoop loop;
    int fire_count = 0;
    coropact::time::TimerId timer_id;

    timer_id = loop.RunEvery(0.01, [&] {
        ++fire_count;
        if (fire_count == 1) {
            loop.Cancel(timer_id);
            loop.RunAfter(0.05, [&loop] { loop.Quit(); });
        }
    });
    loop.Loop();

    return Expect(fire_count == 1,
                  "self-cancelling repeating timer should fire exactly once");
}

bool TestSameDeadlineTimersKeepSequenceOrder() {
    coropact::reactor::EventLoop loop;
    std::vector<int> fired;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(10);

    loop.RunAt(deadline, [&] { fired.push_back(1); });
    loop.RunAt(deadline, [&] { fired.push_back(2); });
    loop.RunAt(deadline, [&] {
        fired.push_back(3);
        loop.Quit();
    });
    loop.Loop();

    return Expect(fired == std::vector<int>({1, 2, 3}),
                  "same-deadline timers should follow sequence order");
}

bool TestCancelEarliestKeepsNextTimerScheduled() {
    coropact::reactor::EventLoop loop;
    bool cancelled_timer_fired = false;
    bool next_timer_fired = false;
    bool timed_out = false;

    auto cancelled = loop.RunAfter(0.01, [&] {
        cancelled_timer_fired = true;
    });
    loop.RunAfter(0.03, [&] {
        next_timer_fired = true;
        loop.Quit();
    });
    loop.RunAfter(0.5, [&] {
        timed_out = true;
        loop.Quit();
    });
    loop.Cancel(cancelled);
    loop.Loop();

    return Expect(!timed_out, "next timer should fire before watchdog") &&
           Expect(!cancelled_timer_fired,
                  "cancelled earliest timer should not fire") &&
           Expect(next_timer_fired,
                  "next timer should remain scheduled after cancellation");
}

bool TestStaleTimerIdCannotCancelReplacement() {
    coropact::reactor::EventLoop loop;
    bool replacement_fired = false;
    bool timed_out = false;

    auto stale = loop.RunAfter(60.0, [] {});
    loop.Cancel(stale);

    auto replacement = loop.RunAfter(0.01, [&] {
        replacement_fired = true;
        loop.Quit();
    });
    loop.RunAfter(0.5, [&] {
        timed_out = true;
        loop.Quit();
    });

    loop.Cancel(stale);
    loop.Loop();

    // The pool slot reuse that makes this an ABA hazard is deliberately no
    // longer observable through the handle: TimerId carries only the sequence.
    return Expect(stale.sequence != replacement.sequence,
                  "replacement timer should have a new sequence") &&
           Expect(!timed_out, "replacement timer should fire before watchdog") &&
           Expect(replacement_fired,
                  "stale TimerId should not cancel a replacement timer");
}

}  // namespace

int main() {
    try {
        if (!TestRunOnOwnerExecutesImmediately()) return 1;
        if (!TestSchedulerWorkIsDeferredAndBound()) return 1;
        if (!TestEventLoopOwnsFrameResource()) return 1;
        if (!TestSchedulerWorkScheduledDuringResumeIsDeferred()) return 1;
        if (!TestRepeatingTimerCanCancelItself()) return 1;
        if (!TestSameDeadlineTimersKeepSequenceOrder()) return 1;
        if (!TestCancelEarliestKeepsNextTimerScheduled()) return 1;
        if (!TestStaleTimerIdCannotCancelReplacement()) return 1;
    } catch (const std::exception& ex) {
        std::cerr << "[FAIL] unexpected exception: " << ex.what() << '\n';
        return 1;
    } catch (...) {
        std::cerr << "[FAIL] unexpected unknown exception\n";
        return 1;
    }

    std::cout << "[PASS] event_loop_smoke_test\n";
    return 0;
}
