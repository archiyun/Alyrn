#include <exception>
#include <future>
#include <iostream>
#include <memory_resource>
#include <thread>
#include <utility>
#include <vector>

#include "coropact/coro/scheduler.h"
#include "coropact/coro/work.h"
#include "coropact/reactor/event_loop.h"
#include "coropact/reactor/event_loop_scheduler.h"
#include "coropact/time/timestamp.h"

namespace {

using namespace std::chrono_literals;

bool Expect(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "[FAIL] " << message << '\n';
        return false;
    }
    return true;
}

bool TestRunInLoopExecutesImmediately() {
    coropact::reactor::EventLoop loop;
    bool called = false;
    std::thread::id callback_thread;

    loop.RunInLoop([&] {
        called = true;
        callback_thread = std::this_thread::get_id();
    });

    return Expect(called, "RunInLoop should execute immediately on owner thread") &&
           Expect(callback_thread == std::this_thread::get_id(),
                  "RunInLoop callback should execute on owner thread");
}

bool TestQueueInLoopWakesLoop() {
    std::promise<coropact::reactor::EventLoop*> ready_promise;
    std::promise<std::thread::id> callback_thread_promise;
    std::promise<void> exited_promise;

    auto ready_future = ready_promise.get_future();
    auto callback_future = callback_thread_promise.get_future();
    auto exited_future = exited_promise.get_future();

    std::thread loop_thread([&] {
        coropact::reactor::EventLoop loop;
        ready_promise.set_value(&loop);
        loop.Loop();
        exited_promise.set_value();
    });

    coropact::reactor::EventLoop* loop = ready_future.get();
    loop->QueueInLoop([&] {
        callback_thread_promise.set_value(std::this_thread::get_id());
        loop->Quit();
    });

    const bool callback_ready =
        callback_future.wait_for(2s) == std::future_status::ready;
    const bool exited_ready =
        exited_future.wait_for(2s) == std::future_status::ready;
    bool ok = true;
    ok &= Expect(callback_ready, "QueueInLoop should wake the blocked loop");
    if (callback_ready) {
        ok &= Expect(callback_future.get() == loop_thread.get_id(),
                     "queued callback should run on the loop thread");
    }
    ok &= Expect(exited_ready, "loop should exit after Quit from queued callback");

    loop_thread.join();
    return ok;
}

bool TestNestedQueueInLoopSchedulesNextTurn() {
    std::promise<coropact::reactor::EventLoop*> ready_promise;
    std::promise<void> nested_functor_promise;
    std::promise<void> exited_promise;

    auto ready_future = ready_promise.get_future();
    auto nested_future = nested_functor_promise.get_future();
    auto exited_future = exited_promise.get_future();

    std::thread loop_thread([&] {
        coropact::reactor::EventLoop loop;
        ready_promise.set_value(&loop);
        loop.Loop();
        exited_promise.set_value();
    });

    coropact::reactor::EventLoop* loop = ready_future.get();
    loop->QueueInLoop([&] {
        loop->QueueInLoop([&] {
            nested_functor_promise.set_value();
            loop->Quit();
        });
    });

    const bool nested_ready =
        nested_future.wait_for(2s) == std::future_status::ready;
    const bool exited_ready =
        exited_future.wait_for(2s) == std::future_status::ready;

    bool ok = true;
    ok &= Expect(nested_ready, "functor queued from pending functor should run");
    ok &= Expect(exited_ready, "loop should exit after nested functor quits");
    loop_thread.join();
    return ok;
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
  coropact::reactor::EventLoopScheduler scheduler(&loop);

  bool ran = false;
  bool scheduler_matched = false;
  SchedulerProbeWork work(&scheduler, &ran, &scheduler_matched, &loop);

  scheduler.Schedule(&work);

  bool ok = true;
  ok &= Expect(!ran, "scheduler work must not run inline");

  loop.Loop();

  ok &= Expect(ran, "scheduler work should run through EventLoop");
  ok &= Expect(scheduler_matched, "scheduler work should run with its Scheduler::Current affinity");
  ok &= Expect(coropact::coro::Scheduler::Current() == nullptr,
               "scheduler work should restore the previous Scheduler::Current value");
  return ok;
}

bool TestEventLoopWorkQueueRejectsDuplicateAdmission() {
  coropact::reactor::EventLoop loop;
  coropact::reactor::EventLoopScheduler scheduler(&loop);

  bool ran = false;
  bool scheduler_matched = false;
  SchedulerProbeWork work(&scheduler, &ran, &scheduler_matched, &loop);

  const bool first_admission = loop.QueueWork(&work);
  const bool duplicate_admission = loop.QueueWork(&work);

  bool ok = true;
  ok &= Expect(first_admission, "first Work admission should succeed");
  ok &= Expect(!duplicate_admission, "duplicate Work admission should be rejected");
  ok &= Expect(!ran, "queued Work must remain deferred before Loop");

  loop.Loop();

  ok &= Expect(ran, "admitted Work should run through EventLoop");
  ok &= Expect(scheduler_matched, "admitted Work should preserve scheduler affinity");
  return ok;
}

bool TestEventLoopWorkQueueRequiresSchedulerBinding() {
  coropact::reactor::EventLoop loop;
  coropact::coro::Work work;
  work.SetRun([](coropact::coro::Work*) noexcept {});

  return Expect(!loop.QueueWork(&work),
                "Work admission should fail when no EventLoop scheduler is bound");
}

bool TestSchedulerMoveRetainsFrameResource() {
  coropact::reactor::EventLoop loop;
  std::pmr::monotonic_buffer_resource first_resource;
  std::pmr::monotonic_buffer_resource second_resource;

  auto created = coropact::reactor::EventLoopScheduler::Create(&loop, &first_resource);
  if (!Expect(created.has_value(), "scheduler creation should succeed")) {
    return false;
  }

  coropact::reactor::EventLoopScheduler moved(std::move(*created));
  coropact::reactor::EventLoopScheduler assigned(&loop, &second_resource);
  assigned = std::move(moved);

  return Expect(assigned.FrameResource() == &first_resource,
                "moved scheduler should retain its original frame resource");
}

bool TestSchedulerWorkFromForeignThreadWakesLoop() {
  using SchedulerContext =
      std::pair<coropact::reactor::EventLoop*, coropact::reactor::EventLoopScheduler*>;

  std::promise<SchedulerContext> ready_promise;
  std::promise<void> exited_promise;
  auto ready_future = ready_promise.get_future();
  auto exited_future = exited_promise.get_future();

  std::thread loop_thread([&] {
    coropact::reactor::EventLoop loop;
    coropact::reactor::EventLoopScheduler scheduler(&loop);
    ready_promise.set_value({&loop, &scheduler});
    loop.Loop();
    exited_promise.set_value();
  });

  const auto [loop, scheduler] = ready_future.get();
  bool ran = false;
  bool scheduler_matched = false;
  SchedulerProbeWork work(scheduler, &ran, &scheduler_matched, loop);

  scheduler->Schedule(&work);

  const bool exited = exited_future.wait_for(2s) == std::future_status::ready;
  if (!exited) {
    loop->Quit();
  }
  loop_thread.join();

  return Expect(exited, "foreign scheduler work should wake and finish the EventLoop") &&
         Expect(ran, "foreign scheduler work should run") &&
         Expect(scheduler_matched,
                "foreign scheduler work should preserve Scheduler::Current affinity");
}

class ScheduleNextWork final : public coropact::coro::Work {
public:
  ScheduleNextWork(coropact::reactor::EventLoopScheduler* scheduler, coropact::coro::Work* next,
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

  coropact::reactor::EventLoopScheduler* scheduler_;
  coropact::coro::Work* next_;
  bool* next_ran_;
  bool* next_was_deferred_;
};

bool TestSchedulerWorkScheduledDuringResumeIsDeferred() {
  coropact::reactor::EventLoop loop;
  coropact::reactor::EventLoopScheduler scheduler(&loop);

  bool second_ran = false;
  bool scheduler_matched = false;
  bool second_was_deferred = false;
  SchedulerProbeWork second(&scheduler, &second_ran, &scheduler_matched, &loop);
  ScheduleNextWork first(&scheduler, &second, &second_ran, &second_was_deferred);

  scheduler.Schedule(&first);
  loop.Loop();

  return Expect(second_was_deferred,
                "work scheduled during a resumed work item must not run inline") &&
         Expect(second_ran, "deferred work should run on a later EventLoop turn") &&
         Expect(scheduler_matched, "deferred work should retain scheduler affinity");
}

bool TestRepeatingTimerCanCancelItself() {
    std::promise<coropact::reactor::EventLoop*> ready_promise;
    std::promise<void> exited_promise;

    auto ready_future = ready_promise.get_future();
    auto exited_future = exited_promise.get_future();

    int fire_count = 0;
    coropact::time::TimerId timer_id;

    std::thread loop_thread([&] {
        coropact::reactor::EventLoop loop;
        ready_promise.set_value(&loop);
        loop.Loop();
        exited_promise.set_value();
    });

    coropact::reactor::EventLoop* loop = ready_future.get();
    loop->QueueInLoop([&] {
        timer_id = loop->RunEvery(0.01, [&] {
            ++fire_count;
            if (fire_count == 1) {
                loop->Cancel(timer_id);
                loop->RunAfter(0.05, [loop] { loop->Quit(); });
            }
        });
    });

    const bool exited_ready =
        exited_future.wait_for(2s) == std::future_status::ready;
    if (!exited_ready) {
        loop->Quit();
    }
    loop_thread.join();

    return Expect(exited_ready, "self-cancelling timer should not stall the loop") &&
           Expect(fire_count == 1,
                  "self-cancelling repeating timer should fire exactly once");
}

bool TestSameDeadlineTimersKeepSequenceOrder() {
    coropact::reactor::EventLoop loop;
    std::vector<int> fired;
    const auto deadline =
        coropact::time::AddTime(coropact::time::Timestamp::Now(), 0.01);

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
        if (!TestRunInLoopExecutesImmediately()) return 1;
        if (!TestQueueInLoopWakesLoop()) return 1;
        if (!TestNestedQueueInLoopSchedulesNextTurn()) return 1;
        if (!TestSchedulerWorkIsDeferredAndBound()) return 1;
        if (!TestEventLoopWorkQueueRejectsDuplicateAdmission()) return 1;
        if (!TestEventLoopWorkQueueRequiresSchedulerBinding()) return 1;
        if (!TestSchedulerMoveRetainsFrameResource()) return 1;
        if (!TestSchedulerWorkFromForeignThreadWakesLoop()) return 1;
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
