#include <exception>
#include <future>
#include <iostream>
#include <thread>
#include <vector>

#include "vexo/net/event_loop.h"
#include "vexo/time/timestamp.h"

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
    vexo::net::EventLoop loop;
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
    std::promise<vexo::net::EventLoop*> ready_promise;
    std::promise<std::thread::id> callback_thread_promise;
    std::promise<void> exited_promise;

    auto ready_future = ready_promise.get_future();
    auto callback_future = callback_thread_promise.get_future();
    auto exited_future = exited_promise.get_future();

    std::thread loop_thread([&] {
        vexo::net::EventLoop loop;
        ready_promise.set_value(&loop);
        loop.Loop();
        exited_promise.set_value();
    });

    vexo::net::EventLoop* loop = ready_future.get();
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
    std::promise<vexo::net::EventLoop*> ready_promise;
    std::promise<void> nested_functor_promise;
    std::promise<void> exited_promise;

    auto ready_future = ready_promise.get_future();
    auto nested_future = nested_functor_promise.get_future();
    auto exited_future = exited_promise.get_future();

    std::thread loop_thread([&] {
        vexo::net::EventLoop loop;
        ready_promise.set_value(&loop);
        loop.Loop();
        exited_promise.set_value();
    });

    vexo::net::EventLoop* loop = ready_future.get();
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

bool TestRepeatingTimerCanCancelItself() {
    std::promise<vexo::net::EventLoop*> ready_promise;
    std::promise<void> exited_promise;

    auto ready_future = ready_promise.get_future();
    auto exited_future = exited_promise.get_future();

    int fire_count = 0;
    vexo::time::TimerId timer_id;

    std::thread loop_thread([&] {
        vexo::net::EventLoop loop;
        ready_promise.set_value(&loop);
        loop.Loop();
        exited_promise.set_value();
    });

    vexo::net::EventLoop* loop = ready_future.get();
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
    vexo::net::EventLoop loop;
    std::vector<int> fired;
    const auto deadline =
        vexo::time::AddTime(vexo::time::Timestamp::Now(), 0.01);

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
    vexo::net::EventLoop loop;
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
    vexo::net::EventLoop loop;
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
