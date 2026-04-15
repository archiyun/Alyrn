#include "runtime/net/event_loop.h"

#include <exception>
#include <future>
#include <iostream>
#include <thread>

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
    runtime::net::EventLoop loop;
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
    std::promise<runtime::net::EventLoop*> ready_promise;
    std::promise<std::thread::id> callback_thread_promise;
    std::promise<void> exited_promise;

    auto ready_future = ready_promise.get_future();
    auto callback_future = callback_thread_promise.get_future();
    auto exited_future = exited_promise.get_future();

    std::thread loop_thread([&] {
        runtime::net::EventLoop loop;
        ready_promise.set_value(&loop);
        loop.Loop();
        exited_promise.set_value();
    });

    runtime::net::EventLoop* loop = ready_future.get();
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
    std::promise<runtime::net::EventLoop*> ready_promise;
    std::promise<void> nested_functor_promise;
    std::promise<void> exited_promise;

    auto ready_future = ready_promise.get_future();
    auto nested_future = nested_functor_promise.get_future();
    auto exited_future = exited_promise.get_future();

    std::thread loop_thread([&] {
        runtime::net::EventLoop loop;
        ready_promise.set_value(&loop);
        loop.Loop();
        exited_promise.set_value();
    });

    runtime::net::EventLoop* loop = ready_future.get();
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

}  // namespace

int main() {
    try {
        if (!TestRunInLoopExecutesImmediately()) return 1;
        if (!TestQueueInLoopWakesLoop()) return 1;
        if (!TestNestedQueueInLoopSchedulesNextTurn()) return 1;
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
