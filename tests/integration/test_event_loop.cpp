#include <gtest/gtest.h>

#include "runtime/net/event_loop.h"

#include <future>
#include <thread>

namespace runtime::net {
namespace {

using namespace std::chrono_literals;

TEST(EventLoopTest, RunInLoopExecutesImmediatelyOnOwningThread) {
    EventLoop loop;
    bool called = false;
    std::thread::id callback_thread;

    loop.RunInLoop([&] {
        called = true;
        callback_thread = std::this_thread::get_id();
    });

    EXPECT_TRUE(called);
    EXPECT_EQ(callback_thread, std::this_thread::get_id());
}

TEST(EventLoopTest, QueueInLoopFromAnotherThreadWakesLoop) {
    std::promise<EventLoop*> ready_promise;
    std::promise<std::thread::id> callback_thread_promise;
    std::promise<void> exited_promise;

    auto ready_future = ready_promise.get_future();
    auto callback_future = callback_thread_promise.get_future();
    auto exited_future = exited_promise.get_future();

    std::thread loop_thread([&] {
        EventLoop loop;
        ready_promise.set_value(&loop);
        loop.Loop();
        exited_promise.set_value();
    });

    EventLoop* loop = ready_future.get();
    loop->QueueInLoop([&] {
        callback_thread_promise.set_value(std::this_thread::get_id());
        loop->Quit();
    });

    EXPECT_EQ(callback_future.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(callback_future.get(), loop_thread.get_id());
    EXPECT_EQ(exited_future.wait_for(2s), std::future_status::ready);

    loop_thread.join();
}

TEST(EventLoopTest, QueueInLoopFromPendingFunctorSchedulesNextTurn) {
    std::promise<EventLoop*> ready_promise;
    std::promise<void> nested_functor_promise;
    std::promise<void> exited_promise;

    auto ready_future = ready_promise.get_future();
    auto nested_future = nested_functor_promise.get_future();
    auto exited_future = exited_promise.get_future();

    std::thread loop_thread([&] {
        EventLoop loop;
        ready_promise.set_value(&loop);
        loop.Loop();
        exited_promise.set_value();
    });

    EventLoop* loop = ready_future.get();
    loop->QueueInLoop([&] {
        loop->QueueInLoop([&] {
            nested_functor_promise.set_value();
            loop->Quit();
        });
    });

    EXPECT_EQ(nested_future.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(exited_future.wait_for(2s), std::future_status::ready);

    loop_thread.join();
}

}  // namespace
}  // namespace runtime::net
