#include <gtest/gtest.h>

#include <thread>

#include "coropact/reactor/event_loop.h"

namespace coropact::reactor {
namespace {

TEST(EventLoopTest, RunOnOwnerExecutesImmediately) {
    EventLoop loop;
    bool called = false;
    std::thread::id callback_thread;

    loop.RunOnOwner([&] {
        called = true;
        callback_thread = std::this_thread::get_id();
    });

    EXPECT_TRUE(called);
    EXPECT_EQ(callback_thread, std::this_thread::get_id());
}

TEST(EventLoopTest, DeferOnOwnerRunsOnTheNextTurn) {
    EventLoop loop;
    bool called = false;
    std::thread::id callback_thread;

    loop.DeferOnOwner([&] {
        called = true;
        callback_thread = std::this_thread::get_id();
        loop.Quit();
    });
    EXPECT_FALSE(called);
    loop.Loop();
    EXPECT_TRUE(called);
    EXPECT_EQ(callback_thread, std::this_thread::get_id());
}

TEST(EventLoopTest, DeferOnOwnerFromCallbackRunsOnTheFollowingTurn) {
    EventLoop loop;
    bool first_called = false;
    bool second_called = false;

    loop.DeferOnOwner([&] {
        first_called = true;
        loop.DeferOnOwner([&] {
            second_called = true;
            loop.Quit();
        });
    });
    loop.Loop();
    EXPECT_TRUE(first_called);
    EXPECT_TRUE(second_called);
}

}  // namespace
}  // namespace coropact::reactor
