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

}  // namespace
}  // namespace coropact::reactor
