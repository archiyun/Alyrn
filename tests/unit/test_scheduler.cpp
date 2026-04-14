#include <gtest/gtest.h>
#include "runtime/task/task.h"
#include "runtime/task/scheduler.h"
#include "runtime/time/timestamp.h"

#include <atomic>
#include <stdexcept>
#include <thread>
#include <chrono>

using namespace runtime::task;
using namespace std::chrono_literals;

// ── Task ──────────────────────────────────────────────────────────────────────

TEST(TaskTest, RunsFunction) {
    int x = 0;
    Task t([&x] { x = 42; });
    t.Run();
    EXPECT_EQ(x, 42);
}

TEST(TaskTest, SkipsWhenCancelled) {
    int x = 0;
    Task t([&x] { x = 1; });
    t.Cancel();
    t.Run();
    EXPECT_EQ(x, 0);
}

TEST(TaskTest, SkipsWhenPastDeadline) {
    int x = 0;
    Task t([&x] { x = 1; });
    t.SetDeadline(runtime::time::AddTime(runtime::time::Timestamp::Now(), -1.0));
    t.Run();
    EXPECT_EQ(x, 0);
}

TEST(TaskTest, RunsBeforeDeadline) {
    int x = 0;
    Task t([&x] { x = 1; });
    t.SetDeadline(runtime::time::AddTime(runtime::time::Timestamp::Now(), 10.0));
    t.Run();
    EXPECT_EQ(x, 1);
}

TEST(TaskTest, NoDeadlineAlwaysRuns) {
    int x = 0;
    Task t([&x] { x = 1; });
    // 默认 Timestamp{} 为 Invalid，不设截止时间
    t.Run();
    EXPECT_EQ(x, 1);
}

// ── Scheduler ─────────────────────────────────────────────────────────────────

TEST(SchedulerTest, ExecutesOnWorkerThread) {
    Scheduler sched(2);
    std::atomic<bool> done{false};
    sched.Submit([&done] { done.store(true); });
    for (int i = 0; i < 50 && !done.load(); ++i)
        std::this_thread::sleep_for(10ms);
    EXPECT_TRUE(done.load());
}

TEST(SchedulerTest, PendingCountDecrementsAfterRun) {
    Scheduler sched(1);
    std::atomic<bool> done{false};
    sched.Submit([&done] {
        std::this_thread::sleep_for(20ms);
        done.store(true);
    });
    for (int i = 0; i < 100 && !done.load(); ++i)
        std::this_thread::sleep_for(10ms);
    EXPECT_EQ(sched.PendingCount(), 0u);
}

TEST(SchedulerTest, ThrowsWhenQueueFull) {
    // 1 个 worker，队列上限 1
    Scheduler sched(1, /*max_queue_size=*/1);
    std::atomic<bool> block{true};

    // 第一个任务堵住 worker
    sched.Submit([&block] {
        while (block.load()) std::this_thread::sleep_for(5ms);
    });
    std::this_thread::sleep_for(20ms);  // 确保 worker 已取走任务，pending-- 后 = 0

    // 第二个任务让 pending = 1
    std::atomic<bool> done2{false};
    sched.Submit([&block, &done2] {
        block.store(false);
        done2.store(true);
    });

    // 此时 pending = 1，再提交应抛出
    EXPECT_THROW(sched.Submit([] {}), std::runtime_error);

    // 解除阻塞，正常退出
    block.store(false);
    for (int i = 0; i < 100 && !done2.load(); ++i)
        std::this_thread::sleep_for(10ms);
}
