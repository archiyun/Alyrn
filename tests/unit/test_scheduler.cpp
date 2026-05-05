#include <gtest/gtest.h>
#include "runtime/task/cancellation_token.h"
#include "runtime/task/scheduler.h"
#include "runtime/task/task_handle.h"
#include "runtime/task/task_options.h"
#include "runtime/task/work_queue.h"

#include <atomic>
#include <stdexcept>
#include <thread>

using namespace runtime::task;
using namespace std::chrono_literals;

// ── CancellationToken ─────────────────────────────────────────────────────────

TEST(CancellationTokenTest, NotCancelledByDefault) {
  CancellationSource src;
  EXPECT_FALSE(src.Token().IsCancelled());
}

TEST(CancellationTokenTest, TokenReflectsCancel) {
  CancellationSource src;
  auto token = src.Token();
  src.Cancel();
  EXPECT_TRUE(token.IsCancelled());
}

// ── WorkQueue ─────────────────────────────────────────────────────────────────

TEST(WorkQueueTest, PriorityOrderingHighBeforeLow) {
  WorkQueue q;

  auto low  = std::make_shared<Task>(1, "low",  TaskPriority::kLow,
                                     [](CancellationToken) {});
  auto high = std::make_shared<Task>(2, "high", TaskPriority::kHigh,
                                     [](CancellationToken) {});

  q.Push(std::move(low));
  q.Push(std::move(high));

  // TryPop is non-blocking; high-priority task must come out first.
  auto first  = q.TryPop();
  auto second = q.TryPop();

  ASSERT_NE(first,  nullptr);
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(first->priority,  TaskPriority::kHigh);
  EXPECT_EQ(second->priority, TaskPriority::kLow);
}

TEST(WorkQueueTest, ShutdownUnblocksWaiters) {
  WorkQueue q;
  std::atomic<bool> exited{false};

  std::jthread waiter([&q, &exited](std::stop_token st) {
    q.Wait(st);
    exited.store(true);
  });

  std::this_thread::sleep_for(20ms);
  q.Shutdown();

  for (int i = 0; i < 50 && !exited.load(); ++i)
    std::this_thread::sleep_for(10ms);

  EXPECT_TRUE(exited.load());
}

// ── Scheduler — basic execution ───────────────────────────────────────────────

TEST(SchedulerTest, SubmitExecutesOnWorkerThread) {
  Scheduler sched(2);
  std::atomic<bool> ran{false};

  auto handle = sched.Submit([&ran](CancellationToken) { ran.store(true); });
  handle.Wait();

  EXPECT_TRUE(ran.load());
  EXPECT_EQ(handle.State(), TaskState::kCompleted);
}

TEST(SchedulerTest, VoidLambdaConvenienceOverload) {
  Scheduler sched(1);
  std::atomic<bool> ran{false};

  // void() lambda, no CancellationToken needed.
  auto handle = sched.Submit([&ran] { ran.store(true); });
  handle.Wait();

  EXPECT_TRUE(ran.load());
}

TEST(SchedulerTest, HandleIdIsUnique) {
  Scheduler sched(2);
  auto h1 = sched.Submit([](CancellationToken) {});
  auto h2 = sched.Submit([](CancellationToken) {});
  EXPECT_NE(h1.Id(), h2.Id());
}

// ── Scheduler — cancellation ──────────────────────────────────────────────────

TEST(SchedulerTest, CancelPendingTaskSkipsExecution) {
  // One busy worker holds the thread; second task stays kPending.
  Scheduler sched(1);
  std::atomic<bool> block{true};

  auto blocker = sched.Submit([&block](CancellationToken) {
    while (block.load()) std::this_thread::sleep_for(5ms);
  });

  std::this_thread::sleep_for(20ms);  // let worker pick up the first task

  std::atomic<bool> second_ran{false};
  auto pending = sched.Submit([&second_ran](CancellationToken) {
    second_ran.store(true);
  });

  pending.Cancel();
  block.store(false);

  blocker.Wait();
  pending.Wait();

  EXPECT_FALSE(second_ran.load());
  EXPECT_EQ(pending.State(), TaskState::kCancelled);
}

TEST(SchedulerTest, CooperativeCancelDuringExecution) {
  Scheduler sched(1);
  std::atomic<int> progress{0};

  auto handle = sched.Submit([&progress](CancellationToken token) {
    for (int i = 0; i < 100; ++i) {
      if (token.IsCancelled()) return;
      progress.store(i);
      std::this_thread::sleep_for(5ms);
    }
  });

  std::this_thread::sleep_for(20ms);
  handle.Cancel();
  handle.Wait();

  EXPECT_EQ(handle.State(), TaskState::kCancelled);
  EXPECT_LT(progress.load(), 99);
}

// ── Scheduler — exception handling ───────────────────────────────────────────

TEST(SchedulerTest, ExceptionPropagatesViaWait) {
  Scheduler sched(1);

  auto handle = sched.Submit([](CancellationToken) {
    throw std::runtime_error("task error");
  });

  EXPECT_THROW(handle.Wait(), std::runtime_error);
  EXPECT_EQ(handle.State(), TaskState::kFailed);
}

// ── Scheduler — metrics ───────────────────────────────────────────────────────

TEST(SchedulerTest, MetricsSubmittedAndCompleted) {
  Scheduler sched(2);

  auto h1 = sched.Submit([](CancellationToken) {});
  auto h2 = sched.Submit([](CancellationToken) {});
  h1.Wait();
  h2.Wait();

  auto snap = sched.Metrics().Load();
  EXPECT_EQ(snap.submitted, 2u);
  EXPECT_EQ(snap.completed, 2u);
  EXPECT_EQ(snap.queue_size, 0);
}

TEST(SchedulerTest, MetricsFailedCountsException) {
  Scheduler sched(1);
  auto h = sched.Submit([](CancellationToken) { throw std::exception{}; });
  try { h.Wait(); } catch (...) {}

  EXPECT_EQ(sched.Metrics().failed.load(), 1u);
}

// ── Scheduler — queue capacity ────────────────────────────────────────────────

TEST(SchedulerTest, ThrowsWhenQueueFull) {
  Scheduler sched(1, /*max_queue_size=*/1);
  std::atomic<bool> block{true};

  sched.Submit([&block](CancellationToken) {
    while (block.load()) std::this_thread::sleep_for(5ms);
  });

  std::this_thread::sleep_for(20ms);  // worker picks up blocker

  sched.Submit([](CancellationToken) {});    // fills the 1 open slot

  EXPECT_THROW(sched.Submit([](CancellationToken) {}), std::runtime_error);

  block.store(false);
}

// ── TaskOptions — name and priority stored on task ───────────────────────────

TEST(SchedulerTest, TaskOptionsApplied) {
  Scheduler sched(1);

  TaskOptions opts;
  opts.name     = "my_task";
  opts.priority = TaskPriority::kHigh;

  auto handle = sched.Submit([](CancellationToken) {}, std::move(opts));
  handle.Wait();

  EXPECT_EQ(handle.State(), TaskState::kCompleted);
}

// ── Scheduler — soft timeout (Phase 3) ───────────────────────────────────────

// A task that ignores cancellation but runs longer than its timeout_ms should
// be marked kTimeout, not kCancelled.
TEST(SchedulerTest, SoftTimeoutCancelsSlowTask) {
  Scheduler sched(2);

  TaskOptions opts;
  opts.timeout_ms = 50;  // 50 ms deadline

  auto handle = sched.Submit([](CancellationToken token) {
    // Poll the token; exit once cancelled.
    for (int i = 0; i < 2000; ++i) {
      if (token.IsCancelled()) return;
      std::this_thread::sleep_for(1ms);
    }
  }, std::move(opts));

  handle.Wait();

  EXPECT_EQ(handle.State(), TaskState::kTimeout);
  EXPECT_EQ(sched.Metrics().timeout.load(), 1u);
  EXPECT_EQ(sched.Metrics().cancelled.load(), 0u);
}

// A task that finishes before its timeout fires should still get kCompleted.
TEST(SchedulerTest, TimeoutDoesNotAffectFastTask) {
  Scheduler sched(1);

  TaskOptions opts;
  opts.timeout_ms = 500;  // 500 ms — task finishes long before this

  auto handle = sched.Submit([](CancellationToken) {
    // Nothing — returns immediately.
  }, std::move(opts));

  handle.Wait();

  EXPECT_EQ(handle.State(), TaskState::kCompleted);
  EXPECT_EQ(sched.Metrics().timeout.load(), 0u);
}

// An explicit Cancel() by the caller must produce kCancelled, not kTimeout.
TEST(SchedulerTest, ExplicitCancelDistinctFromTimeout) {
  Scheduler sched(1);

  TaskOptions opts;
  opts.timeout_ms = 500;  // long timeout — should NOT fire

  std::atomic<bool> started{false};
  auto handle = sched.Submit([&started](CancellationToken token) {
    started.store(true);
    while (!token.IsCancelled()) std::this_thread::sleep_for(1ms);
  }, std::move(opts));

  // Wait until the task is actually running before we cancel it.
  while (!started.load()) std::this_thread::sleep_for(1ms);
  handle.Cancel();
  handle.Wait();

  EXPECT_EQ(handle.State(), TaskState::kCancelled);
  EXPECT_EQ(sched.Metrics().cancelled.load(), 1u);
  EXPECT_EQ(sched.Metrics().timeout.load(), 0u);
}
