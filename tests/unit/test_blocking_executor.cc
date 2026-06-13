#include <gtest/gtest.h>

#include <atomic>
#include <stdexcept>
#include <thread>
#include <vector>

#include "runtime/task/cancellation_token.h"
#include "runtime/task/blocking_executor.h"
#include "runtime/task/task_handle.h"
#include "runtime/task/detail/work_queue.h"

using namespace runtime::task;
using namespace std::chrono_literals;

// ── CancellationToken ─────────────────────────────────────────────────────────

TEST(CancellationTokenTest, NotCancelledByDefault) {
  CancellationSource src;
  EXPECT_FALSE(src.token().IsCancelled());
}

TEST(CancellationTokenTest, TokenReflectsCancel) {
  CancellationSource src;
  auto token = src.token();
  src.Cancel();
  EXPECT_TRUE(token.IsCancelled());
}

// ── WorkQueue ─────────────────────────────────────────────────────────────────

TEST(WorkQueueTest, FifoOrdering) {
  WorkQueue q;

  auto first_task = std::make_shared<Task>(1, [](CancellationToken) {});
  auto second_task = std::make_shared<Task>(2, [](CancellationToken) {});

  q.Push(std::move(first_task));
  q.Push(std::move(second_task));

  auto first  = q.TryPop();
  auto second = q.TryPop();

  ASSERT_NE(first,  nullptr);
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(first->id, 1u);
  EXPECT_EQ(second->id, 2u);
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

TEST(WorkQueueTest, BoundedQueueRejectsConcurrentOverflow) {
  WorkQueue q(1);
  constexpr int kSubmitters = 16;

  std::atomic<int> ready{0};
  std::atomic<int> accepted{0};
  std::atomic<bool> start{false};
  std::vector<std::jthread> threads;
  threads.reserve(kSubmitters);

  for (int i = 0; i < kSubmitters; ++i) {
    threads.emplace_back([&](std::stop_token) {
      ready.fetch_add(1, std::memory_order_relaxed);
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      auto task = std::make_shared<Task>(100, [](CancellationToken) {});
      if (q.Push(std::move(task))) {
        accepted.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  while (ready.load(std::memory_order_relaxed) < kSubmitters) {
    std::this_thread::yield();
  }
  start.store(true, std::memory_order_release);
  threads.clear();

  EXPECT_EQ(accepted.load(std::memory_order_relaxed), 1);
  EXPECT_EQ(q.size(), 1u);
}

// ── BlockingExecutor — basic execution ────────────────────────────────────────

TEST(BlockingExecutorTest, SubmitExecutesOnWorkerThread) {
  BlockingExecutor sched(2);
  std::atomic<bool> ran{false};

  auto handle = sched.Submit([&ran](CancellationToken) { ran.store(true); });
  handle.Wait();

  EXPECT_TRUE(ran.load());
  EXPECT_EQ(handle.state(), TaskState::kCompleted);
}

TEST(BlockingExecutorTest, VoidLambdaConvenienceOverload) {
  BlockingExecutor sched(1);
  std::atomic<bool> ran{false};

  // void() lambda, no CancellationToken needed.
  auto handle = sched.Submit([&ran] { ran.store(true); });
  handle.Wait();

  EXPECT_TRUE(ran.load());
}

TEST(BlockingExecutorTest, HandleIdIsUnique) {
  BlockingExecutor sched(2);
  auto h1 = sched.Submit([](CancellationToken) {});
  auto h2 = sched.Submit([](CancellationToken) {});
  EXPECT_NE(h1.id(), h2.id());
}

// ── BlockingExecutor — cancellation ───────────────────────────────────────────

TEST(BlockingExecutorTest, CancelPendingTaskSkipsExecution) {
  // One busy worker holds the thread; second task stays kPending.
  BlockingExecutor sched(1);
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
  EXPECT_EQ(pending.state(), TaskState::kCancelled);
}

TEST(BlockingExecutorTest, CooperativeCancelDuringExecution) {
  BlockingExecutor sched(1);
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

  EXPECT_EQ(handle.state(), TaskState::kCancelled);
  EXPECT_LT(progress.load(), 99);
}

// ── BlockingExecutor — exception handling ─────────────────────────────────────

TEST(BlockingExecutorTest, ExceptionPropagatesViaWait) {
  BlockingExecutor sched(1);

  auto handle = sched.Submit([](CancellationToken) {
    throw std::runtime_error("task error");
  });

  EXPECT_THROW(handle.Wait(), std::runtime_error);
  EXPECT_EQ(handle.state(), TaskState::kFailed);
}

// ── BlockingExecutor — metrics ────────────────────────────────────────────────

TEST(BlockingExecutorTest, MetricsSubmittedAndCompleted) {
  BlockingExecutor sched(2);

  auto h1 = sched.Submit([](CancellationToken) {});
  auto h2 = sched.Submit([](CancellationToken) {});
  h1.Wait();
  h2.Wait();

  auto snap = sched.metrics().Load();
  EXPECT_EQ(snap.submitted, 2u);
  EXPECT_EQ(snap.completed, 2u);
  EXPECT_EQ(snap.queue_size, 0);
}

TEST(BlockingExecutorTest, MetricsFailedCountsException) {
  BlockingExecutor sched(1);
  auto h = sched.Submit([](CancellationToken) { throw std::exception{}; });
  try { h.Wait(); } catch (...) {}

  EXPECT_EQ(sched.metrics().failed.load(), 1u);
}

// ── BlockingExecutor — queue capacity ─────────────────────────────────────────

TEST(BlockingExecutorTest, ThrowsWhenQueueFull) {
  BlockingExecutor sched(1, /*max_queue_size=*/1);
  std::atomic<bool> block{true};

  sched.Submit([&block](CancellationToken) {
    while (block.load()) std::this_thread::sleep_for(5ms);
  });

  std::this_thread::sleep_for(20ms);  // worker picks up blocker

  sched.Submit([](CancellationToken) {});    // fills the 1 open slot

  EXPECT_THROW(sched.Submit([](CancellationToken) {}), std::runtime_error);

  block.store(false);
}

TEST(BlockingExecutorTest, TrySubmitReportsQueueFull) {
  BlockingExecutor sched(1, /*max_queue_size=*/1);
  std::atomic<bool> block{true};

  auto blocker = sched.Submit([&block](CancellationToken) {
    while (block.load()) std::this_thread::sleep_for(5ms);
  });

  std::this_thread::sleep_for(20ms);  // worker picks up blocker

  auto queued = sched.TrySubmit([](CancellationToken) {});
  ASSERT_TRUE(queued.has_value());

  SubmitError error = SubmitError::kShuttingDown;
  auto rejected = sched.TrySubmit([](CancellationToken) {}, &error);

  EXPECT_FALSE(rejected.has_value());
  EXPECT_EQ(error, SubmitError::kQueueFull);

  block.store(false);
  blocker.Wait();
  queued->Wait();
}
