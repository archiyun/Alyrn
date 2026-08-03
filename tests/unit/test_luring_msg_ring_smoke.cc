// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>

#include "coropact/coro/work.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/op.h"
#include "coropact/luring/options.h"
#include "coropact/utils/macros.h"

namespace {

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

class SignalWork final : public coropact::coro::Work {
public:
  explicit SignalWork(std::atomic_bool* completed) noexcept : completed_(completed) {
    SetRun(&RunWork);
  }

private:
  static void RunWork(coropact::coro::Work* base) noexcept {
    auto* self = static_cast<SignalWork*>(base);
    self->completed_->store(true, std::memory_order_release);
  }

  std::atomic_bool* completed_;
};

bool CheckMailboxNotificationState() {
  coropact::luring::LUringMailbox mailbox;

  const coropact::luring::LUringMessage message{
      .data = 1,
  };

  if (!Check(
          mailbox.Push(message) ==
              coropact::luring::LUringMailboxPushResult::kQueuedNeedsNotification,
          "first mailbox message should arm notification")) {
    return false;
  }
  if (!Check(
          mailbox.Push(message) ==
              coropact::luring::LUringMailboxPushResult::kQueued,
          "second mailbox message should coalesce notification")) {
    return false;
  }
  if (!Check(mailbox.RetryNotification(),
             "retry should remain armed while mailbox contains messages")) {
    return false;
  }

  const std::size_t drained =
      mailbox.Drain([](const coropact::luring::LUringMessage&) noexcept {});
  if (!Check(drained == 2, "mailbox drain should consume both messages")) {
    return false;
  }

  return Check(!mailbox.RetryNotification(),
               "retry should disarm an empty mailbox");
}

bool CheckMsgRingMailboxSchedule() {
  coropact::luring::LUringOptions options;
  options.entries = 16;
  options.submit_batch = 1;

  std::atomic_bool failed{false};
  std::atomic_bool skipped{false};
  std::atomic_bool work_completed{false};
  std::atomic<coropact::luring::LUringLoop*> target_ptr{nullptr};
  std::barrier sync_point(3);
  SignalWork work(&work_completed);

  std::jthread target_thread([&] {
    coropact::luring::LUringLoop target;
    auto init = target.Init(options);
    if (!init.has_value()) {
      failed.store(true, std::memory_order_release);
      sync_point.arrive_and_wait();
      return;
    }

    target_ptr.store(&target, std::memory_order_release);
    sync_point.arrive_and_wait();

    if (failed.load(std::memory_order_acquire)) return;

    constexpr int kPollLimit = 2000;
    for (int i = 0; i < kPollLimit && !failed.load(std::memory_order_acquire) &&
                                      !skipped.load(std::memory_order_acquire) &&
                                      !work_completed.load(std::memory_order_acquire);
         ++i) {
      auto completed = target.PollCompletions();
      if (!completed.has_value()) {
        failed.store(true, std::memory_order_release);
        return;
      }

      target.RunReady();
      if (*completed == 0 && !work_completed.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }

    if (!skipped.load(std::memory_order_acquire) &&
        !work_completed.load(std::memory_order_acquire)) {
      failed.store(true, std::memory_order_release);
    }
  });

  std::jthread source_thread([&] {
    coropact::luring::LUringLoop source;
    auto init = source.Init(options);
    if (!init.has_value()) {
      failed.store(true, std::memory_order_release);
      sync_point.arrive_and_wait();
      return;
    }

    sync_point.arrive_and_wait();

    if (failed.load(std::memory_order_acquire)) return;

    auto* target = target_ptr.load(std::memory_order_acquire);
    if (target == nullptr) {
      failed.store(true, std::memory_order_release);
      return;
    }

    const auto push_result = target->PostMessage({
        .data = static_cast<std::uint64_t>(
            reinterpret_cast<std::uintptr_t>(&work)),
    });

    if (push_result == coropact::luring::LUringMailboxPushResult::kFull) {
      failed.store(true, std::memory_order_release);
      return;
    }

    if (push_result !=
        coropact::luring::LUringMailboxPushResult::kQueuedNeedsNotification) {
      failed.store(true, std::memory_order_release);
      return;
    }

    coropact::luring::LUringOp notify_op{coropact::luring::LUringOpKind::kMsgRing};

    auto submitted = source.SubmitMsgRing(
        &notify_op,
        target->RingFd(),
        0);
    if (!submitted.has_value()) {
      COROPACT_IGNORE_RESULT(target->RetryMessageNotification());
      if (IsEnvironmentSkip(submitted.error())) {
        skipped.store(true, std::memory_order_release);
        return;
      }
      failed.store(true, std::memory_order_release);
      return;
    }

    constexpr int kPollLimit = 2000;
    for (int i = 0; i < kPollLimit && !failed.load(std::memory_order_acquire) &&
                                      !skipped.load(std::memory_order_acquire) &&
                                      !notify_op.IsCompleted();
         ++i) {
      auto completed = source.PollCompletions();
      if (!completed.has_value()) {
        failed.store(true, std::memory_order_release);
        return;
      }

      if (*completed == 0 && !notify_op.IsCompleted()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }

    if (!notify_op.IsCompleted()) {
      failed.store(true, std::memory_order_release);
    }
  });

  sync_point.arrive_and_wait();

  source_thread.join();
  target_thread.join();

  if (skipped.load(std::memory_order_acquire)) {
    std::cout << "SKIP: kernel does not support IORING_OP_MSG_RING\n";
    return true;
  }

  return Check(!failed.load(std::memory_order_acquire),
               "cross-ring message delivery failed") &&
         Check(work_completed.load(std::memory_order_acquire),
               "target loop did not schedule mailbox work");
}

}  // namespace

int main() {
  if (!CheckMailboxNotificationState()) return 1;
  if (!CheckMsgRingMailboxSchedule()) return 1;
  std::cout << "luring msg_ring smoke: PASS\n";
  return 0;
}
