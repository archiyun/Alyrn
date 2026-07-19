// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <expected>
#include <iostream>
#include <system_error>
#include <thread>

#include "vexo/base/error.h"
#include "vexo/coro/work.h"
#include "vexo/io/io_backend.h"
#include "vexo/luring/capabilities.h"
#include "vexo/luring/loop.h"
#include "vexo/luring/op.h"
#include "vexo/luring/options.h"

namespace {

using vexo::base::Error;

bool IsEnvironmentSkip(Error error) {
  return error == std::errc::operation_not_supported ||
         error == std::errc::operation_not_permitted ||
         error == std::errc::permission_denied ||
         error == std::errc::function_not_supported;
}

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

class SignalWork final : public vexo::coro::Work {
public:
  explicit SignalWork(std::atomic_bool* completed) noexcept : completed_(completed) {
    run = &RunWork;
  }

private:
  static void RunWork(vexo::coro::Work* base) noexcept {
    auto* self = static_cast<SignalWork*>(base);
    self->completed_->store(true, std::memory_order_release);
  }

  std::atomic_bool* completed_;
};

bool CheckMailboxNotificationState() {
  vexo::luring::LUringMailbox mailbox;

  const vexo::luring::LUringMessage message{
      .type = vexo::luring::LUringMessage::Type::kResume,
      .data = 1,
  };

  if (!Check(
          mailbox.Push(message) ==
              vexo::luring::LUringMailboxPushResult::kQueuedNeedsNotification,
          "first mailbox message should arm notification")) {
    return false;
  }
  if (!Check(
          mailbox.Push(message) ==
              vexo::luring::LUringMailboxPushResult::kQueued,
          "second mailbox message should coalesce notification")) {
    return false;
  }
  if (!Check(mailbox.RetryNotification(),
             "retry should remain armed while mailbox contains messages")) {
    return false;
  }

  const std::size_t drained =
      mailbox.Drain([](const vexo::luring::LUringMessage&) noexcept {});
  if (!Check(drained == 2, "mailbox drain should consume both messages")) {
    return false;
  }

  return Check(!mailbox.RetryNotification(),
               "retry should disarm an empty mailbox");
}

bool CheckMsgRingMailboxSchedule() {
  vexo::luring::LUringOptions options;
  options.entries = 16;
  options.submit_batch = 1;

  auto capabilities = vexo::luring::ProbeCapabilities(options);
  if (!capabilities.has_value()) {
    if (IsEnvironmentSkip(capabilities.error())) {
      std::cout << "SKIP: io_uring capability probe unavailable: "
                << capabilities.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: capability probe failed: " << capabilities.error().message() << '\n';
    return false;
  }

  if (!capabilities->Has(vexo::io::IoCapability::kMsgRing)) {
    std::cout << "SKIP: kernel does not support IORING_OP_MSG_RING\n";
    return true;
  }

  std::atomic_bool failed{false};
  std::atomic_bool work_completed{false};
  std::atomic<vexo::luring::LUringLoop*> target_ptr{nullptr};
  std::barrier sync_point(3);
  SignalWork work(&work_completed);

  std::jthread target_thread([&] {
    vexo::luring::LUringLoop target;
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

    if (!work_completed.load(std::memory_order_acquire)) {
      failed.store(true, std::memory_order_release);
    }
  });

  std::jthread source_thread([&] {
    vexo::luring::LUringLoop source;
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
        .type = vexo::luring::LUringMessage::Type::kResume,
        .data = static_cast<std::uint64_t>(
            reinterpret_cast<std::uintptr_t>(&work)),
    });

    if (push_result == vexo::luring::LUringMailboxPushResult::kFull) {
      failed.store(true, std::memory_order_release);
      return;
    }

    if (push_result !=
        vexo::luring::LUringMailboxPushResult::kQueuedNeedsNotification) {
      failed.store(true, std::memory_order_release);
      return;
    }

    vexo::luring::LUringOp notify_op{
        .kind = vexo::luring::LUringOpKind::kMsgRing};

    auto submitted = source.SubmitMsgRing(
        &notify_op,
        target->ring_fd(),
        0);
    if (!submitted.has_value()) {
      static_cast<void>(target->RetryMessageNotification());
      failed.store(true, std::memory_order_release);
      return;
    }

    constexpr int kPollLimit = 2000;
    for (int i = 0; i < kPollLimit && !failed.load(std::memory_order_acquire) &&
                                      !notify_op.completed;
         ++i) {
      auto completed = source.PollCompletions();
      if (!completed.has_value()) {
        failed.store(true, std::memory_order_release);
        return;
      }

      if (*completed == 0 && !notify_op.completed) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }

    if (!notify_op.completed) {
      failed.store(true, std::memory_order_release);
    }
  });

  sync_point.arrive_and_wait();

  source_thread.join();
  target_thread.join();

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
