// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include "vexo/luring/loop.h"

#include <liburing.h>

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <expected>
#include <stop_token>
#include <utility>

#include "vexo/base/ctrack.h"
#include "vexo/base/current_thread.h"
#include "vexo/base/error.h"
#include "vexo/coro/scheduler.h"
#include "vexo/luring/op.h"
#include "vexo/luring/options.h"
#include "vexo/luring/ring.h"

namespace vexo::luring {

namespace {

constexpr std::chrono::milliseconds kStopPollInterval{100};

[[nodiscard]] LUringOp* DecodeOp(io_uring_cqe* cqe) noexcept {
  return reinterpret_cast<LUringOp*>(io_uring_cqe_get_data(cqe));
}

}  // namespace

LUringLoop::LUringLoop(std::pmr::memory_resource* frame_resource)
    : Scheduler(frame_resource), thread_id_(base::tid()), timers_(this) {}

base::Result<void> LUringLoop::Init(const LUringOptions& options) noexcept {
  assert(IsInLoopThread());

  if (initialized_) {
    return std::unexpected(base::make_errno(EALREADY));
  }

  auto ring = LUringRing::Create(options);
  if (!ring.has_value()) {
    return std::unexpected(ring.error());
  }

  ring_ = std::move(*ring);
  submit_batch_ = options.submit_batch == 0 ? 1 : options.submit_batch;
  max_ready_work_per_turn_ = options.max_ready_work_per_turn;
  pending_submit_ = 0;
  inflight_ = 0;
  quit_.store(false, std::memory_order_relaxed);
  initialized_ = true;
  return {};
}

void LUringLoop::Loop(std::stop_token token) noexcept {
  assert(IsInLoopThread());

  if (!initialized_) {
    return;
  }

  while (!token.stop_requested() && !quit_.load(std::memory_order_relaxed)) {
    RunReady();

    if (token.stop_requested() || quit_.load(std::memory_order_relaxed)) {
      break;
    }

    auto completed = PollCompletions();
    if (!completed.has_value()) {
      break;
    }

    if (*completed == 0 && ready_.empty() && inflight_ > 0) {
      completed = WaitCompletionsFor(kStopPollInterval);
      if (!completed.has_value() && completed.error().value() != ETIME) {
        break;
      }
    }
  }
}

void LUringLoop::Quit() noexcept { quit_.store(true, std::memory_order_relaxed); }

void LUringLoop::Schedule(coro::Work* work) noexcept {
  assert(IsInLoopThread());
  ready_.PushBack(work);
}

void LUringLoop::RunReady() noexcept {
  assert(IsInLoopThread());

  coro::Scheduler* previous = coro::Scheduler::Current();
  coro::Scheduler::SetCurrent(this);

  std::size_t resumed = 0;
  while (!ready_.empty() && (max_ready_work_per_turn_ == 0 || resumed < max_ready_work_per_turn_)) {
    coro::Work* work = ready_.PopFront();
    {
      VEXO_CTRACK_SCOPE("luring.ready.work");
      Run(work);
    }
    ++resumed;
  }

  coro::Scheduler::SetCurrent(previous);
}

void LUringLoop::RunUntilIdle() {
  assert(IsInLoopThread());

  if (!initialized_) {
    return;
  }

  while (!ready_.empty() || pending_submit_ > 0 || inflight_ > 0) {
    RunReady();

    if (pending_submit_ == 0 && inflight_ == 0) {
      continue;
    }

    auto completed = WaitCompletions();
    if (!completed.has_value()) {
      break;
    }
  }

  RunReady();
}

base::Result<void> LUringLoop::FlushSubmit() noexcept {
  assert(IsInLoopThread());

  while (pending_submit_ > 0) {
    base::Result<std::size_t> submitted;
    {
      VEXO_CTRACK_SCOPE("luring.ring.submit");
      submitted = ring_.Submit();
    }
    if (!submitted.has_value()) {
      return std::unexpected(submitted.error());
    }
    if (*submitted == 0) {
      return std::unexpected(base::make_errno(EAGAIN));
    }

    const std::size_t n = std::min(*submitted, pending_submit_);
    pending_submit_ -= n;
    inflight_ += n;
  }

  return {};
}

base::Result<std::size_t> LUringLoop::PollCompletions() noexcept {
  assert(IsInLoopThread());

  auto flushed = FlushSubmit();
  if (!flushed.has_value()) {
    return std::unexpected(flushed.error());
  }

  {
    VEXO_CTRACK_SCOPE("luring.ring.reap");
    return ring_.Reap([this](io_uring_cqe* cqe) { HandleCqe(cqe); });
  }
}

base::Result<std::size_t> LUringLoop::WaitCompletions() noexcept {
  return WaitCompletionsFor(std::chrono::nanoseconds::max());
}

base::Result<std::size_t> LUringLoop::WaitCompletionsFor(
    std::chrono::nanoseconds timeout) noexcept {
  assert(IsInLoopThread());

  auto flushed = FlushSubmit();
  if (!flushed.has_value()) {
    return std::unexpected(flushed.error());
  }

  io_uring_cqe* cqe = nullptr;
  int r = 0;
  {
    VEXO_CTRACK_SCOPE("luring.ring.wait");
    if (timeout == std::chrono::nanoseconds::max()) {
      r = io_uring_wait_cqe(ring_.native(), &cqe);
    } else {
      constexpr std::int64_t kNanosecondsPerSecond = 1'000'000'000;
      const std::int64_t count = timeout.count();
      __kernel_timespec timeout_spec{};
      timeout_spec.tv_sec = count / kNanosecondsPerSecond;
      timeout_spec.tv_nsec = count % kNanosecondsPerSecond;
      r = io_uring_wait_cqe_timeout(ring_.native(), &cqe, &timeout_spec);
    }
  }
  if (r < 0) {
    return std::unexpected(base::make_neg_errno(r));
  }

  std::size_t n = 0;
  unsigned head = 0;
  io_uring_for_each_cqe(ring_.native(), head, cqe) {
    HandleCqe(cqe);
    ++n;
  }
  io_uring_cq_advance(ring_.native(), static_cast<unsigned>(n));
  return n;
}

void LUringLoop::HandleCqe(io_uring_cqe* cqe) noexcept {
  assert(IsInLoopThread());

  if (cqe->user_data == kMsgRingNotificationUserData) {
    HandleMailbox();
    return;
  }

  LUringOp* op = DecodeOp(cqe);
  if (op == nullptr) {
    if (inflight_ > 0) {
      --inflight_;
    }
    return;
  }

  assert(inflight_ > 0);
  if (inflight_ > 0) {
    --inflight_;
  }

  {
    VEXO_CTRACK_SCOPE("luring.cqe.complete");
    op->Complete(cqe->res);
  }
  if (op->resume_work.handle) {
    Schedule(&op->resume_work);
  }
}

void LUringLoop::HandleMailbox() noexcept {
  assert(IsInLoopThread());

  DrainMessages([this](const LUringMessage& message) noexcept {
    switch (message.type) {
      case LUringMessage::Type::kResume: {
        auto* work = reinterpret_cast<coro::Work*>(static_cast<std::uintptr_t>(message.data));
        if (work == nullptr) {
          assert(false && "mailbox resume message contains a null work pointer");
          return;
        }
        Schedule(work);
        return;
      }
      case LUringMessage::Type::kFunction:
        assert(false && "mailbox function messages are not implemented");
        return;
    }

    assert(false && "unknown mailbox message type");
  });
}

}  // namespace vexo::luring
