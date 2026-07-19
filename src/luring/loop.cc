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
#include <thread>
#include <utility>

#include "vexo/base/current_thread.h"
#include "vexo/base/error.h"
#include "vexo/coro/scheduler.h"
#include "vexo/luring/op.h"
#include "vexo/luring/options.h"
#include "vexo/luring/ring.h"

namespace vexo::luring {

namespace {

[[nodiscard]] LUringOp* DecodeOp(io_uring_cqe* cqe) noexcept {
  return reinterpret_cast<LUringOp*>(io_uring_cqe_get_data(cqe));
}

}  // namespace

LUringLoop::LUringLoop(std::pmr::memory_resource* frame_resource)
    : Scheduler(frame_resource), thread_id_(base::tid()) {}

base::Result<void> LUringLoop::Init(const LUringOptions& options) noexcept {
  assert(IsInLoopThread());

  auto ring = LUringRing::Create(options);
  if (!ring.has_value()) {
    return std::unexpected(ring.error());
  }

  ring_ = std::move(*ring);
  submit_batch_ = options.submit_batch == 0 ? 1 : options.submit_batch;
  pending_submit_ = 0;
  inflight_ = 0;
  quit_.store(false, std::memory_order_relaxed);
  return {};
}

void LUringLoop::Loop(std::stop_token token) noexcept {
  assert(IsInLoopThread());

  while (!token.stop_requested() && !quit_.load(std::memory_order_relaxed)) {
    RunReady();

    if (token.stop_requested() || quit_.load(std::memory_order_relaxed)) {
      break;
    }

    auto completed = PollCompletions();
    if (!completed.has_value()) {
      break;
    }

    if (*completed == 0 && ready_.empty()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
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

  while (!ready_.empty()) {
    coro::Work* work = ready_.PopFront();
    Run(work);
  }

  coro::Scheduler::SetCurrent(previous);
}

void LUringLoop::RunUntilIdle() {
  assert(IsInLoopThread());

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
    auto submitted = ring_.Submit();
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

  return ring_.Reap([this](io_uring_cqe* cqe) { HandleCqe(cqe); });
}

base::Result<std::size_t> LUringLoop::WaitCompletions() noexcept {
  assert(IsInLoopThread());

  auto flushed = FlushSubmit();
  if (!flushed.has_value()) {
    return std::unexpected(flushed.error());
  }

  io_uring_cqe* cqe = nullptr;
  int r = io_uring_wait_cqe(ring_.native(), &cqe);
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
    return;
  }

  assert(inflight_ > 0);
  if (inflight_ > 0) {
    --inflight_;
  }

  op->Complete(cqe->res);
  if (op->resume_work.handle) {
    Schedule(&op->resume_work);
  }
}

void LUringLoop::HandleMailbox() noexcept {
  assert(IsInLoopThread());

  DrainMessages([this](const LUringMessage& message) noexcept {
    switch (message.type) {
      case LUringMessage::Type::kResume: {
        auto* work = reinterpret_cast<coro::Work*>(
            static_cast<std::uintptr_t>(message.data));
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
