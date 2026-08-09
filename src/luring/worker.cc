// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/luring/detail/worker.h"

#include <pthread.h>
#include <sched.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <expected>
#include <optional>
#include <stop_token>
#include <thread>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/coro/spawn.h"
#include "coropact/luring/detail/loop_access.h"
#include "coropact/luring/listener.h"
#include "coropact/luring/loop.h"
#include "coropact/net/accept_source.h"

namespace coropact::luring::detail {

namespace {

coro::DetachedTask AcceptLoop(LUringWorkerContext& context,
                               LUringWorker::ConnectionCallback* callback) {
  while (true) {
    auto accepted = co_await context.listener.Accept();
    if (!accepted.has_value()) {
      const int error = accepted.error().value();
      if (error == ECANCELED || error == EBADF) {
        co_return;
      }
      continue;
    }
    if (*callback) {
      coro::SpawnDetach(context.loop, (*callback)(context, std::move(*accepted)));
    }
  }
}

coro::DetachedTask MultishotAcceptLoop(
    LUringWorkerContext& context,
    LUringWorker::ConnectionCallback* callback) {
  auto source_result = context.listener.AcceptSource({
      .pending_depth = 1,
      .event_capacity = 1024,
  });
  if (!source_result.has_value()) {
    co_return;
  }

  auto source = std::move(*source_result);
  for (;;) {
    auto accepted = co_await source.Next();
    if (!accepted.has_value()) {
      break;
    }
    if (!*accepted) {
      break;
    }

    if (*callback) {
      coro::SpawnDetach(context.loop, (*callback)(context, std::move(**accepted)));
    }
  }

  // Stop() is idempotent and also covers an error or listener-close path. It
  // keeps the source's operation/cancel state converged before its frame is
  // destroyed.
  auto stopped = co_await source.Stop();
  (void)stopped;
}

coro::DetachedTask CloseListener(LUringListener* listener,
                                 std::optional<base::Result<void>>* result) {
  result->emplace(co_await listener->Close());
}

void CloseListenerAndDrain(LUringLoop& loop, LUringListener& listener) noexcept {
  std::optional<base::Result<void>> close_result;
  coro::SpawnDetach(loop, CloseListener(&listener, &close_result));

  for (;;) {
    LoopAccess::RunReady(loop);
    if (close_result.has_value() && LoopAccess::IsDrained(loop)) {
      break;
    }

    if (!close_result.has_value() && LoopAccess::IsDrained(loop)) {
      break;
    }

    if (close_result.has_value()) {
      auto cancelled = LoopAccess::CancelPendingOperations(loop);
      if (!cancelled.has_value()) {
        break;
      }
    }

    if (LoopAccess::PendingSubmitCount(loop) == 0 && LoopAccess::InflightCount(loop) == 0) {
      continue;
    }

    // Shutdown must keep revisiting ready work and cancellation state even if
    // the kernel has not produced a CQE for this turn. A blocking wait can
    // strand the drain loop behind a split-release notification (notably a
    // send-zc notification) while the remaining cancellation work is ready
    // to be observed by the next poll.
    auto completed = LoopAccess::PollCompletions(loop);
    if (!completed.has_value()) {
      break;
    }
    if (*completed == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  LoopAccess::RunReady(loop);
}

base::Result<void> SetCurrentThreadAffinity(unsigned cpu) noexcept {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu, &cpuset);
  const int result = pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
  if (result != 0) {
    return std::unexpected(base::MakeErrno(result));
  }
  return {};
}

}  // namespace

LUringWorker::LUringWorker(std::size_t index, net::Endpoint listen_addr,
                           LUringWorkerOptions options, ThreadInitCallback init_callback,
                           ConnectionCallback connection_callback, ThreadExitCallback exit_callback)
    : index_(index),
      listen_addr_(listen_addr),
      options_(std::move(options)),
      init_callback_(std::move(init_callback)),
      connection_callback_(std::move(connection_callback)),
      exit_callback_(std::move(exit_callback)) {}

LUringWorker::~LUringWorker() noexcept { Stop(); }

base::Result<void> LUringWorker::Start() {
  if (thread_.joinable()) {
    return std::unexpected(base::MakeErrno(EALREADY));
  }

  {
    std::lock_guard lock{mutex_};
    init_done_ = false;
    start_result_ = base::Result<void>{};
  }

  thread_ = std::jthread([this](std::stop_token token) { WorkLoop(std::move(token)); });

  std::unique_lock lock{mutex_};
  cv_.wait(lock, thread_.get_stop_token(), [this] { return init_done_; });

  if (!init_done_) {
    return std::unexpected(base::MakeErrno(ECANCELED));
  }
  return start_result_;
}

void LUringWorker::Stop() noexcept {
  if (thread_.joinable()) {
    thread_.request_stop();
  }
}

void LUringWorker::WorkLoop(std::stop_token token) noexcept {
  auto PublishStart = [this](base::Result<void> result) noexcept {
    {
      std::lock_guard lock{mutex_};
      start_result_ = std::move(result);
      init_done_ = true;
    }
    cv_.notify_one();
  };

  if (options_.cpu_affinity.has_value()) {
    auto affinity = SetCurrentThreadAffinity(*options_.cpu_affinity);
    if (!affinity.has_value()) {
      PublishStart(std::unexpected(affinity.error()));
      return;
    }
  }

  coro::FrameAllocatorScope frame_scope{options_.frame_resource};
  LUringLoop loop(options_.frame_resource);

  auto loop_init = loop.Init(options_.loop_options);
  if (!loop_init.has_value()) {
    PublishStart(std::unexpected(loop_init.error()));
    return;
  }

  auto listener = LUringListener::Create(&loop, listen_addr_, options_.listen_options);

  if (!listener.has_value()) {
    PublishStart(std::unexpected(listener.error()));
    return;
  }

  auto connector = LUringConnector::Create(&loop);
  if (!connector.has_value()) {
    PublishStart(std::unexpected(connector.error()));
    return;
  }

  LUringWorkerContext context{index_, loop, *listener, *connector};

  if (init_callback_) {
    try {
      init_callback_(context);
    } catch (...) {
      PublishStart(std::unexpected(base::MakeErrno(EFAULT)));
      return;
    }
  }

  std::stop_callback on_stop{token, [&loop] { loop.Quit(); }};

  if (connection_callback_) {
    if (options_.accept_mode == AcceptMode::kMultishot) {
      coro::SpawnDetach(loop, MultishotAcceptLoop(context, &connection_callback_));
    } else {
      const std::size_t accept_depth =
          std::max<std::size_t>(1, options_.listen_options.accept_depth);
      for (std::size_t i = 0; i < accept_depth; ++i) {
        coro::SpawnDetach(loop, AcceptLoop(context, &connection_callback_));
      }
    }
  }

  PublishStart(base::Result<void>{});
  loop.Loop(token);
  CloseListenerAndDrain(loop, *listener);

  if (exit_callback_) {
    try {
      exit_callback_(context);
    } catch (...) {
      // Worker exit cleanup must not escape WorkLoop's noexcept boundary.
    }
  }
}

}  // namespace coropact::luring::detail
