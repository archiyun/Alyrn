// SPDX-License-Identifier: MIT
#include "alyrn/luring/detail/worker.h"

#include <pthread.h>
#include <sched.h>

#include <algorithm>
#include <cerrno>
#include <expected>
#include <optional>
#include <stop_token>
#include <thread>
#include <utility>

#include "alyrn/result.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/luring/detail/loop_access.h"
#include "alyrn/luring/listener.h"
#include "alyrn/luring/loop.h"
#include "alyrn/net/accept_source.h"

namespace alyrn::luring::detail {

namespace {

coro::DetachedTask AcceptLoop(WorkerContext& context,
                               Worker::ConnectionCallback* callback) {
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
    WorkerContext& context,
    Worker::ConnectionCallback* callback) {
  auto source_result = context.listener.CreateAcceptSource({
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

coro::DetachedTask CloseListener(Listener* listener,
                                 std::optional<Result<void>>* result) {
  result->emplace(co_await listener->Close());
}

void CloseListenerAfterLoopDrain(Loop& loop, Listener& listener) noexcept {
  std::optional<Result<void>> close_result;
  coro::SpawnDetach(loop, CloseListener(&listener, &close_result));
  LoopAccess::RunReady(loop);
  ALYRN_CHECK(close_result.has_value(),
                 "Loop drain left listener Close coroutine pending");
}

Result<void> SetCurrentThreadAffinity(unsigned cpu) noexcept {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu, &cpuset);
  const int result = pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
  if (result != 0) {
    return std::unexpected(Errno(result));
  }
  return {};
}

}  // namespace

Worker::Worker(std::size_t index, net::Endpoint listen_addr,
                           WorkerOptions options, ThreadInitCallback init_callback,
                           ConnectionCallback connection_callback, ThreadExitCallback exit_callback)
    : index_(index),
      listen_addr_(listen_addr),
      options_(std::move(options)),
      init_callback_(std::move(init_callback)),
      connection_callback_(std::move(connection_callback)),
      exit_callback_(std::move(exit_callback)) {}

Worker::~Worker() noexcept { Stop(); }

Result<void> Worker::Start() {
  if (thread_.joinable()) {
    return std::unexpected(Errno(EALREADY));
  }

  {
    std::lock_guard lock{mutex_};
    init_done_ = false;
    start_result_ = Result<void>{};
  }

  thread_ = std::jthread([this](std::stop_token token) { WorkLoop(std::move(token)); });

  std::unique_lock lock{mutex_};
  cv_.wait(lock, thread_.get_stop_token(), [this] { return init_done_; });

  if (!init_done_) {
    return std::unexpected(Errno(ECANCELED));
  }
  return start_result_;
}

void Worker::Stop() noexcept {
  if (thread_.joinable()) {
    thread_.request_stop();
  }
}

void Worker::WorkLoop(std::stop_token token) noexcept {
  auto PublishStart = [this](Result<void> result) noexcept {
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
  Loop loop(options_.frame_resource);

  auto loop_init = loop.Init(options_.loop_options);
  if (!loop_init.has_value()) {
    PublishStart(std::unexpected(loop_init.error()));
    return;
  }

  auto listener = Listener::Create(&loop, listen_addr_, options_.listen_options);

  if (!listener.has_value()) {
    PublishStart(std::unexpected(listener.error()));
    return;
  }

  auto connector = Connector::Create(&loop);
  if (!connector.has_value()) {
    PublishStart(std::unexpected(connector.error()));
    return;
  }

  WorkerContext context{index_, loop, *listener, *connector};

  if (init_callback_) {
    try {
      init_callback_(context);
    } catch (...) {
      PublishStart(std::unexpected(Errno(EFAULT)));
      return;
    }
  }

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

  PublishStart(Result<void>{});
  loop.Run(token);
  CloseListenerAfterLoopDrain(loop, *listener);

  if (exit_callback_) {
    try {
      exit_callback_(context);
    } catch (...) {
      // Worker exit cleanup must not escape WorkLoop's noexcept boundary.
    }
  }
}

}  // namespace alyrn::luring::detail
