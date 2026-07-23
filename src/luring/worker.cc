// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/luring/worker.h"

#include <pthread.h>
#include <sched.h>

#include <algorithm>
#include <cerrno>
#include <expected>
#include <optional>
#include <stop_token>
#include <utility>

#include "vexo/base/error.h"
#include "vexo/coro/spawn.h"
#include "vexo/coro/task.h"
#include "vexo/luring/listener.h"
#include "vexo/luring/loop.h"

namespace vexo::luring {

namespace {

coro::Task<void> AcceptLoop(LUringWorkerContext& context,
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
      coro::Spawn(context.loop, (*callback)(context, std::move(*accepted))).Detach();
    }
  }
}

coro::Task<void> CloseListener(LUringListener* listener,
                               std::optional<base::Result<void>>* result) {
  result->emplace(co_await listener->Close());
}

void CloseListenerAndDrain(LUringLoop& loop, LUringListener& listener) noexcept {
  std::optional<base::Result<void>> close_result;
  coro::Spawn(loop, CloseListener(&listener, &close_result)).Detach();

  while (!close_result.has_value()) {
    loop.RunReady();
    if (close_result.has_value()) {
      break;
    }

    if (loop.PendingSubmitCount() == 0 && loop.InflightCount() == 0) {
      break;
    }

    auto completed = loop.WaitCompletions();
    if (!completed.has_value()) {
      break;
    }
  }

  loop.RunReady();
}

base::Result<void> SetCurrentThreadAffinity(unsigned cpu) noexcept {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu, &cpuset);
  const int result = pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
  if (result != 0) {
    return std::unexpected(base::make_errno(result));
  }
  return {};
}

}  // namespace

LUringWorker::LUringWorker(std::size_t index, net::InetAddress listen_addr,
                           LUringWorkerOptions options, ThreadInitCallback init_callback,
                           ConnectionCallback connection_callback)
    : index_(index),
      listen_addr_(listen_addr),
      options_(std::move(options)),
      init_callback_(std::move(init_callback)),
      connection_callback_(std::move(connection_callback)) {}

LUringWorker::~LUringWorker() noexcept { Stop(); }

base::Result<void> LUringWorker::Start() {
  if (thread_.joinable()) {
    return std::unexpected(base::make_errno(EALREADY));
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
    return std::unexpected(base::make_errno(ECANCELED));
  }
  return start_result_;
}

void LUringWorker::Stop() noexcept {
  if (thread_.joinable()) {
    thread_.request_stop();
  }
}

void LUringWorker::WorkLoop(std::stop_token token) noexcept {
  auto publish_start = [this](base::Result<void> result) noexcept {
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
      publish_start(std::unexpected(affinity.error()));
      return;
    }
  }

  coro::FrameAllocatorScope frame_scope{options_.frame_resource};
  LUringLoop loop(options_.frame_resource);

  auto loop_init = loop.Init(options_.loop_options);
  if (!loop_init.has_value()) {
    publish_start(std::unexpected(loop_init.error()));
    return;
  }

  auto listener = LUringListener::Create(&loop, listen_addr_, options_.listen_options);

  if (!listener.has_value()) {
    publish_start(std::unexpected(listener.error()));
    return;
  }

  auto connector = LUringConnector::Create(&loop);
  if (!connector.has_value()) {
    publish_start(std::unexpected(connector.error()));
    return;
  }

  LUringWorkerContext context{index_, loop, *listener, *connector};

  if (init_callback_) {
    try {
      init_callback_(context);
    } catch (...) {
      publish_start(std::unexpected(base::make_errno(EFAULT)));
      return;
    }
  }

  std::stop_callback on_stop{token, [&loop] { loop.Quit(); }};

  if (connection_callback_) {
    const std::size_t accept_depth = std::max<std::size_t>(1, options_.listen_options.accept_depth);
    for (std::size_t i = 0; i < accept_depth; ++i) {
      coro::Spawn(loop, AcceptLoop(context, &connection_callback_)).Detach();
    }
  }

  publish_start(base::Result<void>{});
  loop.Loop(token);
  CloseListenerAndDrain(loop, *listener);
}

}  // namespace vexo::luring
