// SPDX-License-Identifier: MIT
#include "alyrn/epoll/detail/worker.h"

#include <cerrno>
#include <expected>
#include <stop_token>
#include <utility>

#include "alyrn/coro/frame_allocator.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/result.h"

namespace alyrn::epoll::detail {

namespace {

coro::DetachedTask AcceptLoop(WorkerContext& context, Worker::ConnectionCallback* callback) {
  while (true) {
    auto accepted = co_await context.listener.Accept();
    if (!accepted.HasValue()) {
      const int error = accepted.Error().value();
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

}  // namespace

Worker::Worker(std::size_t index, net::Endpoint listen_addr, WorkerOptions options,
               ThreadInitCallback init_callback, ConnectionCallback connection_callback,
               ThreadExitCallback exit_callback)
    : index_(index),
      listen_addr_(listen_addr),
      options_(options),
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
  coro::FrameAllocatorScope frame_scope{options_.frame_resource};

  auto publish_start = [this](Result<void> result) noexcept {
    {
      std::lock_guard lock{mutex_};
      start_result_ = result;
      init_done_ = true;
    }
    cv_.notify_one();
  };

  Loop loop{options_.frame_resource};

  auto listener = Listener::Create(&loop, listen_addr_, options_.listener_options);
  if (!listener.HasValue()) {
    publish_start(std::unexpected(listener.Error()));
    return;
  }

  auto connector = Connector::Create(&loop, options_.connector_options);
  if (!connector.HasValue()) {
    publish_start(std::unexpected(connector.Error()));
    return;
  }

  WorkerContext context{index_, loop, *listener, *connector};

  if (init_callback_) {
    try {
      init_callback_(context);
    } catch (...) {
      publish_start(std::unexpected(Errno(EFAULT)));
      return;
    }
  }

  if (connection_callback_) {
    coro::SpawnDetach(loop, AcceptLoop(context, &connection_callback_));
  }

  publish_start(Result<void>{});
  loop.Run(std::move(token));

  if (exit_callback_) {
    try {
      exit_callback_(context);
    } catch (...) {
      // Worker exit cleanup must not escape WorkLoop's noexcept boundary.
    }
  }
}

}  // namespace alyrn::epoll::detail
