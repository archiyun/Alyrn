// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/net/reactor_worker.h"

#include <cerrno>
#include <expected>
#include <stop_token>
#include <utility>

#include "coropact/coro/frame_allocator.h"
#include "coropact/coro/spawn.h"

namespace coropact::net {

namespace {

coro::Task<void> AcceptLoop(ReactorWorkerContext& context,
                            ReactorWorker::ConnectionCallback* callback) {
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
      coro::Spawn(context.scheduler, (*callback)(context, std::move(*accepted))).Detach();
    }
  }
}

coro::Task<void> CloseListenerAndQuit(EventLoop& loop, ReactorListener& listener) {
  auto result = co_await listener.Close();
  (void)result;

  // Closing a listener may schedule the pending Accept continuation. Queue the
  // quit request so that continuation gets one more EventLoop turn.
  loop.QueueInLoop([&loop] { loop.Quit(); });
}

}  // namespace

ReactorWorker::ReactorWorker(std::size_t index, InetAddress listen_addr,
                             ReactorWorkerOptions options, ThreadInitCallback init_callback,
                             ConnectionCallback connection_callback)
    : index_(index),
      listen_addr_(listen_addr),
      options_(std::move(options)),
      init_callback_(std::move(init_callback)),
      connection_callback_(std::move(connection_callback)) {}

ReactorWorker::~ReactorWorker() noexcept { Stop(); }

base::Result<void> ReactorWorker::Start() {
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

void ReactorWorker::Stop() noexcept {
  if (thread_.joinable()) {
    thread_.request_stop();
  }
}

void ReactorWorker::WorkLoop(std::stop_token token) noexcept {
  coro::FrameAllocatorScope frame_scope{options_.frame_resource};

  EventLoop loop;

  auto publish_start = [this](base::Result<void> result) noexcept {
    {
      std::lock_guard lock{mutex_};
      start_result_ = result;
      init_done_ = true;
    }
    cv_.notify_one();
  };

  auto scheduler = EventLoopScheduler::Create(&loop, options_.frame_resource);
  if (!scheduler.has_value()) {
    publish_start(std::unexpected(scheduler.error()));
    return;
  }

  auto listener = ReactorListener::Create(&loop, listen_addr_, options_.listener_options);
  if (!listener.has_value()) {
    publish_start(std::unexpected(listener.error()));
    return;
  }

  auto connector = ReactorConnector::Create(&loop);
  if (!connector.has_value()) {
    publish_start(std::unexpected(connector.error()));
    return;
  }

  ReactorWorkerContext context{index_, loop, *scheduler, *listener, *connector};

  if (init_callback_) {
    try {
      init_callback_(context);
    } catch (...) {
      publish_start(std::unexpected(base::make_errno(EFAULT)));
      return;
    }
  }

  std::stop_callback on_stop{
      token, [&loop, &scheduler, &listener] {
        loop.QueueInLoop([&loop, &scheduler, &listener] {
          coro::Spawn(*scheduler, CloseListenerAndQuit(loop, *listener)).Detach();
        });
      }};

  if (connection_callback_) {
    coro::Spawn(*scheduler, AcceptLoop(context, &connection_callback_)).Detach();
  }

  publish_start(base::Result<void>{});
  loop.Loop();
}

}  // namespace coropact::net
