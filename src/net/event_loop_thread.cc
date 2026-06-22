// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/net/event_loop_thread.h"

#include <cassert>

#include "vexo/net/event_loop.h"

namespace vexo::net {

EventLoopThread::EventLoopThread() = default;

EventLoopThread::EventLoopThread(ThreadInitCallback cb)
    : init_callback_(std::move(cb)) {}

EventLoop* EventLoopThread::StartLoop() {
  thread_ = std::jthread([this](std::stop_token token) {
    WorkLoop(std::move(token));
  });
  EventLoop* loop = nullptr;
  {
    std::unique_lock lk{mutex_};
    cv_.wait(lk, thread_.get_stop_token(),[this]{
      return loop_ != nullptr;
    });
    loop = loop_;
  }
  return loop;
}

void EventLoopThread::WorkLoop(std::stop_token token) {
  EventLoop loop;

  if (init_callback_) init_callback_(&loop);
  std::stop_callback on_stop{token, [&loop] { loop.Quit(); }};
  {
    std::lock_guard lk{mutex_};
    loop_ = &loop;
    cv_.notify_one();
  }
  loop.Loop();
  {
    std::lock_guard lk{mutex_};
    loop_ = nullptr;
  }
}
}  // namespace vexo::net

