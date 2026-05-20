// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
#pragma once

#include "runtime/base/noncopyable.h"

#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>

namespace runtime::net {

class EventLoop;

// EventLoopThread starts a dedicated thread and creates one EventLoop inside it.
class EventLoopThread : public runtime::base::NonCopyable {
public:
  using ThreadInitCallback = std::function<void(EventLoop*)>;

  EventLoopThread();
  explicit EventLoopThread(ThreadInitCallback cb);
  ~EventLoopThread() = default;

  // Starts the thread and blocks until the EventLoop is ready.
  EventLoop* StartLoop();

private:
  void WorkLoop(std::stop_token token);

  EventLoop* loop_{nullptr};
  std::mutex mutex_;
  std::condition_variable_any cv_;
  ThreadInitCallback init_callback_;
  std::jthread thread_;  
};

}  // namespace runtime::net
