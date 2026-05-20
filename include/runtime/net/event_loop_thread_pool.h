// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/net/event_loop_thread.h"

#include <memory>
#include <vector>

namespace runtime::net {

class EventLoop;

// EventLoopThreadPool manages a set of I/O threads, each owning one EventLoop.
class EventLoopThreadPool : public runtime::base::NonCopyable {
public:
  using ThreadInitCallback = EventLoopThread::ThreadInitCallback;

  EventLoopThreadPool(EventLoop* main_loop, int sub_loop_num);
  ~EventLoopThreadPool();

  void Start();
  void Start(ThreadInitCallback cb);

  // Returns the next EventLoop in round-robin order. (RR)
  // Must be called from the main loop thread — the round-robin cursor is
  // unsynchronized.
  EventLoop* GetNextLoop();

  bool Started() const { return started_; }
private:
  EventLoop* main_loop_;
  bool started_;
  int sub_loop_num_;
  int next_;

  std::vector<std::unique_ptr<EventLoopThread>> threads_;
  std::vector<EventLoop*> loops_;
};

}  // namespace runtime::net
