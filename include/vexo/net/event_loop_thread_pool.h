// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <memory>
#include <vector>

#include "vexo/net/event_loop_thread.h"
#include "vexo/utils/macros.h"

namespace vexo::net {

class EventLoop;

// EventLoopThreadPool manages a set of I/O threads, each owning one EventLoop.
class EventLoopThreadPool {
public:
  using ThreadInitCallback = EventLoopThread::ThreadInitCallback;

  EventLoopThreadPool(EventLoop* main_loop, int sub_loop_num);
  ~EventLoopThreadPool();

  VEXO_DELETE_COPY_MOVE(EventLoopThreadPool);

  void Start();
  void Start(ThreadInitCallback cb);

  // Returns the next EventLoop in round-robin order. (RR)
  // Must be called from the main loop thread — the round-robin cursor is
  // unsynchronized.
  EventLoop* GetNextLoop();

  bool started() const { return started_; }
private:
  EventLoop* main_loop_;
  bool started_;
  int sub_loop_num_;
  int next_;

  std::vector<std::unique_ptr<EventLoopThread>> threads_;
  std::vector<EventLoop*> loops_;
};

}  // namespace vexo::net
