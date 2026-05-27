// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "runtime/net/event_loop_thread_pool.h"

#include "runtime/net/event_loop.h"
#include "runtime/net/net_assert.h"

#include <cassert>

namespace runtime::net {

class EventLoop;

EventLoopThreadPool::EventLoopThreadPool(EventLoop* main_loop, int sub_loop_num)
    : main_loop_(main_loop),
      started_(false),
      sub_loop_num_(sub_loop_num),
      next_(0) {}

EventLoopThreadPool::~EventLoopThreadPool() = default;

void EventLoopThreadPool::Start() {
  assert(!started_);
  started_ = true;

  for (int i = 0; i < sub_loop_num_; ++i) {
    auto thread = std::make_unique<EventLoopThread>();
    EventLoop* loop = thread->StartLoop();
    threads_.push_back(std::move(thread));
    loops_.push_back(loop);
  }
}

void EventLoopThreadPool::Start(ThreadInitCallback cb) {
  assert(!started_);
  started_ = true;

  for (int i = 0; i < sub_loop_num_; ++i) {
    auto thread = std::make_unique<EventLoopThread>(cb);
    EventLoop* loop = thread->StartLoop();
    threads_.push_back(std::move(thread));
    loops_.push_back(loop);
  }

  // Sub-loop-less mode: still invoke the init hook on the main loop, so
  // callers can rely on it running exactly once per Start().
  if (sub_loop_num_ == 0) {
    cb(main_loop_);
  }
}

EventLoop* EventLoopThreadPool::GetNextLoop() {
  // next_ is a plain int with no synchronization; the round-robin counter is
  // only safe to advance from the main loop thread (typically Acceptor's
  // NewConnection callback). Off-thread callers would race.
  RUNTIME_ASSERT(main_loop_->IsInLoopThread(),
                 "GetNextLoop() must be called from the main loop thread");
  assert(started_);

  EventLoop* loop = main_loop_;
  if (!loops_.empty()) {
    loop = loops_[next_++];
    if (next_ >= static_cast<int>(loops_.size())) {
      next_ = 0;
    }
  }
  return loop;
}

}  // namespace runtime::net
