#include "runtime/net/event_loop_thread_pool.h"
#include "runtime/net/event_loop.h"

#include <cassert>

namespace runtime::net {

class EventLoop;

EventLoopThreadPool::EventLoopThreadPool(EventLoop* main_loop, int num_threads)
    : main_loop_(main_loop),
      started_(false),
      num_threads_(num_threads),
      next_(0) {}

EventLoopThreadPool::~EventLoopThreadPool() = default;

void EventLoopThreadPool::Start(const ThreadInitCallback& cb) {
  assert(!started_);
  started_ = true;

  for (int i = 0; i < num_threads_; ++i) {
    auto thread = std::make_unique<EventLoopThread>(cb);
    EventLoop* loop = thread->StartLoop();
    threads_.push_back(std::move(thread));
    loops_.push_back(loop);
  }

  if (num_threads_ == 0 && cb) {
    cb(main_loop_);
  }
}

EventLoop* EventLoopThreadPool::GetNextLoop() {
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

std::vector<EventLoop*> EventLoopThreadPool::GetAllLoops() const {
  if (loops_.empty()) {
    return std::vector<EventLoop*>(1, main_loop_);
  }
  return loops_;
}
}  // namespace runtime::net
