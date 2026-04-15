#include "runtime/net/event_loop_thread.h"

#include "runtime/net/event_loop.h"

#include <cassert>

namespace runtime::net {

EventLoopThread::EventLoopThread(const ThreadInitCallback& cb)
    : loop_(nullptr),
      exiting_(false),
      callback_(cb) {}

EventLoopThread::~EventLoopThread() {
  exiting_ = true;
  if (loop_ != nullptr) {
    loop_->Quit();
    if (thread_.joinable()) {
      thread_.join();
    }
  }
}

EventLoop* EventLoopThread::StartLoop() {
  thread_ = std::thread(&EventLoopThread::ThreadFunc, this);

  EventLoop* loop = nullptr;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    while (loop_ == nullptr) {
      cond_.wait(lock);
    }
    loop = loop_;
  }
  return loop;
}

void EventLoopThread::ThreadFunc() {
  EventLoop loop;

  if (callback_) {
    callback_(&loop);
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    loop_ = &loop;
    cond_.notify_one();
  }

  loop.Loop();

  {
    std::lock_guard<std::mutex> lock(mutex_);
    loop_ = nullptr;
  }
}
}  // namespace runtime::net
