#include "runtime/net/event_loop_thread.h"
#include "runtime/net/event_loop.h"

#include <cassert>

namespace runtime::net {

EventLoopThread::EventLoopThread(const ThreadInitCallback& cb)
    : init_callback_(cb) {}


EventLoop* EventLoopThread::StartLoop() {
  thread_ = std::jthread([this](std::stop_token token) {
    ThreadFunc(std::move(token));
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

void EventLoopThread::ThreadFunc(std::stop_token token) {
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
}  // namespace runtime::net
