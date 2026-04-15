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

  explicit EventLoopThread(
      const ThreadInitCallback& cb = ThreadInitCallback());

  ~EventLoopThread();

  // Starts the thread and blocks until the EventLoop is ready.
  EventLoop* StartLoop();

private:
  void ThreadFunc();
private:
  EventLoop* loop_;
  bool exiting_;
  std::thread thread_;
  std::mutex mutex_;
  std::condition_variable cond_;
  ThreadInitCallback callback_;
};

}  // namespace runtime::net
