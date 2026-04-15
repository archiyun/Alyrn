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

  EventLoopThreadPool(EventLoop* base_loop, int num_threads);

  ~EventLoopThreadPool();

  void Start(const ThreadInitCallback& cb = ThreadInitCallback());

  // Returns the next EventLoop in round-robin order.
  EventLoop* GetNextLoop();

  // Returns all managed EventLoops. If no worker threads exist, returns the
  // base loop as the only element.
  std::vector<EventLoop*> GetAllLoops() const;

  bool Started() const { return started_; }
private:
  EventLoop* base_loop_;
  bool started_;
  int num_threads_;
  int next_;

  std::vector<std::unique_ptr<EventLoopThread>> threads_;
  std::vector<EventLoop*> loops_;
};

}  // namespace runtime::net
