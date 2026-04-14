#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/net/timer_id.h"
#include "runtime/time/timestamp.h"

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace runtime::net {

class Channel;
class Poller;
class TimerQueue;

// EventLoop is the core event dispatcher in the Reactor model.
//
// Each EventLoop is bound to exactly one thread. It owns a Poller for waiting
// on I/O events, dispatches active Channel callbacks, runs queued functors in
// thread order, and manages timer callbacks through TimerQueue.
class EventLoop : public runtime::base::NonCopyable {
public:
  using Functor = std::function<void()>;

  EventLoop();
  ~EventLoop();

  // Starts the event loop and blocks until Quit() is requested.
  void Loop();

  // Requests the loop to exit. The loop stops after the current iteration.
  void Quit();

  runtime::time::Timestamp PollReturnTime() const { return poll_return_time_; }

  // Runs cb immediately if called from the owning loop thread; otherwise,
  // schedules it to run in the loop thread.
  void RunInLoop(Functor cb);

  // Queues cb to run in the loop thread on a later iteration.
  void QueueInLoop(Functor cb);

  void UpdateChannel(Channel* channel);
  void RemoveChannel(Channel* channel);
  bool HasChannel(Channel* channel) const;

  // Returns true if the caller is running in the owning loop thread.
  bool IsInLoopThread() const;

  // Schedules cb to run once at the specified time point.
  TimerId RunAt(runtime::time::Timestamp time, Functor cb);

  // Schedules cb to run once after delay_sec seconds.
  TimerId RunAfter(double delay_sec, Functor cb);

  // Schedules cb to run repeatedly every interval_sec seconds.
  TimerId RunEvery(double interval_sec, Functor cb);

  // Cancels a previously scheduled timer.
  void Cancel(TimerId id);

private:
  // Wakes up the loop when work is queued from another thread.
  void Wakeup();

  // Handles readability on the wakeup fd.
  void HandleRead();

  // Runs all functors queued through QueueInLoop().
  void DoPendingFunctors();

  std::atomic<bool> looping_;
  std::atomic<bool> quit_;
  std::atomic<bool> calling_pending_functors_;

  const std::thread::id thread_id_;
  runtime::time::Timestamp poll_return_time_;

  std::unique_ptr<Poller> poller_;
  std::vector<Channel*> active_channels_;

  // Eventfd or pipe-based wakeup mechanism used to interrupt epoll_wait when
  // another thread queues work into this loop.
  int wakeup_fd_;
  std::unique_ptr<Channel> wakeup_channel_;

  std::mutex mutex_;
  std::vector<Functor> pending_functors_;

  std::unique_ptr<TimerQueue> timer_queue_;
};

}  // namespace runtime::net
