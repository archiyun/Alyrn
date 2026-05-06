#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/time/timestamp.h"
#include "runtime/net/timer_id.h"
#include "runtime/net/timer_rbtree.h"
#include "runtime/net/timer.h"
#include "runtime/memory/object_pool.h"

#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

namespace runtime::net {

class Channel;
class EventLoop;

// TimerQueue manages timerfd-driven timer scheduling for one EventLoop.
//
// TimerQueue owns Timer objects. TimerTree only indexes them by expiration
// time using intrusive red-black tree nodes embedded inside Timer.
class TimerQueue : public runtime::base::NonCopyable {
public:
  using TimerCallback = std::function<void()>;

  explicit TimerQueue(EventLoop* loop);
  ~TimerQueue();

  TimerId AddTimer(TimerCallback cb,
                   runtime::time::Timestamp when,
                   double interval_sec);
  void Cancel(TimerId id);

private:

  void HandleRead();
  void ResetTimerfd(runtime::time::Timestamp expiration);
  std::vector<Timer*> GetExpired(runtime::time::Timestamp now);
  void Reset(const std::vector<Timer*>& expired, runtime::time::Timestamp now);

  void Reclaim(Timer* timer);

  EventLoop* loop_;
  int timerfd_;
  std::unique_ptr<Channel> timerfd_channel_;
  TimerTree timers_;
  runtime::memory::ObjectPool<Timer, 512> timer_pool_;
  std::unordered_map<int64_t, Timer*> active_timers_;
};

}  // namespace runtime::net
