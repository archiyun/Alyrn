#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/net/timer_id.h"
#include "runtime/time/timestamp.h"

#include <functional>
#include <memory>
#include <set>
#include <vector>

namespace runtime::net {

class Channel;
class EventLoop;
class Timer;

// TimerQueue manages timerfd-driven timer scheduling for one EventLoop.
class TimerQueue : public runtime::base::NonCopyable {
public:
  using Callback = std::function<void()>;

  explicit TimerQueue(EventLoop* loop);
  ~TimerQueue();

  TimerId AddTimer(Callback cb,
                   runtime::time::Timestamp when,
                   double interval_sec);
  void Cancel(TimerId id);

private:
  using Entry = std::pair<runtime::time::Timestamp, Timer*>;
  using TimerSet = std::set<Entry>;

  void HandleRead();
  void ResetTimerfd(runtime::time::Timestamp expiration);
  std::vector<Entry> GetExpired(runtime::time::Timestamp now);
  void Reset(const std::vector<Entry>& expired, runtime::time::Timestamp now);

  EventLoop* loop_;
  int timerfd_;
  std::unique_ptr<Channel> timerfd_channel_;
  TimerSet timers_;
};

}  // namespace runtime::net
