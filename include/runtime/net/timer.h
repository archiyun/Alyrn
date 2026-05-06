#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/time/timestamp.h"
#include "runtime/net/timer_rbtree.h"

#include <cstdint>
#include <functional>
#include <atomic>
#include <utility>

namespace runtime::net {


// Timer represents one scheduled callback managed by TimerQueue.
//
// It stores the callback, next expiration time, repeat interval, and an
// intrusive red-black tree node used by TimerTree.
class Timer : public runtime::base::NonCopyable {
public:
  using TimerCallback = std::function<void()>;

  Timer(TimerCallback cb, runtime::time::Timestamp when, double interval_sec)
      : timer_callback_(std::move(cb)),
        expiration_(when),
        interval_sec_(interval_sec),
        repeat_(interval_sec > 0.0),
        sequence_(next_sequence_.fetch_add(1, std::memory_order_relaxed)) {}

  void Run() const { timer_callback_(); }
  runtime::time::Timestamp Expiration() const { return expiration_; }
  bool Repeat() const { return repeat_; }
  int64_t Sequence() const { return sequence_; }
  void Restart(runtime::time::Timestamp now) {
    expiration_ = runtime::time::AddTime(now, interval_sec_);
  }
private:
  friend class TimerTree;

  TimerCallback timer_callback_;
  runtime::time::Timestamp expiration_;
  double interval_sec_;
  bool repeat_;
  int64_t sequence_;

  // Intrusive tree node. TimerTree links this node directly without allocating
  // an extra container node like std::set would.
  TimerTreeNode tree_node_;

  inline static std::atomic<int64_t> next_sequence_;
};

}  // namespace runtime::net
