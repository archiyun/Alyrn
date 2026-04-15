#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/time/timestamp.h"

#include <functional>
#include <atomic>
#include <utility>

namespace runtime::net {

// Timer represents one scheduled callback.
class Timer : public runtime::base::NonCopyable {
public:
  using Callback = std::function<void()>;

  Timer(Callback cb, runtime::time::Timestamp when, double interval_sec)
      : callback_(std::move(cb)),
        expiration_(when),
        interval_sec_(interval_sec),
        repeat_(interval_sec > 0.0),
        sequence_(next_sequence_.fetch_add(1)) {}

  void Run() const { callback_(); }
  runtime::time::Timestamp Expiration() const { return expiration_; }
  bool Repeat() const { return repeat_; }
  int64_t Sequence() const { return sequence_; }
  void Restart(runtime::time::Timestamp now) {
    expiration_ = runtime::time::AddTime(now, interval_sec_);
  }
private:
  Callback callback_;
  runtime::time::Timestamp expiration_;
  double interval_sec_;
  bool repeat_;
  int64_t sequence_;
  static std::atomic<int64_t> next_sequence_;
};

}  // namespace runtime::net
