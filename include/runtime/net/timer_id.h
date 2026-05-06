#pragma once

#include <cstdint>

// 中文:
// 
namespace runtime::net {

class Timer;

// TimerId identifies a timer instance inside TimerQueue.
struct TimerId {
  Timer* timer{nullptr};
  int64_t sequence{0};

  bool Valid() const {
    return timer != nullptr;
  }
};

}  // namespace runtime::net
