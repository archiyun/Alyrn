#pragma once

#include <cstdint>

namespace runtime::net {

class Timer;

// TimerId identifies a timer instance inside TimerQueue.
struct TimerId {
  Timer* timer{nullptr};
  int64_t sequence{0};
};

}  // namespace runtime::net
