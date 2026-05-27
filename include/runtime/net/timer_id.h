// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>

namespace runtime::net {

class Timer;

// A lightweight cancellation handle returned by TimerQueue.
//
// TimerId does not own the Timer. `sequence` is used together with the raw
// pointer to avoid ABA: an old handle must not cancel a new Timer that reuses
// the same memory address.
struct TimerId {
  Timer* timer{nullptr};
  int64_t sequence{0};

  bool Valid() const {
    return timer != nullptr;
  }
};

}  // namespace runtime::net
