// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>

namespace runtime::time {

// A lightweight cancellation handle returned by timer schedulers.
//
// TimerId is just the Timer's monotonic sequence number. Sequences are never
// reused, so the sequence alone defeats ABA: once a Timer expires or is
// cancelled its sequence leaves the scheduler's registry, so a stale handle can
// never match a newer Timer that reuses the same memory address. Carrying no
// raw Timer pointer means a stale handle never even names freed storage.
struct TimerId {
  static constexpr int64_t kInvalid = -1;

  int64_t sequence{kInvalid};

  bool Valid() const { return sequence != kInvalid; }
};

}  // namespace runtime::time
