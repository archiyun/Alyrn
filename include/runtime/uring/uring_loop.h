// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>

#include "runtime/base/noncopyable.h"
#include "runtime/uring/ring.h"

namespace runtime::uring {

class UringLoop : public runtime::base::NonCopyable {
public:
  explicit UringLoop() {}
  void Loop();
  void Quit();

  Ring& get_ring() { return ring_; }

private:
  Ring ring_;
  std::atomic<bool> quit_{false};
};

}  // namespace runtime::uring
