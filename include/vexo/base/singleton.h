// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "vexo/utils/macros.h"

namespace vexo::base {

// CRTP singleton base. Derived types inherit as Singleton<Derived> and usually
// declare Singleton<Derived> as a friend when their constructors are private.
template <typename T>
class Singleton {
public:
  VEXO_DELETE_COPY_MOVE(Singleton);

  static T& Instance() {
    static T instance;
    return instance;
  }

protected:
  Singleton() = default;
  ~Singleton() = default;
};

}  // namespace vexo::base
