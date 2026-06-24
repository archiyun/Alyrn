// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

namespace vexo::base {

// Legacy inheritance helper that disables copying and also prevents derived
// types from being moved implicitly. Retained for source compatibility; new
// types should declare their intended copy and move semantics directly with
// VEXO_DELETE_COPY_MOVE or VEXO_DISABLE_COPY_ALLOW_MOVE from
// vexo/utils/macros.h.
class [[deprecated(
    "Use VEXO_DELETE_COPY_MOVE or VEXO_DISABLE_COPY_ALLOW_MOVE instead")]] NonCopyable {
protected:
  NonCopyable() = default;
  ~NonCopyable() = default;

  NonCopyable(const NonCopyable&) = delete;
  NonCopyable& operator=(const NonCopyable&) = delete;
};

}  // namespace vexo::base
