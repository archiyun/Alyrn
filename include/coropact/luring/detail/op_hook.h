// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/luring/op.h"

namespace coropact::luring::detail {

// A typed view over the LUringOp base subobject embedded in an owner.
//
// The io_uring user_data still carries the non-template LUringOp pointer.
// Completion handlers downcast it to the exact tagged hook that was submitted,
// then use the real base relationship to recover the owner. This is the same
// invariant used by the intrusive data structures: the pointer must name the
// matching base subobject, and inheritance must be public and non-virtual.
template <typename Owner, typename Tag = void>
class LUringOpHook : public LUringOp {
public:
  explicit LUringOpHook(LUringOpKind kind) noexcept { this->kind = kind; }

  [[nodiscard]]
  Owner* owner() noexcept {
    return static_cast<Owner*>(this);
  }

  [[nodiscard]]
  const Owner* owner() const noexcept {
    return static_cast<const Owner*>(this);
  }
};

static_assert(sizeof(LUringOpHook<LUringOp>) == sizeof(LUringOp));

}  // namespace coropact::luring::detail
