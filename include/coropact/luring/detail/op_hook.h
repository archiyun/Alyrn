// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/luring/detail/op.h"

namespace coropact::luring::detail {

// A typed view over the LUringOp base subobject embedded in an owner.
//
// The io_uring user_data still carries the non-template LUringOp pointer.
// Completion handlers downcast it to the exact tagged hook that was submitted,
// then use the real base relationship to recover the owner. This is the same
// invariant used by the intrusive data structures: the pointer must name the
// matching base subobject, and inheritance must be public and non-virtual.
template <typename TOwner, typename Tag = void>
class LUringOpHook : public LUringOp {
public:
  explicit LUringOpHook(LUringOpKind kind) noexcept { this->kind = kind; }

  [[nodiscard]]
  LUringOp* Op() noexcept {
    return this;
  }

  [[nodiscard]]
  const LUringOp* Op() const noexcept {
    return this;
  }

  [[nodiscard]]
  TOwner* Owner() noexcept {
    return static_cast<TOwner*>(this);
  }

  [[nodiscard]]
  const TOwner* Owner() const noexcept {
    return static_cast<const TOwner*>(this);
  }

  [[nodiscard]]
  static TOwner* OwnerFrom(LUringOp* op) noexcept {
    return static_cast<LUringOpHook*>(op)->Owner();
  }

  [[nodiscard]]
  static const TOwner* OwnerFrom(const LUringOp* op) noexcept {
    return static_cast<const LUringOpHook*>(op)->Owner();
  }
};

static_assert(sizeof(LUringOpHook<LUringOp>) == sizeof(LUringOp));

}  // namespace coropact::luring::detail
