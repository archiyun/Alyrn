// SPDX-License-Identifier: MIT
#pragma once

#include "alyrn/uring/detail/op.h"

namespace alyrn::uring::detail {

// A typed view over the Op base subobject embedded in an owner.
//
// The io_uring user_data still carries the non-template Op pointer.
// Completion handlers downcast it to the exact tagged hook that was submitted,
// then use the real base relationship to recover the owner. This is the same
// invariant used by the intrusive data structures: the pointer must name the
// matching base subobject, and inheritance must be public and non-virtual.
template <typename TOwner, typename Tag = void>
class OpHook : public Op {
public:
  explicit OpHook(OpKind kind) noexcept { this->kind = kind; }

  Op* Operation() noexcept {
    return this;
  }

  const Op* Operation() const noexcept {
    return this;
  }

  TOwner* Owner() noexcept {
    return static_cast<TOwner*>(this);
  }

  const TOwner* Owner() const noexcept {
    return static_cast<const TOwner*>(this);
  }

  static TOwner* OwnerFrom(Op* op) noexcept {
    return static_cast<OpHook*>(op)->Owner();
  }

  static const TOwner* OwnerFrom(const Op* op) noexcept {
    return static_cast<const OpHook*>(op)->Owner();
  }
};

static_assert(sizeof(OpHook<Op>) == sizeof(Op));

}  // namespace alyrn::uring::detail
