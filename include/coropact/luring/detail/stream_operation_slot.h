// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/result.h"

namespace coropact::luring {

class LUringStream;

namespace detail {

enum class StreamOperationDirection : unsigned char {
  kRead,
  kWrite,
};

// The stream has one logical read and one logical write lane. This helper
// owns only the common lane protocol: affinity, lifecycle validation, and
// reservation/release. Awaiters retain their operation-specific SQE,
// CQE, buffer, and result logic.
class StreamOperationSlot {
public:
  [[nodiscard]]
  static Result<void> Validate(LUringStream& stream,
                                     StreamOperationDirection direction) noexcept;

  [[nodiscard]]
  static Result<void> ValidateAvailable(LUringStream& stream,
                                              StreamOperationDirection direction) noexcept;

  [[nodiscard]]
  static Result<void> Reserve(LUringStream& stream, StreamOperationDirection direction,
                                    void* operation) noexcept;

  static void Release(LUringStream& stream, StreamOperationDirection direction,
                      void* operation) noexcept;
};

}  // namespace detail
}  // namespace coropact::luring
