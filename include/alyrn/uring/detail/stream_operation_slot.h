// SPDX-License-Identifier: MIT
#pragma once

#include "alyrn/result.h"

namespace alyrn::uring {

class Stream;

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
  static Result<void> Validate(Stream& stream,
                                     StreamOperationDirection direction) noexcept;

  static Result<void> ValidateAvailable(Stream& stream,
                                              StreamOperationDirection direction) noexcept;

  static Result<void> Reserve(Stream& stream, StreamOperationDirection direction,
                                    void* operation) noexcept;

  static void Release(Stream& stream, StreamOperationDirection direction,
                      void* operation) noexcept;
};

}  // namespace detail
}  // namespace alyrn::uring
