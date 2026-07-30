// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>

namespace coropact::operation::detail {

// Classifies an adapter's observable completion protocol. This does not
// describe a backend opcode or own an I/O resource; family handlers keep that
// backend-specific detail locally.
enum class OperationFamily : std::uint8_t {
  kSingleResult,
  kComposite,
  kEventSource,
  kSplitRelease,
};

}  // namespace coropact::operation::detail
