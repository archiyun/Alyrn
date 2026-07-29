// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>

namespace coropact::net::detail {

// Shared completion vocabulary for logical sources backed by either a native
// multishot request or a sequence of single-shot requests.
enum class SourceState : std::uint8_t {
  kIdle,
  kActive,
  kStopping,
  kDraining,
  kTerminal,
};

enum class MultishotRequestDisposition : std::uint8_t {
  kMore,
  kTerminal,
};

enum class EventDisposition : std::uint8_t {
  kNone,
  kProduced,
};

}  // namespace coropact::net::detail
