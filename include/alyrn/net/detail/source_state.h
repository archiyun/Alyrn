// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>

namespace alyrn::net::detail {

// Shared completion vocabulary for logical sources backed by either a native
// multishot request or a sequence of single-shot requests.
enum class SourceState : std::uint8_t {
  kIdle,
  kActive,
  kPausing,
  kPaused,
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
  // The event is handed directly to an already-waiting consumer instead of
  // entering the source's bounded queue. It still owns a live buffer lease.
  kDelivered,
};

}  // namespace alyrn::net::detail
