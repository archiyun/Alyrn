// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/luring/capability.h"

namespace coropact::luring::detail {

class CapabilityBuilder final {
public:
  [[nodiscard]]
  static constexpr Capabilities Create() noexcept {
    return Capabilities{};
  }

  static constexpr void Enable(
      Capabilities& capabilities,
      NativeFeature feature) noexcept {
    capabilities.Enable(feature);
  }
};

}  // namespace coropact::luring::detail
