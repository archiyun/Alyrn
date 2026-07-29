// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/io/io_backend.h"

namespace coropact::io::detail {

// Internal bridge for concrete backend probes. This header is intentionally
// outside the public io.h facade: applications request CapabilitySet profiles
// and consume immutable BackendCapabilities, while backend implementations
// alone manufacture detected capability values.
class BackendCapabilityBuilder final {
public:
  [[nodiscard]]
  static constexpr BackendCapabilities Create(Backend backend) noexcept {
    return BackendCapabilities(backend);
  }

  static constexpr void Enable(
      BackendCapabilities& capabilities,
      IoCapability capability) noexcept {
    capabilities.Enable(capability);
  }
};

}  // namespace coropact::io::detail
