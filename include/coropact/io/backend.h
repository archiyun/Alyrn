// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cerrno>
#include <cstdint>
#include <expected>

#include "coropact/base/error.h"
#include "coropact/io/profile.h"

namespace coropact::io {

enum class Backend : std::uint8_t {
  kReactor,
  kLuring,
};

struct BackendBinding {
  Backend backend;
  CapabilitySet active_profile;
};

[[nodiscard]]
constexpr base::Result<BackendBinding> BindReactor(
    CapabilitySet active_profile = CapabilitySet::Reactor()) noexcept {
  const auto supported = CapabilitySet::Reactor();
  if (!supported.ContainsAll(active_profile)) {
    return std::unexpected(base::MakeErrno(ENOTSUP));
  }
  return BackendBinding{
      .backend = Backend::kReactor,
      .active_profile = active_profile,
  };
}

}  // namespace coropact::io
