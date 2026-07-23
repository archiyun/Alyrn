// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/base/error.h"
#include "coropact/io/io_backend.h"
#include "coropact/luring/options.h"

namespace coropact::luring {

[[nodiscard]] base::Result<coropact::io::CapabilitySet> ProbeCapabilities(
    const LUringOptions& options) noexcept;

[[nodiscard]] base::Result<coropact::io::BackendBinding> BindLUring(
    const LUringOptions& options,
    coropact::io::CapabilitySet active_profile = coropact::io::CapabilitySet::CoreGateway()) noexcept;

}  // namespace coropact::luring
