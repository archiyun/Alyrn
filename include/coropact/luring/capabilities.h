// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/base/error.h"
#include "coropact/io/io_backend.h"
#include "coropact/luring/options.h"

namespace coropact::luring {

[[nodiscard]]
base::Result<io::CapabilitySet> ProbeCapabilities(const LUringOptions& options) noexcept;

[[nodiscard]]
base::Result<io::BackendBinding> BindLUring(
    const LUringOptions& options,
    io::CapabilitySet active_profile = io::CapabilitySet::CoreGateway()) noexcept;

}  // namespace coropact::luring
