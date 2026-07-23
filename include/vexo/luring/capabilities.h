// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "vexo/base/error.h"
#include "vexo/io/io_backend.h"
#include "vexo/luring/options.h"

namespace vexo::luring {

[[nodiscard]] base::Result<vexo::io::CapabilitySet> ProbeCapabilities(
    const LUringOptions& options) noexcept;

[[nodiscard]] base::Result<vexo::io::BackendBinding> BindLUring(
    const LUringOptions& options,
    vexo::io::CapabilitySet active_profile = vexo::io::CapabilitySet::CoreGateway()) noexcept;

}  // namespace vexo::luring
