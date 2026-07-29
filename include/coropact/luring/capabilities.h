// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/base/error.h"
#include "coropact/luring/capability.h"
#include "coropact/luring/options.h"

namespace coropact::luring {

[[nodiscard]]
base::Result<Capabilities> ProbeCapabilities(
    const LUringOptions& options) noexcept;

[[nodiscard]]
base::Result<RuntimeBinding> BindLUring(
    const LUringOptions& options,
    RuntimeProfile active_profile = RuntimeProfile::Core()) noexcept;

}  // namespace coropact::luring
