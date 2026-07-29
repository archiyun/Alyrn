// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/io/profile.h"
#include "coropact/luring/capabilities.h"
#include "coropact/luring/options.h"

namespace coropact::io {

// Translate application semantics into the native profile understood by the
// io_uring backend. This adapter is above luring; luring itself never includes
// this header.
[[nodiscard]]
base::Result<luring::RuntimeBinding> BindLuring(
    luring::LUringOptions& options,
    CapabilitySet requested_profile) noexcept;

}  // namespace coropact::io
