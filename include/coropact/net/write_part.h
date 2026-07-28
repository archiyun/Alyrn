// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <span>

namespace coropact::net {

// One non-owning segment for scatter/gather network writes. The referenced
// bytes must remain alive until the backend reports the write completion.
struct WritePart {
  std::span<const std::byte> bytes;
};

}  // namespace coropact::net
