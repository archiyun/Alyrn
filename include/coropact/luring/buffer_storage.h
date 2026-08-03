// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>

namespace coropact::luring {

// Storage used by an io_uring provided-buffer pool. The storage backend does
// not change buffer_id semantics: buffer_id remains the index of a fixed-size
// slot in the contiguous region.
enum class ProvidedBufferStorageKind : std::uint8_t {
  kVector,
  kMmap,
};

}  // namespace coropact::luring
