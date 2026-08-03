// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/luring/detail/provided_buffer_storage.h"

namespace {

using coropact::luring::detail::ProvidedBufferStorage;

void CheckStorage() {
  constexpr std::size_t kCapacity = 4;
  constexpr std::size_t kBufferSize = 4096;

  auto result = ProvidedBufferStorage::Create(kCapacity, kBufferSize);
  assert(result.has_value());

  auto storage = std::move(*result);
  assert(storage.capacity() == kCapacity);
  assert(storage.buffer_size() == kBufferSize);
  assert(storage.size_bytes() == kCapacity * kBufferSize);

  for (std::size_t i = 0; i < kCapacity; ++i) {
    auto* slot = storage.slot(i);
    assert(slot == storage.data() + i * kBufferSize);
    slot[0] = static_cast<std::byte>(i + 1);
    slot[kBufferSize - 1] = static_cast<std::byte>(0xa0 + i);
  }

  auto moved = std::move(storage);
  assert(moved.capacity() == kCapacity);
  assert(moved.slot(0)[0] == static_cast<std::byte>(1));
  assert(moved.slot(kCapacity - 1)[kBufferSize - 1] ==
         static_cast<std::byte>(0xa3));

  ProvidedBufferStorage assigned;
  assigned = std::move(moved);
  assert(assigned.capacity() == kCapacity);
  assert(assigned.slot(2)[0] == static_cast<std::byte>(3));
}

void CheckInvalidArguments() {
  auto zero_capacity = ProvidedBufferStorage::Create(
      0, 4096);
  assert(!zero_capacity.has_value());
  assert(zero_capacity.error() == std::errc::invalid_argument);

  auto zero_size = ProvidedBufferStorage::Create(4, 0);
  assert(!zero_size.has_value());
  assert(zero_size.error() == std::errc::invalid_argument);

  auto overflow = ProvidedBufferStorage::Create(
      std::numeric_limits<std::size_t>::max(), 2);
  assert(!overflow.has_value());
  assert(overflow.error() == std::errc::value_too_large);
}

}  // namespace

int main() {
  CheckStorage();
  CheckInvalidArguments();
  return 0;
}
