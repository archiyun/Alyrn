// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#pragma once

#include <ranges>
#include <span>
#include <type_traits>

namespace coropact::utils {

template <typename Range>
concept ContiguousSizedRange =
    std::ranges::contiguous_range<Range> && std::ranges::sized_range<Range>;

template <typename Range>
concept WritableContiguousSizedRange =
    ContiguousSizedRange<Range> &&
    !std::is_const_v<std::remove_reference_t<std::ranges::range_reference_t<Range>>>;

template <ContiguousSizedRange Range>
[[nodiscard]]
constexpr auto AsBytes(const Range& range) noexcept {
  return std::as_bytes(std::span{range});
}

template <WritableContiguousSizedRange Range>
[[nodiscard]]
constexpr auto AsWritableBytes(Range& range) noexcept {
  return std::as_writable_bytes(std::span{range});
}

}  // namespace coropact::utils
