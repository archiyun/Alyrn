// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <span>
#include <string_view>

namespace alyrn::net {

// Views text as the byte span that stream Write() takes, without copying.
// The view borrows the text's storage, which must stay alive until the write
// completes.
[[nodiscard]]
inline std::span<const std::byte> AsBytes(std::string_view text) noexcept {
  return std::as_bytes(std::span<const char>(text.data(), text.size()));
}

}  // namespace alyrn::net
