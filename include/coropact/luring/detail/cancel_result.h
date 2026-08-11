// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cerrno>

namespace coropact::luring::detail {

// A cancel CQE is terminal for the cancel request itself, never for the
// target request. These outcomes are expected while the target independently
// converges: ENOENT raced a target completion, EALREADY requires waiting for
// the target CQE, and ECANCELED can result from wider loop shutdown.
[[nodiscard]]
constexpr bool IsExpectedCancelCqeResult(int result) noexcept {
  return result >= 0 || result == -ENOENT || result == -EALREADY || result == -ECANCELED;
}

}  // namespace coropact::luring::detail
