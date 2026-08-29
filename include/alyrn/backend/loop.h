// SPDX-License-Identifier: MIT
#pragma once

// Shared adapter-contract header for epoll, uring, and kqueue. Include this
// file directly; there is no alyrn/backend.h. Applications use alyrn/io.h.

#include <concepts>
#include <cstdint>
#include <stop_token>

namespace alyrn::backend {

enum class LoopState : std::uint8_t {
  kCreated,
  kRunning,
  kStopping,
  kStopped,
};

template <typename T>
concept ManagedLoop = requires(T& loop, std::stop_token token) {
  { loop.Run(token) } -> std::same_as<void>;
  { loop.RequestStop() } noexcept;
  { loop.State() } noexcept -> std::same_as<LoopState>;
  { loop.IsInLoopThread() } noexcept -> std::convertible_to<bool>;
};

}  // namespace alyrn::backend
