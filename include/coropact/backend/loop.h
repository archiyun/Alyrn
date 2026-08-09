#pragma once

#include <concepts>
#include <cstdint>
#include <stop_token>

namespace coropact::backend {

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

}  // namespace coropact::backend
