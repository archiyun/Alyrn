// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace coropact::io {

// Application-visible I/O requirements. These names describe semantics, not
// the mechanism a concrete backend uses to implement them.
enum class IoRequirement : std::uint8_t {
  kReadSome,
  kWriteSome,
  kAccept,
  kConnect,
  kShutdown,
  kClose,
  kCancelByClose,
  kTimeout,
  kAcceptSource,
  kRecvSource,
  kIncrementalBufferLease,
  kSendZeroCopy,
  kCount,
};

class CapabilitySet {
public:
  static constexpr std::size_t kSize =
      static_cast<std::size_t>(IoRequirement::kCount);

  [[nodiscard]]
  constexpr bool Has(IoRequirement requirement) const noexcept {
    return enabled_[Index(requirement)];
  }

  [[nodiscard]]
  constexpr CapabilitySet Require(IoRequirement requirement) const noexcept {
    CapabilitySet result = *this;
    result.enabled_[Index(requirement)] = true;
    if (requirement == IoRequirement::kIncrementalBufferLease) {
      result.enabled_[Index(IoRequirement::kRecvSource)] = true;
    }
    return result;
  }

  [[nodiscard]]
  constexpr bool ContainsAll(const CapabilitySet& required) const noexcept {
    for (std::size_t i = 0; i < enabled_.size(); ++i) {
      if (required.enabled_[i] && !enabled_[i]) {
        return false;
      }
    }
    return true;
  }

  static constexpr CapabilitySet CoreStream() noexcept {
    CapabilitySet set;
    set = set.Require(IoRequirement::kReadSome);
    set = set.Require(IoRequirement::kWriteSome);
    set = set.Require(IoRequirement::kShutdown);
    set = set.Require(IoRequirement::kClose);
    set = set.Require(IoRequirement::kCancelByClose);
    return set;
  }

  static constexpr CapabilitySet TimedStream() noexcept {
    return CoreStream().Require(IoRequirement::kTimeout);
  }

  static constexpr CapabilitySet CoreNetwork() noexcept {
    return CoreStream()
        .Require(IoRequirement::kAccept)
        .Require(IoRequirement::kConnect);
  }

  static constexpr CapabilitySet TimedNetwork() noexcept {
    return CoreNetwork().Require(IoRequirement::kTimeout);
  }

  static constexpr CapabilitySet Reactor() noexcept {
    return CoreNetwork()
        .Require(IoRequirement::kTimeout)
        .Require(IoRequirement::kAcceptSource)
        .Require(IoRequirement::kRecvSource);
  }

private:
  static constexpr std::size_t Index(IoRequirement requirement) noexcept {
    return static_cast<std::size_t>(requirement);
  }

  std::array<bool, kSize> enabled_{};
};

}  // namespace coropact::io
