// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <expected>

#include "coropact/base/error.h"

namespace coropact::luring {

// Native io_uring features. These are deliberately not part of the io
// facade: they describe kernel/ring mechanisms, not application semantics.
enum class NativeFeature : std::uint8_t {
  kSubmitRead,
  kSubmitWrite,
  kSqPoll,
  kIoPoll,
  kMsgRing,
  kProvidedBuffer,
  kProvidedBufferRing,
  kProvidedBufferRingIncremental,
  kMultishotRecv,
  kMultishotAccept,
  kLinkedOps,
  kSendZeroCopy,
  kCount,
};

class RuntimeProfile {
public:
  static constexpr std::size_t kSize =
      static_cast<std::size_t>(NativeFeature::kCount);

  [[nodiscard]]
  constexpr bool Has(NativeFeature feature) const noexcept {
    return enabled_[Index(feature)];
  }

  [[nodiscard]]
  constexpr RuntimeProfile Require(NativeFeature feature) const noexcept {
    RuntimeProfile result = *this;
    result.enabled_[Index(feature)] = true;
    if (feature == NativeFeature::kProvidedBufferRingIncremental) {
      result.enabled_[Index(NativeFeature::kProvidedBufferRing)] = true;
    }
    return result;
  }

  [[nodiscard]]
  constexpr bool ContainsAll(const RuntimeProfile& required) const noexcept {
    for (std::size_t i = 0; i < enabled_.size(); ++i) {
      if (required.enabled_[i] && !enabled_[i]) {
        return false;
      }
    }
    return true;
  }

  static constexpr RuntimeProfile Core() noexcept {
    RuntimeProfile profile;
    profile = profile.Require(NativeFeature::kSubmitRead);
    return profile.Require(NativeFeature::kSubmitWrite);
  }

private:
  static constexpr std::size_t Index(NativeFeature feature) noexcept {
    return static_cast<std::size_t>(feature);
  }

  std::array<bool, kSize> enabled_{};
};

namespace detail {
class CapabilityBuilder;
}  // namespace detail

class Capabilities {
public:
  static constexpr std::size_t kSize = RuntimeProfile::kSize;

  [[nodiscard]]
  constexpr bool Has(NativeFeature feature) const noexcept {
    return enabled_[Index(feature)];
  }

  [[nodiscard]]
  constexpr bool ContainsAll(const RuntimeProfile& required) const noexcept {
    for (std::size_t i = 0; i < enabled_.size(); ++i) {
      if (required.Has(static_cast<NativeFeature>(i)) && !enabled_[i]) {
        return false;
      }
    }
    return true;
  }

private:
  friend class detail::CapabilityBuilder;

  constexpr Capabilities() noexcept = default;

  static constexpr std::size_t Index(NativeFeature feature) noexcept {
    return static_cast<std::size_t>(feature);
  }

  constexpr void Enable(NativeFeature feature) noexcept {
    enabled_[Index(feature)] = true;
  }

  std::array<bool, kSize> enabled_{};
};

struct RuntimeBinding {
  RuntimeProfile active_profile;
  Capabilities capabilities;
};

[[nodiscard]]
constexpr base::Result<RuntimeBinding> BindCapabilities(
    Capabilities capabilities, RuntimeProfile active_profile) noexcept {
  if (!capabilities.ContainsAll(active_profile)) {
    return std::unexpected(base::MakeErrno(ENOTSUP));
  }
  return RuntimeBinding{
      .active_profile = active_profile,
      .capabilities = capabilities,
  };
}

}  // namespace coropact::luring
