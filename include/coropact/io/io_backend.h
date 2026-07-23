// Copyright (c) 2026 Arsenova
// include/coropact/io/io_backend
#pragma once

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <expected>

#include "coropact/base/error.h"

namespace coropact::io {

enum class Backend : uint8_t {
  kReactor,
  kLuring,
};

enum class IoCapability : uint8_t {
  // A: coro observable semantics
  kReadSome,
  kWriteSome,
  kAccept,
  kConnect,
  kShutdown,
  kClose,
  kCancelByClose,
  kTimeout,

  // B: implementation tags. Not valid in active_profile
  kReadinessPoll,
  kSubmitRead,
  kSubmitWrite,
  kRegisteredBuffer,
  kFixedFile,
  kSqPoll,
  kIoPoll,
  kMsgRing,

  // C: explicit extension semantics.
  kProvidedBuffer,
  kMultishotRecv,
  kMultishotAccept,
  kLinkedOps,
  kSendZeroCopy,

  kCount,
};

enum class CapabilityRole : uint8_t {
  kCore,
  kImplementationTag,
  kExtension,
};

[[nodiscard]] constexpr CapabilityRole RoleOf(IoCapability cap) noexcept {
  switch (cap) {
    case IoCapability::kReadSome:
    case IoCapability::kWriteSome:
    case IoCapability::kAccept:
    case IoCapability::kConnect:
    case IoCapability::kShutdown:
    case IoCapability::kClose:
    case IoCapability::kCancelByClose:
    case IoCapability::kTimeout:
      return CapabilityRole::kCore;

    case IoCapability::kReadinessPoll:
    case IoCapability::kSubmitRead:
    case IoCapability::kSubmitWrite:
    case IoCapability::kRegisteredBuffer:
    case IoCapability::kFixedFile:
    case IoCapability::kSqPoll:
    case IoCapability::kIoPoll:
    case IoCapability::kMsgRing:
      return CapabilityRole::kImplementationTag;

    case IoCapability::kProvidedBuffer:
    case IoCapability::kMultishotRecv:
    case IoCapability::kMultishotAccept:
    case IoCapability::kLinkedOps:
    case IoCapability::kSendZeroCopy:
      return CapabilityRole::kExtension;

    case IoCapability::kCount:
      return CapabilityRole::kImplementationTag;
  }
  return CapabilityRole::kImplementationTag;
}

class CapabilitySet {
public:
  static constexpr std::size_t kSize = static_cast<std::size_t>(IoCapability::kCount);

  constexpr void Enable(IoCapability cap) noexcept { enabled_[Index(cap)] = true; }

  [[nodiscard]] constexpr bool Has(IoCapability cap) const noexcept { return enabled_[Index(cap)]; }

  [[nodiscard]] constexpr bool ContainsAll(CapabilitySet required) const noexcept {
    for (std::size_t i = 0; i < enabled_.size(); ++i) {
      if (required.enabled_[i] && !enabled_[i]) return false;
    }
    return true;
  }

  [[nodiscard]] constexpr bool HasImplementationTags() const noexcept {
    for (std::size_t i = 0; i < enabled_.size(); ++i) {
      if (!enabled_[i]) continue;
      if (RoleOf(static_cast<IoCapability>(i)) == CapabilityRole::kImplementationTag) {
        return true;
      }
    }
    return false;
  }

  static constexpr CapabilitySet CoreStream() noexcept {
    CapabilitySet set;
    set.Enable(IoCapability::kReadSome);
    set.Enable(IoCapability::kWriteSome);
    set.Enable(IoCapability::kShutdown);
    set.Enable(IoCapability::kClose);
    set.Enable(IoCapability::kCancelByClose);
    return set;
  }

  static constexpr CapabilitySet TimedStream() noexcept {
    CapabilitySet set = CoreStream();
    set.Enable(IoCapability::kTimeout);
    return set;
  }

  static constexpr CapabilitySet CoreGateway() noexcept {
    CapabilitySet set = CoreStream();
    set.Enable(IoCapability::kAccept);
    set.Enable(IoCapability::kConnect);
    return set;
  }

  static constexpr CapabilitySet TimedGateway() noexcept {
    CapabilitySet set = CoreGateway();
    set.Enable(IoCapability::kTimeout);
    return set;
  }

  static constexpr CapabilitySet Reactor() noexcept {
    CapabilitySet set = TimedGateway();
    set.Enable(IoCapability::kReadinessPoll);
    return set;
  }

private:
  static constexpr std::size_t Index(IoCapability cap) noexcept {
    return static_cast<std::size_t>(cap);
  }
  std::array<bool, kSize> enabled_{};
};

struct BackendBinding {
  Backend backend{Backend::kReactor};
  CapabilitySet active_profile{};
  CapabilitySet backend_capabilities{};
};

[[nodiscard]] constexpr base::Result<BackendBinding> BindBackend(
    Backend backend, CapabilitySet backend_capabilities,
    CapabilitySet active_profile = CapabilitySet::CoreGateway()) noexcept {
  if (active_profile.HasImplementationTags()) {
    return std::unexpected(base::make_errno(EINVAL));
  }
  if (!backend_capabilities.ContainsAll(active_profile)) {
    return std::unexpected(base::make_errno(ENOTSUP));
  }
  return BackendBinding{
      .backend = backend,
      .active_profile = active_profile,
      .backend_capabilities = backend_capabilities,
  };
}

[[nodiscard]] constexpr base::Result<BackendBinding> BindReactor(
    CapabilitySet active_profile = CapabilitySet::CoreGateway()) noexcept {
  return BindBackend(Backend::kReactor, CapabilitySet::Reactor(), active_profile);
}

}  // namespace coropact::io
