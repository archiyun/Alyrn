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
  // Legacy IORING_OP_PROVIDE_BUFFERS/REMOVE_BUFFERS.
  kProvidedBuffer,
  // IORING_REGISTER_PBUF_RING-backed provided buffers.
  kProvidedBufferRing,
  // IOU_PBUF_RING_INC incremental/partial buffer consumption.
  kProvidedBufferRingIncremental,
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

[[nodiscard]]
constexpr CapabilityRole RoleOf(IoCapability cap) noexcept {
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
    case IoCapability::kProvidedBufferRing:
    case IoCapability::kProvidedBufferRingIncremental:
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

  [[nodiscard]]
  constexpr bool Has(IoCapability cap) const noexcept { return enabled_[Index(cap)]; }

  // Return a new requested profile with one semantic capability added. A
  // profile is immutable after construction; backend probing uses the
  // separate BackendCapabilities type below.
  [[nodiscard]]
  constexpr CapabilitySet Require(IoCapability cap) const noexcept {
    CapabilitySet result = *this;
    result.enabled_[Index(cap)] = true;
    return result;
  }

  [[nodiscard]]
  constexpr bool ContainsAll(CapabilitySet required) const noexcept {
    for (std::size_t i = 0; i < enabled_.size(); ++i) {
      if (required.enabled_[i] && !enabled_[i]) return false;
    }
    return true;
  }

  [[nodiscard]]
  constexpr bool HasImplementationTags() const noexcept {
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
    set = set.Require(IoCapability::kReadSome);
    set = set.Require(IoCapability::kWriteSome);
    set = set.Require(IoCapability::kShutdown);
    set = set.Require(IoCapability::kClose);
    set = set.Require(IoCapability::kCancelByClose);
    return set;
  }

  static constexpr CapabilitySet TimedStream() noexcept {
    CapabilitySet set = CoreStream();
    return set.Require(IoCapability::kTimeout);
  }

  static constexpr CapabilitySet CoreGateway() noexcept {
    CapabilitySet set = CoreStream();
    set = set.Require(IoCapability::kAccept);
    return set.Require(IoCapability::kConnect);
  }

  static constexpr CapabilitySet TimedGateway() noexcept {
    CapabilitySet set = CoreGateway();
    return set.Require(IoCapability::kTimeout);
  }

private:
  static constexpr std::size_t Index(IoCapability cap) noexcept {
    return static_cast<std::size_t>(cap);
  }
  std::array<bool, kSize> enabled_{};
};

namespace detail {
class BackendCapabilityBuilder;
}  // namespace detail

// Capabilities discovered from a concrete backend/ring. The constructor and
// mutator are intentionally unavailable to normal callers: a caller can
// request a profile, but only a backend probe may manufacture the advertised
// capability set.
class BackendCapabilities {
public:
  static constexpr std::size_t kSize = CapabilitySet::kSize;

  [[nodiscard]]
  constexpr Backend BackendKind() const noexcept { return backend_; }

  [[nodiscard]]
  constexpr bool Has(IoCapability cap) const noexcept {
    return enabled_[Index(cap)];
  }

  [[nodiscard]]
  constexpr bool ContainsAll(const CapabilitySet& required) const noexcept {
    for (std::size_t i = 0; i < enabled_.size(); ++i) {
      if (required.Has(static_cast<IoCapability>(i)) && !enabled_[i]) {
        return false;
      }
    }
    return true;
  }

  // Reactor is a compiled-in backend, so its static implementation profile is
  // safe to construct here. io_uring capabilities are always produced by its
  // runtime probe through BackendCapabilityBuilder.
  static constexpr BackendCapabilities Reactor() noexcept {
    BackendCapabilities result(Backend::kReactor);
    result.Enable(IoCapability::kReadSome);
    result.Enable(IoCapability::kWriteSome);
    result.Enable(IoCapability::kAccept);
    result.Enable(IoCapability::kConnect);
    result.Enable(IoCapability::kShutdown);
    result.Enable(IoCapability::kClose);
    result.Enable(IoCapability::kCancelByClose);
    result.Enable(IoCapability::kTimeout);
    result.Enable(IoCapability::kReadinessPoll);
    result.Enable(IoCapability::kSubmitRead);
    result.Enable(IoCapability::kSubmitWrite);
    return result;
  }

private:
  friend class detail::BackendCapabilityBuilder;

  explicit constexpr BackendCapabilities(Backend backend) noexcept
      : backend_(backend) {}

  static constexpr std::size_t Index(IoCapability cap) noexcept {
    return static_cast<std::size_t>(cap);
  }

  constexpr void Enable(IoCapability cap) noexcept { enabled_[Index(cap)] = true; }

  Backend backend_{Backend::kReactor};
  std::array<bool, kSize> enabled_{};
};

struct BackendBinding {
  Backend backend{Backend::kReactor};
  CapabilitySet active_profile{};
  BackendCapabilities backend_capabilities;
};

[[nodiscard]]
constexpr base::Result<BackendBinding> BindBackend(
    Backend backend, const BackendCapabilities& backend_capabilities,
    CapabilitySet active_profile = CapabilitySet::CoreGateway()) noexcept {
  if (active_profile.HasImplementationTags()) {
    return std::unexpected(base::MakeErrno(EINVAL));
  }
  if (backend_capabilities.BackendKind() != backend) {
    return std::unexpected(base::MakeErrno(EINVAL));
  }
  if (!backend_capabilities.ContainsAll(active_profile)) {
    return std::unexpected(base::MakeErrno(ENOTSUP));
  }
  return BackendBinding{
      .backend = backend,
      .active_profile = active_profile,
      .backend_capabilities = backend_capabilities,
  };
}

[[nodiscard]]
constexpr base::Result<BackendBinding> BindReactor(
    CapabilitySet active_profile = CapabilitySet::CoreGateway()) noexcept {
  return BindBackend(Backend::kReactor, BackendCapabilities::Reactor(), active_profile);
}

}  // namespace coropact::io
