// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cassert>
#include <cerrno>
#include <cstdint>
#include <expected>

#include "coropact/base/error.h"

namespace coropact::net::detail {

// Backend-neutral logical state for a connected stream. It owns no fd and
// performs no syscall; Reactor and io_uring adapters map their physical
// operations onto these transitions.
class StreamLifecycle final {
public:
  StreamLifecycle() noexcept = default;
  StreamLifecycle(const StreamLifecycle&) = delete;
  StreamLifecycle& operator=(const StreamLifecycle&) = delete;

  StreamLifecycle(StreamLifecycle&& other) noexcept
      : resource_(other.resource_), write_(other.write_) {
    other.MarkClosed();
  }

  StreamLifecycle& operator=(StreamLifecycle&& other) noexcept {
    if (this != &other) {
      resource_ = other.resource_;
      write_ = other.write_;
      other.MarkClosed();
    }
    return *this;
  }

  [[nodiscard]]
  base::Result<void> ValidateRead() const noexcept {
    if (resource_ == ResourceState::kClosing) {
      return std::unexpected(base::MakeErrno(ECANCELED));
    }
    if (resource_ == ResourceState::kClosed) {
      return std::unexpected(base::MakeErrno(EBADF));
    }
    return {};
  }

  [[nodiscard]]
  base::Result<void> ValidateWrite() const noexcept {
    auto readable = ValidateRead();
    if (!readable.has_value()) {
      return readable;
    }
    if (write_ == WriteState::kShutdown) {
      return std::unexpected(base::MakeErrno(EPIPE));
    }
    return {};
  }

  // true means the adapter must perform the physical shutdown syscall;
  // false means the write side was already shut down.
  [[nodiscard]]
  base::Result<bool> PrepareShutdown(bool write_pending) const noexcept {
    auto readable = ValidateRead();
    if (!readable.has_value()) {
      return std::unexpected(readable.error());
    }
    if (write_ == WriteState::kShutdown) {
      return false;
    }
    if (write_pending) {
      return std::unexpected(base::MakeErrno(EBUSY));
    }
    return true;
  }

  void CommitShutdown() noexcept {
    assert(resource_ == ResourceState::kOpen);
    assert(write_ == WriteState::kWritable);
    write_ = WriteState::kShutdown;
  }

  // Starts an owner-local close preparation.  It temporarily excludes new
  // stream operations while the adapter either commits physical drain/close
  // or aborts before any cancel request reached the backend.  false means the
  // resource was already closed; a concurrent preparation receives EBUSY.
  [[nodiscard]]
  base::Result<bool> PrepareClose() noexcept {
    if (resource_ == ResourceState::kClosed) {
      return false;
    }
    if (resource_ == ResourceState::kClosing) {
      return std::unexpected(base::MakeErrno(EBUSY));
    }
    resource_ = ResourceState::kClosing;
    return true;
  }

  // This is valid only before a cancellation SQE joins the owner loop's
  // submission protocol. Once backend drain is committed, Close is
  // irreversible.
  void AbortClosePreparation() noexcept {
    assert(resource_ == ResourceState::kClosing);
    resource_ = ResourceState::kOpen;
  }

  void MarkClosed() noexcept { resource_ = ResourceState::kClosed; }

private:
  enum class ResourceState : std::uint8_t {
    kOpen,
    kClosing,
    kClosed,
  };

  enum class WriteState : std::uint8_t {
    kWritable,
    kShutdown,
  };

  ResourceState resource_{ResourceState::kOpen};
  WriteState write_{WriteState::kWritable};
};

static_assert(sizeof(StreamLifecycle) == 2);

}  // namespace coropact::net::detail
