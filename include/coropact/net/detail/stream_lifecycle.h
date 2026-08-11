// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cerrno>
#include <cstdint>
#include <expected>

#include "coropact/base/check.h"
#include "coropact/result.h"

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
  Result<void> ValidateRead() const noexcept {
    if (resource_ == ResourceState::kClosing) {
      return std::unexpected(Errno(ECANCELED));
    }
    if (resource_ == ResourceState::kClosed) {
      return std::unexpected(Errno(EBADF));
    }
    return {};
  }

  [[nodiscard]]
  Result<void> ValidateWrite() const noexcept {
    auto readable = ValidateRead();
    if (!readable.has_value()) {
      return readable;
    }
    if (write_ == WriteState::kShutdown) {
      return std::unexpected(Errno(EPIPE));
    }
    if (write_ == WriteState::kShutdownPreparing) {
      return std::unexpected(Errno(EBUSY));
    }
    return {};
  }

  // Begins a synchronous physical shutdown transition. true means the adapter
  // now owns a required shutdown syscall and must either CommitShutdown() on
  // success or AbortShutdownPreparation() before reporting its local error.
  // false means the write side was already shut down.
  [[nodiscard]]
  Result<bool> PrepareShutdown(bool write_pending) noexcept {
    auto readable = ValidateRead();
    if (!readable.has_value()) {
      return std::unexpected(readable.error());
    }
    if (write_ == WriteState::kShutdown) {
      return false;
    }
    if (write_ == WriteState::kShutdownPreparing) {
      return std::unexpected(Errno(EBUSY));
    }
    if (write_pending) {
      return std::unexpected(Errno(EBUSY));
    }
    write_ = WriteState::kShutdownPreparing;
    return true;
  }

  void CommitShutdown() noexcept {
    COROPACT_CHECK(resource_ == ResourceState::kOpen,
                   "StreamLifecycle::CommitShutdown requires an open resource");
    COROPACT_CHECK(write_ == WriteState::kShutdownPreparing,
                   "StreamLifecycle::CommitShutdown requires shutdown preparation");
    write_ = WriteState::kShutdown;
  }

  void AbortShutdownPreparation() noexcept {
    COROPACT_CHECK(resource_ == ResourceState::kOpen,
                   "StreamLifecycle::AbortShutdownPreparation requires an open resource");
    COROPACT_CHECK(write_ == WriteState::kShutdownPreparing,
                   "StreamLifecycle::AbortShutdownPreparation requires shutdown preparation");
    write_ = WriteState::kWritable;
  }

  // Starts an owner-local close preparation.  It temporarily excludes new
  // stream operations while the adapter either commits physical drain/close
  // or aborts before any cancel request reached the backend.  false means the
  // resource was already closed; a concurrent preparation receives EBUSY.
  [[nodiscard]]
  Result<bool> PrepareClose() noexcept {
    if (resource_ == ResourceState::kClosed) {
      return false;
    }
    if (resource_ == ResourceState::kClosing) {
      return std::unexpected(Errno(EBUSY));
    }
    if (write_ == WriteState::kShutdownPreparing) {
      return std::unexpected(Errno(EBUSY));
    }
    resource_ = ResourceState::kClosing;
    return true;
  }

  // This is valid only before a cancellation SQE joins the owner loop's
  // submission protocol. Once backend drain is committed, Close is
  // irreversible.
  void AbortClosePreparation() noexcept {
    COROPACT_CHECK(resource_ == ResourceState::kClosing,
                   "StreamLifecycle::AbortClosePreparation requires closing state");
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
    kShutdownPreparing,
    kShutdown,
  };

  ResourceState resource_{ResourceState::kOpen};
  WriteState write_{WriteState::kWritable};
};

static_assert(sizeof(StreamLifecycle) == 2);

}  // namespace coropact::net::detail
