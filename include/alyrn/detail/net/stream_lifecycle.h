// SPDX-License-Identifier: MIT
#pragma once

#include <cerrno>
#include <cstdint>
#include <expected>

#include "alyrn/detail/check.h"
#include "alyrn/result.h"

namespace alyrn::net::detail {

/*
 * Backend-neutral state for a connected stream. It owns no fd and performs no
 * syscall; backend adapters map physical operations onto these transitions.
 * In particular, half-close preparation excludes new operations in the
 * affected direction until the adapter either commits its physical shutdown
 * or aborts the local submission.
 */
class StreamLifecycle final {
public:
  StreamLifecycle() noexcept = default;
  StreamLifecycle(const StreamLifecycle&) = delete;
  StreamLifecycle& operator=(const StreamLifecycle&) = delete;

  StreamLifecycle(StreamLifecycle&& other) noexcept
      : resource_(other.resource_), read_(other.read_), write_(other.write_) {
    other.MarkClosed();
  }

  StreamLifecycle& operator=(StreamLifecycle&& other) noexcept {
    if (this != &other) {
      resource_ = other.resource_;
      read_ = other.read_;
      write_ = other.write_;
      other.MarkClosed();
    }
    return *this;
  }

  Result<void> ValidateRead() const noexcept {
    if (resource_ == ResourceState::kClosing) {
      return std::unexpected(Errno(ECANCELED));
    }
    if (resource_ == ResourceState::kClosed) {
      return std::unexpected(Errno(EBADF));
    }
    return {};
  }

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

  bool IsReadShutdown() const noexcept {
    return read_ == ReadState::kShutdown;
  }

  // Begins a synchronous physical read-side shutdown transition. true means
  // the adapter now owns a required SHUT_RD syscall and must either
  // CommitCloseRead() on success or AbortCloseReadPreparation() before
  // reporting its local error. false means the read side was already shut
  // down.
  Result<bool> PrepareCloseRead(bool read_pending) noexcept {
    auto readable = ValidateRead();
    if (!readable.has_value()) {
      return std::unexpected(readable.error());
    }
    if (read_ == ReadState::kShutdown) {
      return false;
    }
    if (read_ == ReadState::kShutdownPreparing || read_pending) {
      return std::unexpected(Errno(EBUSY));
    }
    read_ = ReadState::kShutdownPreparing;
    return true;
  }

  void CommitCloseRead() noexcept {
    ALYRN_CHECK(resource_ == ResourceState::kOpen,
                   "StreamLifecycle::CommitCloseRead requires an open resource");
    ALYRN_CHECK(read_ == ReadState::kShutdownPreparing,
                   "StreamLifecycle::CommitCloseRead requires close-read preparation");
    read_ = ReadState::kShutdown;
  }

  void AbortCloseReadPreparation() noexcept {
    ALYRN_CHECK(resource_ == ResourceState::kOpen,
                   "StreamLifecycle::AbortCloseReadPreparation requires an open resource");
    ALYRN_CHECK(read_ == ReadState::kShutdownPreparing,
                   "StreamLifecycle::AbortCloseReadPreparation requires close-read preparation");
    read_ = ReadState::kReadable;
  }

  // Begins a synchronous physical shutdown transition. true means the adapter
  // now owns a required shutdown syscall and must either CommitShutdown() on
  // success or AbortShutdownPreparation() before reporting its local error.
  // false means the write side was already shut down.
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
    ALYRN_CHECK(resource_ == ResourceState::kOpen,
                   "StreamLifecycle::CommitShutdown requires an open resource");
    ALYRN_CHECK(write_ == WriteState::kShutdownPreparing,
                   "StreamLifecycle::CommitShutdown requires shutdown preparation");
    write_ = WriteState::kShutdown;
  }

  void AbortShutdownPreparation() noexcept {
    ALYRN_CHECK(resource_ == ResourceState::kOpen,
                   "StreamLifecycle::AbortShutdownPreparation requires an open resource");
    ALYRN_CHECK(write_ == WriteState::kShutdownPreparing,
                   "StreamLifecycle::AbortShutdownPreparation requires shutdown preparation");
    write_ = WriteState::kWritable;
  }

  // Starts an owner-local close preparation.  It temporarily excludes new
  // stream operations while the adapter either commits physical drain/close
  // or aborts before any cancel request reached the backend.  false means the
  // resource was already closed; a concurrent preparation receives EBUSY.
  Result<bool> PrepareClose() noexcept {
    if (resource_ == ResourceState::kClosed) {
      return false;
    }
    if (resource_ == ResourceState::kClosing) {
      return std::unexpected(Errno(EBUSY));
    }
    if (read_ == ReadState::kShutdownPreparing || write_ == WriteState::kShutdownPreparing) {
      return std::unexpected(Errno(EBUSY));
    }
    resource_ = ResourceState::kClosing;
    return true;
  }

  // This is valid only before a cancellation SQE joins the owner loop's
  // submission protocol. Once backend drain is committed, Close is
  // irreversible.
  void AbortClosePreparation() noexcept {
    ALYRN_CHECK(resource_ == ResourceState::kClosing,
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

  enum class ReadState : std::uint8_t {
    kReadable,
    kShutdownPreparing,
    kShutdown,
  };

  ResourceState resource_{ResourceState::kOpen};
  ReadState read_{ReadState::kReadable};
  WriteState write_{WriteState::kWritable};
};

static_assert(sizeof(StreamLifecycle) == 3);

}  // namespace alyrn::net::detail
