// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <optional>
#include <span>
#include <utility>

#include "coropact/backend/async_stream.h"
#include "coropact/base/error.h"
#include "coropact/coro/task.h"
#include "coropact/luring/recv_source.h"
#include "coropact/luring/stream.h"
#include "coropact/net/write_part.h"
#include "coropact/utils/macros.h"

namespace coropact::luring {

// Adapts the luring-only multishot receive source to the ordinary AsyncStream
// read contract. It is deliberately an adapter rather than a LUringStream
// mode: ReadSome() still copies into the caller's span, while the source keeps
// its distinct CQE, BufferLease, and provided-buffer-ring lifetimes visible at
// the backend boundary.
class LUringRecvSourceStream final {
public:
  COROPACT_DELETE_COPY(LUringRecvSourceStream);

  [[nodiscard]]
  static base::Result<LUringRecvSourceStream> Create(
      LUringStream stream,
      LUringRecvSourceOptions options = {}) noexcept;

  ~LUringRecvSourceStream() = default;

  LUringRecvSourceStream(LUringRecvSourceStream&&) noexcept = default;
  LUringRecvSourceStream& operator=(LUringRecvSourceStream&&) noexcept = default;

  coro::Task<base::Result<std::size_t>> ReadSome(std::span<std::byte> buffer);

  [[nodiscard]]
  LUringStream::WriteSomeAwaiter WriteSome(
      std::span<const std::byte> buffer) noexcept {
    return stream_.WriteSome(buffer);
  }

  [[nodiscard]]
  LUringStream::WriteSomePartsAwaiter WriteSome(
      std::span<const backend::WritePart> buffers) noexcept {
    return stream_.WriteSome(buffers);
  }

  [[nodiscard]]
  bool ZeroCopyWritesEnabled() const noexcept {
    return stream_.ZeroCopyWritesEnabled();
  }

  [[nodiscard]]
  LUringStream::SendZeroCopyAwaiter SendZeroCopy(
      std::span<const std::byte> buffer) noexcept {
    return stream_.SendZeroCopy(buffer);
  }

  coro::Task<base::Result<void>> Shutdown() {
    co_return co_await stream_.Shutdown();
  }

  // Stop admission first, release any already queued lease, then wait for the
  // source terminal boundary before closing the underlying fd. This preserves
  // the provided-buffer ring's ownership contract during session teardown.
  coro::Task<base::Result<void>> Close();

  [[nodiscard]]
  const net::Endpoint& PeerAddress() const noexcept {
    return stream_.PeerAddress();
  }

private:
  LUringRecvSourceStream(LUringStream stream, LUringRecvSource source) noexcept
      : stream_(std::move(stream)), source_(std::move(source)) {}

  [[nodiscard]]
  base::Result<std::size_t> CopyPending(std::span<std::byte> buffer) noexcept;

  LUringStream stream_;
  LUringRecvSource source_;
  std::optional<net::BufferLease> pending_lease_;
  std::size_t pending_offset_{0};
};

static_assert(backend::AsyncStream<LUringRecvSourceStream>);

}  // namespace coropact::luring
