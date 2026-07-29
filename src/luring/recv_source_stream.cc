// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/luring/recv_source_stream.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <expected>
#include <utility>

namespace coropact::luring {

base::Result<LUringRecvSourceStream> LUringRecvSourceStream::Create(
    LUringStream stream,
    LUringRecvSourceOptions options) noexcept {
  auto source = LUringRecvSource::Create(stream.Loop(), stream.Fd(), options);
  if (!source.has_value()) {
    return std::unexpected(source.error());
  }
  return LUringRecvSourceStream(std::move(stream), std::move(*source));
}

base::Result<std::size_t> LUringRecvSourceStream::CopyPending(
    std::span<std::byte> buffer) noexcept {
  if (!pending_lease_.has_value() || pending_offset_ >= pending_lease_->Size()) {
    pending_lease_.reset();
    pending_offset_ = 0;
    return std::size_t{0};
  }

  const auto bytes = pending_lease_->Bytes().subspan(pending_offset_);
  const std::size_t copied = std::min(buffer.size(), bytes.size());
  std::memcpy(buffer.data(), bytes.data(), copied);
  pending_offset_ += copied;
  if (pending_offset_ == pending_lease_->Size()) {
    pending_lease_.reset();
    pending_offset_ = 0;
  }
  return copied;
}

coro::Task<base::Result<std::size_t>> LUringRecvSourceStream::ReadSome(
    std::span<std::byte> buffer) {
  if (buffer.empty()) {
    co_return std::size_t{0};
  }

  if (pending_lease_.has_value()) {
    co_return CopyPending(buffer);
  }

  auto next = co_await source_.Next();
  if (!next.has_value()) {
    co_return std::unexpected(next.error());
  }
  if (!next->has_value()) {
    co_return std::size_t{0};
  }

  pending_lease_.emplace(std::move((*next)->buffer));
  pending_offset_ = 0;
  co_return CopyPending(buffer);
}

coro::Task<base::Result<void>> LUringRecvSourceStream::Close() {
  // A lease held for a partially consumed ReadSome() must leave before Stop
  // can reach its terminal ownership boundary.
  pending_lease_.reset();
  pending_offset_ = 0;

  auto requested = source_.RequestStop();
  if (!requested.has_value()) {
    co_return std::unexpected(requested.error());
  }

  // Stop() intentionally waits for queued events too. This adapter owns those
  // events, so discard them here by obtaining and immediately releasing each
  // lease before waiting for the source's terminal state.
  for (;;) {
    auto next = co_await source_.Next();
    if (!next.has_value()) {
      break;
    }
    if (!next->has_value()) {
      break;
    }
  }

  auto stopped = co_await source_.Stop();
  if (!stopped.has_value()) {
    co_return std::unexpected(stopped.error());
  }
  co_return co_await stream_.Close();
}

}  // namespace coropact::luring
