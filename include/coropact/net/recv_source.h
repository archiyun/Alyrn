// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <utility>

#include "coropact/utils/macros.h"

namespace coropact::net {

// A move-only view of one backend-owned receive buffer. The reclaim callback
// returns the buffer to the backend pool/ring exactly once, when the lease is
// released or destroyed.
class BufferLease final {
public:
  COROPACT_DELETE_COPY(BufferLease);
  using ReclaimFn = void (*)(void* context, std::uint32_t buffer_id) noexcept;

  BufferLease() noexcept = default;

  BufferLease(std::byte* data, std::size_t size, std::uint32_t buffer_id, void* context,
              ReclaimFn reclaim) noexcept
      : data_(data), size_(size), buffer_id_(buffer_id), context_(context), reclaim_(reclaim) {}

  BufferLease(BufferLease&& other) noexcept
      : data_(std::exchange(other.data_, nullptr)),
        size_(std::exchange(other.size_, 0)),
        buffer_id_(std::exchange(other.buffer_id_, 0)),
        context_(std::exchange(other.context_, nullptr)),
        reclaim_(std::exchange(other.reclaim_, nullptr)) {}

  BufferLease& operator=(BufferLease&& other) noexcept {
    if (this != &other) {
      Release();
      data_ = std::exchange(other.data_, nullptr);
      size_ = std::exchange(other.size_, 0);
      buffer_id_ = std::exchange(other.buffer_id_, 0);
      context_ = std::exchange(other.context_, nullptr);
      reclaim_ = std::exchange(other.reclaim_, nullptr);
    }
    return *this;
  }

  ~BufferLease() noexcept { Release(); }

  [[nodiscard]]
  bool Valid() const noexcept {
    return reclaim_ != nullptr;
  }

  [[nodiscard]]
  std::span<const std::byte> Bytes() const noexcept {
    return {data_, size_};
  }

  [[nodiscard]]
  std::size_t Size() const noexcept {
    return size_;
  }

  [[nodiscard]]
  std::uint32_t BufferId() const noexcept {
    return buffer_id_;
  }

  void Release() noexcept {
    const ReclaimFn reclaim = std::exchange(reclaim_, nullptr);
    void* const context = std::exchange(context_, nullptr);
    const std::uint32_t buffer_id = std::exchange(buffer_id_, 0);
    data_ = nullptr;
    size_ = 0;

    if (reclaim != nullptr) {
      reclaim(context, buffer_id);
    }
  }

private:
  std::byte* data_{nullptr};
  std::size_t size_{0};
  std::uint32_t buffer_id_{0};
  void* context_{nullptr};
  ReclaimFn reclaim_{nullptr};
};

struct RecvEvent {
  BufferLease buffer;
};

struct RecvSourceOptions {
  std::size_t pending_depth{1};
  std::size_t event_capacity{16};
  std::size_t buffer_capacity{16};
  std::size_t resume_threshold{0};

  [[nodiscard]]
  constexpr bool Valid() const noexcept {
    return pending_depth > 0 && event_capacity > 0 && buffer_capacity >= event_capacity &&
           (resume_threshold == 0 || resume_threshold < event_capacity);
  }

  [[nodiscard]]
  constexpr std::size_t ResumeThreshold() const noexcept {
    return resume_threshold == 0 ? event_capacity / 2 : resume_threshold;
  }
};

}  // namespace coropact::net

// Backend-only lifecycle accounting for RecvSource implementations.
#include "coropact/net/detail/recv_source_state.h"
