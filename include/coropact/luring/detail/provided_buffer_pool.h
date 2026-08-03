// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <liburing.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <utility>
#include <vector>

#include "coropact/base/error.h"
#include "coropact/luring/detail/provided_buffer_storage.h"
#include "coropact/utils/macros.h"

namespace coropact::luring::detail {

// One loop-affine provided-buffer ring shared by all receive sources on the
// loop. The kernel-visible buffer ids are pool-global, not source-local.
//
// This pool deliberately models non-incremental buffers only: one CQE consumes
// one complete slot and one BufferLease release returns it to the ring.
// Incremental consumption is not part of the current RecvSource contract.
class ProvidedBufferPool final {
public:
  COROPACT_DELETE_COPY(ProvidedBufferPool);

  ProvidedBufferPool() noexcept = default;
  ~ProvidedBufferPool() noexcept { Release(); }

  ProvidedBufferPool(ProvidedBufferPool&& other) noexcept
      : ring_(std::exchange(other.ring_, nullptr)),
        buffer_ring_(std::exchange(other.buffer_ring_, nullptr)),
        buffer_group_(std::exchange(other.buffer_group_, 0)),
        capacity_(std::exchange(other.capacity_, 0)),
        buffer_size_(std::exchange(other.buffer_size_, 0)),
        mask_(std::exchange(other.mask_, 0)),
        storage_(std::move(other.storage_)),
        in_use_(std::move(other.in_use_)) {}

  ProvidedBufferPool& operator=(ProvidedBufferPool&& other) noexcept {
    if (this == &other) {
      return *this;
    }
    Release();
    ring_ = std::exchange(other.ring_, nullptr);
    buffer_ring_ = std::exchange(other.buffer_ring_, nullptr);
    buffer_group_ = std::exchange(other.buffer_group_, 0);
    capacity_ = std::exchange(other.capacity_, 0);
    buffer_size_ = std::exchange(other.buffer_size_, 0);
    mask_ = std::exchange(other.mask_, 0);
    storage_ = std::move(other.storage_);
    in_use_ = std::move(other.in_use_);
    return *this;
  }

  [[nodiscard]]
  static base::Result<ProvidedBufferPool> Create(
      io_uring* ring,
      std::uint16_t buffer_group,
      std::size_t capacity,
      std::size_t buffer_size) noexcept {
    if (ring == nullptr || capacity == 0 || capacity > 32 * 1024 ||
        (capacity & (capacity - 1)) != 0 || buffer_size == 0 ||
        buffer_size > std::numeric_limits<std::uint32_t>::max()) {
      return std::unexpected(base::MakeErrno(EINVAL));
    }

    auto storage = ProvidedBufferStorage::Create(capacity, buffer_size);
    if (!storage.has_value()) {
      return std::unexpected(storage.error());
    }

    int setup_error = 0;
    io_uring_buf_ring* buffer_ring = io_uring_setup_buf_ring(
        ring, static_cast<unsigned>(capacity),
        static_cast<int>(buffer_group), 0, &setup_error);
    if (buffer_ring == nullptr) {
      if (setup_error < 0) {
        return std::unexpected(base::MakeNegErrno(setup_error));
      }
      return std::unexpected(base::MakeErrno(
          setup_error == 0 ? EIO : setup_error));
    }

    ProvidedBufferPool pool;
    pool.ring_ = ring;
    pool.buffer_ring_ = buffer_ring;
    pool.buffer_group_ = buffer_group;
    pool.capacity_ = capacity;
    pool.buffer_size_ = buffer_size;
    pool.mask_ = io_uring_buf_ring_mask(static_cast<unsigned>(capacity));
    pool.storage_ = std::move(*storage);

    try {
      pool.in_use_.assign(capacity, false);
    } catch (...) {
      return std::unexpected(base::MakeErrno(ENOMEM));
    }

    io_uring_buf_ring_init(pool.buffer_ring_);
    for (std::size_t id = 0; id < capacity; ++id) {
      io_uring_buf_ring_add(
          pool.buffer_ring_, pool.storage_.slot(id),
          static_cast<unsigned>(buffer_size),
          static_cast<unsigned short>(id), pool.mask_,
          static_cast<int>(id));
    }
    io_uring_buf_ring_advance(
        pool.buffer_ring_, static_cast<int>(capacity));
    return pool;
  }

  [[nodiscard]]
  std::uint16_t BufferGroup() const noexcept { return buffer_group_; }

  [[nodiscard]]
  std::size_t capacity() const noexcept { return capacity_; }

  [[nodiscard]]
  std::size_t buffer_size() const noexcept { return buffer_size_; }

  [[nodiscard]]
  std::byte* slot(std::uint32_t buffer_id) noexcept {
    return buffer_id < capacity_ ? storage_.slot(buffer_id) : nullptr;
  }

  // Called exactly once for every positive CQE that carries this pool's id.
  [[nodiscard]]
  bool Acquire(std::uint32_t buffer_id) noexcept {
    if (buffer_id >= in_use_.size() || in_use_[buffer_id]) {
      return false;
    }
    in_use_[buffer_id] = true;
    return true;
  }

  // Re-adds a released slot to the shared ring. All calls are loop-thread
  // affine, so no lock or atomic operation is needed here.
  [[nodiscard]]
  bool Return(std::uint32_t buffer_id) noexcept {
    if (buffer_id >= in_use_.size() || !in_use_[buffer_id] ||
        buffer_ring_ == nullptr) {
      return false;
    }
    io_uring_buf_ring_add(
        buffer_ring_, storage_.slot(buffer_id),
        static_cast<unsigned>(buffer_size_),
        static_cast<unsigned short>(buffer_id), mask_, 0);
    io_uring_buf_ring_advance(buffer_ring_, 1);
    in_use_[buffer_id] = false;
    return true;
  }

private:
  void Release() noexcept {
    if (buffer_ring_ != nullptr && ring_ != nullptr) {
      (void)io_uring_free_buf_ring(
          ring_, buffer_ring_, static_cast<unsigned>(capacity_),
          static_cast<int>(buffer_group_));
    }
    ring_ = nullptr;
    buffer_ring_ = nullptr;
    buffer_group_ = 0;
    capacity_ = 0;
    buffer_size_ = 0;
    mask_ = 0;
    in_use_.clear();
  }

  io_uring* ring_{nullptr};
  io_uring_buf_ring* buffer_ring_{nullptr};
  std::uint16_t buffer_group_{0};
  std::size_t capacity_{0};
  std::size_t buffer_size_{0};
  int mask_{0};
  ProvidedBufferStorage storage_;
  std::vector<std::uint8_t> in_use_;
};

}  // namespace coropact::luring::detail
