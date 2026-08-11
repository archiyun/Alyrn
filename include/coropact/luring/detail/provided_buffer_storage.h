// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <sys/mman.h>

#include <cerrno>
#include <cstddef>
#include <expected>
#include <limits>
#include <utility>

#include "coropact/result.h"
#include "coropact/utils/macros.h"

namespace coropact::luring::detail {

// Owns the anonymous mmap payload memory referenced by a provided-buffer ring.
// The ring itself is owned by the backend; this class only owns the byte
// storage and gives the backend stable slot addresses.
class ProvidedBufferStorage final {
public:
  COROPACT_DELETE_COPY(ProvidedBufferStorage);

  ProvidedBufferStorage() noexcept = default;
  ~ProvidedBufferStorage() noexcept { Release(); }

  ProvidedBufferStorage(ProvidedBufferStorage&& other) noexcept
      : mapped_(std::exchange(other.mapped_, nullptr)),
        capacity_(std::exchange(other.capacity_, 0)),
        buffer_size_(std::exchange(other.buffer_size_, 0)),
        size_bytes_(std::exchange(other.size_bytes_, 0)) {
  }

  ProvidedBufferStorage& operator=(ProvidedBufferStorage&& other) noexcept {
    if (this == &other) {
      return *this;
    }

    Release();
    mapped_ = std::exchange(other.mapped_, nullptr);
    capacity_ = std::exchange(other.capacity_, 0);
    buffer_size_ = std::exchange(other.buffer_size_, 0);
    size_bytes_ = std::exchange(other.size_bytes_, 0);
    return *this;
  }

  [[nodiscard]]
  static Result<ProvidedBufferStorage> Create(
      std::size_t capacity, std::size_t buffer_size) noexcept {
    if (capacity == 0 || buffer_size == 0) {
      return std::unexpected(Errno(EINVAL));
    }
    if (capacity > (std::numeric_limits<std::size_t>::max() / buffer_size)) {
      return std::unexpected(Errno(EOVERFLOW));
    }
    ProvidedBufferStorage storage;
    storage.capacity_ = capacity;
    storage.buffer_size_ = buffer_size;
    storage.size_bytes_ = capacity * buffer_size;

    void* mapped = ::mmap(nullptr, storage.size_bytes_, PROT_READ | PROT_WRITE,
                          MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (mapped == MAP_FAILED) [[unlikely]] {
      return std::unexpected(CurrentErrno());
    }
    storage.mapped_ = static_cast<std::byte*>(mapped);
    return storage;
  }

  [[nodiscard]]
  std::size_t capacity() const noexcept {
    return capacity_;
  }

  [[nodiscard]]
  std::size_t buffer_size() const noexcept {
    return buffer_size_;
  }

  [[nodiscard]]
  std::size_t size_bytes() const noexcept {
    return size_bytes_;
  }

  [[nodiscard]]
  std::byte* data() noexcept {
    return mapped_;
  }

  [[nodiscard]]
  const std::byte* data() const noexcept {
    return mapped_;
  }

  [[nodiscard]]
  std::byte* slot(std::size_t buffer_id) noexcept {
    if (buffer_id >= capacity_) {
      return nullptr;
    }
    return data() + (buffer_id * buffer_size_);
  }

  [[nodiscard]]
  const std::byte* slot(std::size_t buffer_id) const noexcept {
    if (buffer_id >= capacity_) {
      return nullptr;
    }
    return data() + (buffer_id * buffer_size_);
  }

private:
  void Release() noexcept {
    if (mapped_ != nullptr) {
      (void)::munmap(mapped_, size_bytes_);
      mapped_ = nullptr;
    }
    capacity_ = 0;
    buffer_size_ = 0;
    size_bytes_ = 0;
  }

  std::byte* mapped_{nullptr};
  std::size_t capacity_{0};
  std::size_t buffer_size_{0};
  std::size_t size_bytes_{0};
};

}  // namespace coropact::luring::detail
