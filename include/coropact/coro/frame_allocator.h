// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Optional coroutine-frame allocation through a std::pmr::memory_resource.
// The default resource is std::pmr::new_delete_resource(), so existing code
// keeps heap-backed frame allocation unless a scope or Scheduler selects a
// different resource.
#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <limits>
#include <memory>
#include <memory_resource>
#include <new>

#include "coropact/utils/macros.h"

namespace coropact::coro {
namespace detail {

inline thread_local std::pmr::memory_resource* current_frame_resource =
    std::pmr::new_delete_resource();

inline std::pmr::memory_resource* CurrentFrameResource() noexcept { return current_frame_resource; }

inline void SetCurrentFrameResource(std::pmr::memory_resource* resource) noexcept {
  current_frame_resource = resource != nullptr ? resource : std::pmr::new_delete_resource();
}

// The coroutine frame contains no user-visible pointer to the allocator. Keep
// the allocation metadata immediately before the frame and recover it from a
// marker slot during promise destruction.
struct FrameAllocationHeader {
  std::pmr::memory_resource* resource;
  void* raw;
  std::size_t bytes;
  std::size_t alignment;
};

inline constexpr std::size_t FrameAllocationMarkerSize = sizeof(FrameAllocationHeader*);

inline void* AllocateFrame(std::size_t frame_size, std::size_t frame_alignment) {
  const std::size_t alignment =
      std::max({frame_alignment, alignof(FrameAllocationHeader), alignof(FrameAllocationHeader*)});
  const std::size_t prefix = sizeof(FrameAllocationHeader) + FrameAllocationMarkerSize;
  if (frame_size > std::numeric_limits<std::size_t>::max() - prefix ||
      alignment > std::numeric_limits<std::size_t>::max() - (prefix + frame_size)) {
    throw std::bad_alloc();
  }

  const std::size_t bytes = prefix + frame_size + alignment - 1;
  auto* resource = CurrentFrameResource();
  void* raw = resource->allocate(bytes, alignment);

  auto* header = ::new (raw) FrameAllocationHeader{resource, raw, bytes, alignment};
  void* frame = static_cast<std::byte*>(raw) + prefix;
  std::size_t available = bytes - prefix;
  if (std::align(frame_alignment, frame_size, frame, available) == nullptr) {
    header->~FrameAllocationHeader();
    resource->deallocate(raw, bytes, alignment);
    throw std::bad_alloc();
  }

  auto** marker = reinterpret_cast<FrameAllocationHeader**>(static_cast<std::byte*>(frame) -
                                                            FrameAllocationMarkerSize);
  *marker = header;
  return frame;
}

inline void DeallocateFrame(void* frame) noexcept {
  if (frame == nullptr) {
    return;
  }

  auto** marker = reinterpret_cast<FrameAllocationHeader**>(static_cast<std::byte*>(frame) -
                                                            FrameAllocationMarkerSize);
  FrameAllocationHeader* header = *marker;
  auto* resource = header->resource;
  void* raw = header->raw;
  const std::size_t bytes = header->bytes;
  const std::size_t alignment = header->alignment;
  header->~FrameAllocationHeader();
  resource->deallocate(raw, bytes, alignment);
}

// Inherited by every promise type that owns a coroutine frame. The allocation
// operators are deliberately centralized so Task, SpawnRoot, and SyncWaitRoot
// all honor the same selected resource.
class FrameAllocationSupport {
public:
  static void* operator new(std::size_t frame_size) {
    return AllocateFrame(frame_size, alignof(std::max_align_t));
  }

  static void* operator new(std::size_t frame_size, std::align_val_t alignment) {
    return AllocateFrame(frame_size, static_cast<std::size_t>(alignment));
  }

  static void operator delete(void* frame) noexcept { DeallocateFrame(frame); }
  static void operator delete(void* frame, std::size_t) noexcept { DeallocateFrame(frame); }
  static void operator delete(void* frame, std::align_val_t) noexcept { DeallocateFrame(frame); }
  static void operator delete(void* frame, std::size_t, std::align_val_t) noexcept {
    DeallocateFrame(frame);
  }
};

}  // namespace detail

// A worker-local size-class allocator for coroutine frames. The resource owns
// lazily allocated chunks and keeps a separate intrusive free list for each
// size class. It is intentionally not synchronized: a frame resource must be
// confined to the worker thread that owns it.
class CoroFramePoolResource final : public std::pmr::memory_resource {
public:
  static constexpr std::size_t kSizeClassCount = 9;
  static constexpr std::size_t kChunkBytes = 64 * 1024;
  static constexpr std::size_t kMaxPooledBytes = 16 * 1024;
  inline static constexpr std::array<std::size_t, kSizeClassCount> kSizeClasses{
      64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384};

  CoroFramePoolResource() noexcept : CoroFramePoolResource(*std::pmr::new_delete_resource()) {}

  explicit CoroFramePoolResource(std::pmr::memory_resource& upstream) noexcept
      : upstream_(&upstream) {}

  ~CoroFramePoolResource() override { ReleaseChunks(); }

  COROPACT_DELETE_COPY_MOVE(CoroFramePoolResource);

private:
  struct FreeNode {
    FreeNode* next;
  };

  struct ChunkHeader {
    ChunkHeader* next;
    void* raw;
    std::size_t bytes;
    std::size_t alignment;
  };

  struct SizeClass {
    FreeNode* free_list{nullptr};
    ChunkHeader* chunks{nullptr};
  };

  static constexpr std::size_t kSlotAlignment = alignof(std::max_align_t);

  static constexpr std::size_t AlignUp(std::size_t value, std::size_t alignment) noexcept {
    return (value + alignment - 1) / alignment * alignment;
  }

  [[nodiscard]] std::size_t FindSizeClass(std::size_t bytes, std::size_t alignment) const noexcept {
    if (bytes > kMaxPooledBytes || alignment > kSlotAlignment) {
      return kSizeClassCount;
    }
    if (bytes <= kSizeClasses[0]) {
      return 0;
    }

    // The size classes are powers of two starting at 64 bytes. This turns the
    // common classification path into a few integer instructions instead of a
    // linear scan through every class.
    const auto first_width = static_cast<std::size_t>(std::bit_width(kSizeClasses[0] - 1));
    const auto width = static_cast<std::size_t>(std::bit_width(bytes - 1));
    const std::size_t index = width - first_width;
    if (index < kSizeClassCount) {
      return index;
    }
    return kSizeClassCount;
  }

  void AddChunk(SizeClass& size_class, std::size_t block_size) {
    const std::size_t slot_count = std::max<std::size_t>(8, kChunkBytes / block_size);
    const std::size_t header_bytes = AlignUp(sizeof(ChunkHeader), kSlotAlignment);
    if (slot_count > (std::numeric_limits<std::size_t>::max() - header_bytes) / block_size) {
      throw std::bad_alloc();
    }

    const std::size_t bytes = header_bytes + slot_count * block_size;
    void* raw = upstream_->allocate(bytes, kSlotAlignment);
    auto* chunk = ::new (raw) ChunkHeader{
        .next = size_class.chunks, .raw = raw, .bytes = bytes, .alignment = kSlotAlignment};
    size_class.chunks = chunk;

    auto* slot = static_cast<std::byte*>(raw) + header_bytes;
    for (std::size_t i = 0; i < slot_count; ++i) {
      auto* node = reinterpret_cast<FreeNode*>(slot + i * block_size);
      node->next = size_class.free_list;
      size_class.free_list = node;
    }
  }

  void ReleaseChunks() noexcept {
    for (auto& size_class : classes_) {
      for (ChunkHeader* chunk = size_class.chunks; chunk != nullptr;) {
        ChunkHeader* next = chunk->next;
        void* raw = chunk->raw;
        const std::size_t bytes = chunk->bytes;
        const std::size_t alignment = chunk->alignment;
        chunk->~ChunkHeader();
        upstream_->deallocate(raw, bytes, alignment);
        chunk = next;
      }
      size_class.free_list = nullptr;
      size_class.chunks = nullptr;
    }
  }

  void* do_allocate(std::size_t bytes, std::size_t alignment) override {
    const std::size_t index = FindSizeClass(bytes, alignment);
    if (index == kSizeClassCount) {
      return upstream_->allocate(bytes, alignment);
    }

    SizeClass& size_class = classes_[index];
    if (size_class.free_list == nullptr) {
      AddChunk(size_class, kSizeClasses[index]);
    }
    FreeNode* node = size_class.free_list;
    size_class.free_list = node->next;
    return node;
  }

  void do_deallocate(void* pointer, std::size_t bytes, std::size_t alignment) noexcept override {
    const std::size_t index = FindSizeClass(bytes, alignment);
    if (index == kSizeClassCount) {
      upstream_->deallocate(pointer, bytes, alignment);
      return;
    }

    auto* node = static_cast<FreeNode*>(pointer);
    node->next = classes_[index].free_list;
    classes_[index].free_list = node;
  }

  bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override {
    return this == &other;
  }

  std::pmr::memory_resource* upstream_;
  std::array<SizeClass, kSizeClassCount> classes_{};
};

// Selects the resource used while coroutine frames are created. The resource
// pointer is stored in each allocation, so destruction remains correct even if
// this scope has ended or another resource is current later.
class FrameAllocatorScope {
public:
  COROPACT_DELETE_COPY(FrameAllocatorScope);

  explicit FrameAllocatorScope(std::pmr::memory_resource& resource) noexcept
      : previous_(detail::CurrentFrameResource()) {
    detail::SetCurrentFrameResource(&resource);
  }

  explicit FrameAllocatorScope(std::pmr::memory_resource* resource) noexcept
      : previous_(detail::CurrentFrameResource()) {
    detail::SetCurrentFrameResource(resource);
  }

  ~FrameAllocatorScope() { detail::SetCurrentFrameResource(previous_); }

  static std::pmr::memory_resource* Current() noexcept { return detail::CurrentFrameResource(); }

private:
  std::pmr::memory_resource* previous_;
};

}  // namespace coropact::coro
