// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Optional coroutine-frame allocation through a std::pmr::memory_resource.
// The default resource is std::pmr::new_delete_resource(), so existing code
// keeps heap-backed frame allocation unless a scope or Scheduler selects a
// different resource.
#pragma once

#include <algorithm>
#include <cstddef>
#include <limits>
#include <memory>
#include <memory_resource>
#include <new>

#include "vexo/utils/macros.h"

namespace vexo::coro {
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

// Selects the resource used while coroutine frames are created. The resource
// pointer is stored in each allocation, so destruction remains correct even if
// this scope has ended or another resource is current later.
class FrameAllocatorScope {
public:
  VEXO_DELETE_COPY(FrameAllocatorScope);

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

}  // namespace vexo::coro
