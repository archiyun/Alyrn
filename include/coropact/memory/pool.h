// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
// Reference: https://github.com/nginx/nginx/blob/master/src/core/ngx_palloc.h
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>

#include "coropact/base/check.h"
#include "coropact/utils/macros.h"

namespace coropact::memory {

// Pool is an nginx-style, request-scoped arena allocator.
//
// Pool is not thread-safe. Keep it confined to its owning thread, or provide
// external synchronization when sharing it.
//
// Layout:
// The Pool object itself is embedded at the beginning of the first chunk
// via placement new and a custom deleter, matching nginx's ngx_pool_t layout.
//
// A single heap allocation contains both the Pool header and the initial
// allocation buffer. Additional chunks use a smaller ChunkHeader layout
// to maximize usable payload space.
//
// Typical usage:
//   auto pool = Pool::Create(8192);
//   void* p = pool->Allocate(64);
//   pool->RegisterCleanup(&close_fd, sizeof(int));
//   // Pool destruction:
//   //   cleanup handlers -> large allocations -> chunk chain -> Pool itself
class Pool {
public:
  COROPACT_DELETE_COPY_MOVE(Pool);

  static constexpr std::size_t kDefaultChunkSize = 1 << 12;
  static constexpr std::size_t kMaxSmallAlloc = kDefaultChunkSize - 1;
  static constexpr std::size_t kFailedThreshold = 1 << 2;
  static constexpr std::size_t kMinChunkSize = 1 << 7;

  struct Deleter {
    void operator()(Pool* p) const noexcept;
  };
  using Ptr = std::unique_ptr<Pool, Deleter>;

  // Creates a pool whose header is placement-new'ed at the beginning of its
  // first arena chunk. Stack allocation and direct new are intentionally
  // disallowed.
  static Ptr Create(std::size_t chunk_size = kDefaultChunkSize);

  // Allocations up to the pool's small-allocation limit use the bump arena
  // fast path. Larger allocations bypass the arena.
  void* Allocate(std::size_t size);
  // The alignment must be a nonzero power of two.
  void* AllocateAligned(std::size_t size, std::size_t alignment);
  void* AllocateUnaligned(std::size_t size);
  void* Callocate(std::size_t size);

  // Releases a large allocation. Small allocations are reclaimed by Reset()
  // or Pool destruction. Passing a non-large or foreign pointer is a no-op.
  void Free(void* pointer) noexcept;

  // Releases large allocations and rewinds all chunk bump pointers. Cleanup
  // handlers are discarded without being executed.
  void Reset() noexcept;

  // Registers handler(data) for LIFO execution during Pool destruction. The
  // returned data storage is allocated from the arena itself.
  void* RegisterCleanup(void (*handler)(void*), std::size_t data_size);

  [[nodiscard]] std::size_t chunk_count() const noexcept;
  [[nodiscard]] std::size_t large_count() const noexcept;
  [[nodiscard]] std::size_t ByteUsed() const noexcept;

private:
  struct ChunkHeader {
    std::byte* next_free;
    std::byte* end;
    ChunkHeader* next;
    std::uint32_t failed_allocations;
  };

  struct LargeNode {
    void* allocation;
    LargeNode* next;
  };

  struct CleanupNode {
    void (*handler)(void*);
    void* data;
    CleanupNode* next;
  };

  explicit Pool(std::size_t chunk_size) noexcept;
  ~Pool() = default;

  void DestroyArena() noexcept;
  void* AllocateSmall(std::size_t size, std::size_t alignment);
  void* AllocateLarge(std::size_t size);
  ChunkHeader* AllocateChunk();

  // The Pool object is embedded at the beginning of the first chunk.
  ChunkHeader first_chunk_;
  std::size_t max_small_alloc_;
  ChunkHeader* current_chunk_;
  LargeNode* large_head_;
  CleanupNode* cleanup_head_;
};

namespace pool_detail {

constexpr std::size_t kDefaultAlignment = alignof(std::max_align_t);
constexpr std::size_t kLargeAllocationSearchLimit = 3;

// Rounds pointer address upward to the next alignment boundary.
inline const std::byte* AlignPointer(const std::byte* pointer, std::size_t alignment) noexcept {
  const auto address = reinterpret_cast<std::uintptr_t>(pointer);
  const auto mask = static_cast<std::uintptr_t>(alignment) - 1;
  return reinterpret_cast<const std::byte*>((address + mask) & ~mask);
}

inline std::byte* AlignPointer(std::byte* pointer, std::size_t alignment) noexcept {
  return const_cast<std::byte*>(AlignPointer(static_cast<const std::byte*>(pointer), alignment));
}

}  // namespace pool_detail

// Ensure the minimum chunk size can hold the Pool header
// plus at least two LargeNode-sized allocations.
// Based on Nginx.
static_assert(Pool::kMinChunkSize >= sizeof(Pool) + 2 * sizeof(void*) * 2,
              "kMinChunkSize must fit Pool + 2 LargeNode slots");

inline Pool::Ptr Pool::Create(std::size_t chunk_size) {
  COROPACT_CHECK(chunk_size >= kMinChunkSize, "Pool::Create: chunk_size below kMinChunkSize");

  void* storage = ::operator new(chunk_size, std::align_val_t{pool_detail::kDefaultAlignment});
  auto* pool = ::new (storage) Pool(chunk_size);
  return Ptr{pool, Deleter{}};
}

inline void Pool::Deleter::operator()(Pool* p) const noexcept {
  if (p == nullptr) {
    return;
  }

  p->DestroyArena();
  p->~Pool();
  ::operator delete(static_cast<void*>(p), std::align_val_t{pool_detail::kDefaultAlignment});
}

inline Pool::Pool(std::size_t chunk_size) noexcept
    : first_chunk_{},
      max_small_alloc_{},
      current_chunk_{},
      large_head_{nullptr},
      cleanup_head_{nullptr} {
  // The first payload starts immediately after the embedded Pool header.
  auto* pool_begin = reinterpret_cast<std::byte*>(this);
  first_chunk_.next_free = pool_begin + sizeof(Pool);
  first_chunk_.end = pool_begin + chunk_size;
  first_chunk_.next = nullptr;
  first_chunk_.failed_allocations = 0;

  const std::size_t usable = chunk_size - sizeof(Pool);
  max_small_alloc_ = std::min(usable, kMaxSmallAlloc);
  current_chunk_ = &first_chunk_;
}

// Destruction order: cleanup handlers, large allocations, extra chunks, and
// finally the first chunk containing the Pool object.
inline void Pool::DestroyArena() noexcept {
  for (auto* cleanup = cleanup_head_; cleanup != nullptr; cleanup = cleanup->next) {
    if (cleanup->handler != nullptr) {
      cleanup->handler(cleanup->data);
    }
  }

  for (auto* large = large_head_; large != nullptr; large = large->next) {
    if (large->allocation != nullptr) {
      ::operator delete(large->allocation);
    }
  }

  // Extra chunks embed their ChunkHeader at the beginning of their allocation
  // block. The first chunk is released separately by Deleter.
  for (auto* chunk = first_chunk_.next; chunk != nullptr;) {
    auto* next = chunk->next;
    ::operator delete(chunk, std::align_val_t{pool_detail::kDefaultAlignment});
    chunk = next;
  }
}

inline void* Pool::Allocate(std::size_t size) {
  return AllocateAligned(size, pool_detail::kDefaultAlignment);
}

inline void* Pool::AllocateUnaligned(std::size_t size) { return AllocateAligned(size, 1); }

inline void* Pool::AllocateAligned(std::size_t size, std::size_t alignment) {
  return size <= max_small_alloc_ ? AllocateSmall(size, alignment) : AllocateLarge(size);
}

inline void* Pool::Callocate(std::size_t size) {
  void* memory = AllocateAligned(size, pool_detail::kDefaultAlignment);
  if (memory != nullptr) {
    std::memset(memory, 0, size);
  }
  return memory;
}

inline void Pool::Free(void* pointer) noexcept {
  for (LargeNode* node = large_head_; node != nullptr; node = node->next) {
    if (node->allocation == pointer) {
      ::operator delete(node->allocation);
      node->allocation = nullptr;
      return;
    }
  }
}

inline void Pool::Reset() noexcept {
  for (LargeNode* node = large_head_; node != nullptr; node = node->next) {
    if (node->allocation != nullptr) {
      ::operator delete(node->allocation);
      node->allocation = nullptr;
    }
  }
  large_head_ = nullptr;
  cleanup_head_ = nullptr;

  auto* pool_begin = reinterpret_cast<std::byte*>(this);
  first_chunk_.next_free = pool_begin + sizeof(Pool);
  first_chunk_.failed_allocations = 0;

  for (ChunkHeader* chunk = first_chunk_.next; chunk != nullptr; chunk = chunk->next) {
    chunk->next_free = pool_detail::AlignPointer(
        reinterpret_cast<std::byte*>(chunk) + sizeof(ChunkHeader), pool_detail::kDefaultAlignment);
    chunk->failed_allocations = 0;
  }
  current_chunk_ = &first_chunk_;
}

inline void* Pool::RegisterCleanup(void (*cleanup_handler)(void*), std::size_t data_size) {
  const std::size_t allocation_size = sizeof(CleanupNode) + data_size;
  auto* memory =
      static_cast<std::byte*>(AllocateAligned(allocation_size, pool_detail::kDefaultAlignment));
  if (memory == nullptr) {
    return nullptr;
  }

  auto* node = reinterpret_cast<CleanupNode*>(memory);
  node->handler = cleanup_handler;
  node->data = (data_size > 0) ? memory + sizeof(CleanupNode) : nullptr;
  node->next = cleanup_head_;
  cleanup_head_ = node;
  return node->data;
}

inline std::size_t Pool::chunk_count() const noexcept {
  std::size_t count = 1;  // The first chunk contains the Pool object.
  for (auto* chunk = first_chunk_.next; chunk != nullptr; chunk = chunk->next) {
    ++count;
  }
  return count;
}

inline std::size_t Pool::large_count() const noexcept {
  std::size_t count = 0;
  for (auto* node = large_head_; node != nullptr; node = node->next) {
    if (node->allocation != nullptr) {
      ++count;
    }
  }
  return count;
}

inline std::size_t Pool::ByteUsed() const noexcept {
  const auto* first_payload = reinterpret_cast<const std::byte*>(this) + sizeof(Pool);
  auto bytes_used = static_cast<std::size_t>(first_chunk_.next_free - first_payload);

  for (auto* chunk = first_chunk_.next; chunk != nullptr; chunk = chunk->next) {
    const auto* chunk_begin = reinterpret_cast<const std::byte*>(chunk);
    const auto* payload_begin = pool_detail::AlignPointer(chunk_begin + sizeof(ChunkHeader),
                                                          pool_detail::kDefaultAlignment);
    bytes_used += static_cast<std::size_t>(chunk->next_free - payload_begin);
  }
  return bytes_used;
}

inline void* Pool::AllocateSmall(std::size_t size, std::size_t alignment) {
  ChunkHeader* chunk = current_chunk_;
  while (true) {
    std::byte* aligned = pool_detail::AlignPointer(chunk->next_free, alignment);
    if (aligned <= chunk->end && static_cast<std::size_t>(chunk->end - aligned) >= size) {
      chunk->next_free = aligned + size;
      return aligned;
    }

    if (chunk->next == nullptr) {
      break;
    }
    chunk = chunk->next;
  }

  // No existing chunk has enough space. Allocate a new chunk.
  ChunkHeader* fresh = AllocateChunk();
  std::byte* aligned = pool_detail::AlignPointer(fresh->next_free, alignment);
  void* result = aligned;
  fresh->next_free = aligned + size;

  // nginx-style heuristic:
  // Increment failed counters for skipped chunks and gradually advance
  // current_chunk_ toward newer chunks.
  chunk = current_chunk_;
  while (chunk->next != nullptr) {
    if (chunk->failed_allocations++ >= kFailedThreshold) {
      current_chunk_ = chunk->next;
    }
    chunk = chunk->next;
  }
  chunk->next = fresh;
  return result;
}

inline void* Pool::AllocateLarge(std::size_t size) {
  void* allocation = ::operator new(size);

  std::size_t slots_checked = 0;
  for (LargeNode* node = large_head_; node != nullptr; node = node->next) {
    if (node->allocation == nullptr) {
      node->allocation = allocation;
      return allocation;
    }
    if (++slots_checked >= pool_detail::kLargeAllocationSearchLimit) {
      break;
    }
  }

  auto* node = static_cast<LargeNode*>(AllocateSmall(sizeof(LargeNode), alignof(LargeNode)));
  node->allocation = allocation;
  node->next = large_head_;
  large_head_ = node;
  return allocation;
}

inline Pool::ChunkHeader* Pool::AllocateChunk() {
  const auto chunk_size =
      static_cast<std::size_t>(first_chunk_.end - reinterpret_cast<std::byte*>(this));

  auto* raw = static_cast<std::byte*>(
      ::operator new(chunk_size, std::align_val_t{pool_detail::kDefaultAlignment}));

  auto* chunk = reinterpret_cast<ChunkHeader*>(raw);
  chunk->next_free =
      pool_detail::AlignPointer(raw + sizeof(ChunkHeader), pool_detail::kDefaultAlignment);
  chunk->end = raw + chunk_size;
  chunk->next = nullptr;
  chunk->failed_allocations = 0;
  return chunk;
}

}  // namespace coropact::memory
