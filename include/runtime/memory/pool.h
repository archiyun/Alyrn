// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
// Reference: https://github.com/nginx/nginx/blob/master/src/core/ngx_palloc.h
#pragma once

#include "runtime/base/noncopyable.h"  // IWYU pragma: keep

#include <cstddef>
#include <cstdint>
#include <memory>

namespace runtime::memory {

// Pool is an nginx-style per-request arena allocator.
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
class Pool : public runtime::base::NonCopyable {
public:
  inline static constexpr std::size_t kDefaultChunkSize = 1 << 12;
  inline static constexpr std::size_t kMaxSmallAlloc    = kDefaultChunkSize - 1;
  inline static constexpr std::size_t kFailedThreshold  = 1 << 2;
  inline static constexpr std::size_t kMinChunkSize     = 1 << 7;

  struct Deleter {
    void operator()(Pool* p) const noexcept;
  };
  using Ptr = std::unique_ptr<Pool, Deleter>;

  // Pool must be placement-new'ed at the beginning of its own arena memory,
  // so stack allocation and direct new are intentionally disallowed.
  static Ptr Create(std::size_t chunk_size = kDefaultChunkSize);

  // size <= max_ uses the bump arena fast path.
  // Larger allocations bypass the arena and use the large-allocation path.
  void* Allocate(std::size_t size);
  void* AllocateAligned(std::size_t size, std::size_t align);
  void* AllocateUnaligned(std::size_t size);
  void* Callocate(std::size_t size);

  // Only valid for large allocations.
  // Small allocations are reclaimed by Reset() or Pool destruction.
  void Free(void* p) noexcept;

  // Does not execute cleanup handlers.
  // Releases large allocations and rewinds all chunk bump pointers.
  void Reset() noexcept;

  // handler(data) is executed in LIFO order during Pool destruction.
  // Returned data memory is allocated from the arena itself.
  void* RegisterCleanup(void (*handler)(void*), std::size_t data_size);

  std::size_t ChunkCount() const noexcept;
  std::size_t LargeCount() const noexcept;
  std::size_t ByteUsed()   const noexcept;

private:
  struct ChunkHeader {
    std::byte*    last;
    std::byte*    end;
    ChunkHeader*  next;
    std::uint32_t failed;
  };

  struct LargeNode {
    void*       alloc;
    LargeNode*  next;
  };

  struct CleanupNode {
    void (*handler)(void*);
    void*         data;
    CleanupNode*  next;
  };

  explicit Pool(std::size_t chunk_size) noexcept;
  ~Pool() = default;

  void DestroyArena() noexcept;
  void* AllocateSmall(std::size_t size, std::size_t alignment);
  void* AllocateLarge(std::size_t size);
  ChunkHeader* AllocateChunk();

  // reinterpret_cast<ChunkHeader*>(this) == &d_,
  ChunkHeader  d_;
  std::size_t  max_;
  ChunkHeader* current_;
  LargeNode*   large_;
  CleanupNode* cleanup_;
};

}  // namespace runtime::memory
