// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
// Referecnce: https://github.com/nginx/nginx/blob/master/src/core/ngx_palloc.h
#pragma once

#include "runtime/base/noncopyable.h"  // IWYU pragma: keep

#include <cstdint>
#include <cstddef>

namespace runtime::memory {

class Pool : public runtime::base::NonCopyable {
public:
  inline static constexpr std::size_t  kDefaultChunkSize = 1 << 12;
  inline static constexpr std::size_t  kMaxSmallAlloc    = kDefaultChunkSize - 1;
  inline static constexpr std::size_t  kFailedThreshold  = 4;
  // 最小 chunk: 至少要装下后续 chunk 的 ChunkHeader + 几个 LargeNode/CleanupNode,
  // 否则后续 chunk 几乎没有可用空间. 对标 nginx NGX_MIN_POOL_SIZE (~112B).
  inline static constexpr std::size_t  kMinChunkSize     = 128;

  explicit Pool(std::size_t chunk_size = kDefaultChunkSize);
  ~Pool() noexcept;

  void* Allocate(std::size_t size);

  void* AllocateAligned(std::size_t size, std::size_t align);

  void* AllocateUnaligned(std::size_t size);

  void* Callocate(std::size_t size);

  void Free(void* p) noexcept;

  void Reset() noexcept;

  void* RegisterCleanup(void (*handler)(void*), std::size_t data_size);

  // metrics
  std::size_t chunk_count() const noexcept;
  std::size_t large_count() const noexcept;
  std::size_t bytes_used()  const noexcept;

private:
  struct ChunkHeader {
    std::byte*    last; // bump start_point
    std::byte*    end; // chunk endpoint
    ChunkHeader*  next; // next chunk
    std::uint32_t failed; // bump
  };

  struct LargeNode {
    void*       alloc; // operator new Large chunk
    LargeNode*  next;
  };

  struct CleanupNode {
    void (*handler)(void*);
    void*         data;
    CleanupNode*  next;
  };
private:
  // 后续 chunk 的预留字节数 (ChunkHeader + 对齐 padding). 定义在 .cpp,
  // 因为依赖 .cpp 内的 kDefaultAlign 常量, 且与 AllocateChunk 共享同一布局.
  static const std::size_t kHeaderReserve;

  void Destroy() noexcept;
  void* AllocateSmall(std::size_t size, std::size_t alignment);
  void* AllocateLarge(std::size_t size);
  ChunkHeader* AllocateChunk(std::size_t size);
private:
  ChunkHeader   d_;
  std::size_t   max_;
  std::size_t   chunk_size_;
  ChunkHeader*  current_;
  LargeNode*    large_;
  CleanupNode*  cleanup_;
};

} // namespace runtime::memory