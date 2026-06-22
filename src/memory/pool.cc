// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
// Reference: https://github.com/nginx/nginx/blob/master/src/core/ngx_palloc.c
#include "vexo/memory/pool.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>

namespace vexo::memory {

namespace {

constexpr std::size_t kDefaultAlign    = alignof(std::max_align_t);
constexpr std::size_t kLargeSlotSearch = 3;

// Align pointer address upward to the next `align` boundary.
// such as, p:100 -> 112 when align = 16.
inline std::byte* AlignPtr(std::byte* p, std::size_t align) noexcept {
  const auto v    = reinterpret_cast<std::uintptr_t>(p);
  const auto mask = static_cast<std::uintptr_t>(align) - 1;
  return reinterpret_cast<std::byte*>((v + mask) & ~mask);
}

[[noreturn]] void AbortWithReason(const char* msg) noexcept {
  std::fprintf(stderr, "[vexo::memory::Pool] %s\n", msg);
  std::abort();
}

}  // namespace

// Ensure the minimum chunk size can hold the Pool header
// plus at least two LargeNode-sized allocations.
// Based on Nginx.
static_assert(
    Pool::kMinChunkSize >= sizeof(Pool) + 2 * sizeof(void*) * 2,
    "kMinChunkSize must fit Pool + 2 LargeNode slots");


Pool::Ptr Pool::Create(std::size_t chunk_size) {
  if (chunk_size < kMinChunkSize) {
    AbortWithReason("Pool::Create: chunk_size below kMinChunkSize");
  }

  void* raw  = ::operator new(chunk_size, std::align_val_t{kDefaultAlign});
  auto* pool = ::new (raw) Pool(chunk_size);
  return Ptr{pool, Deleter{}};
}

void Pool::Deleter::operator()(Pool* p) const noexcept {
  if (p == nullptr) return;
  p->DestroyArena();
  p->~Pool();
  ::operator delete(static_cast<void*>(p),
                    std::align_val_t{kDefaultAlign});
}

Pool::Pool(std::size_t chunk_size) noexcept
    : d_{}, max_{}, current_{}, large_{nullptr}, cleanup_{nullptr} {
  // like nginx:
  //   p->d.last = (u_char*)p + sizeof(ngx_pool_t);
  //   p->d.end  = (u_char*)p + size;
  //   size      = size - sizeof(ngx_pool_t);
  //   p->max    = min(size, NGX_MAX_ALLOC_FROM_POOL);

  // (this) == &d_
  auto* start = reinterpret_cast<std::byte*>(this);
  d_.last     = start + sizeof(Pool);
  d_.end      = start + chunk_size;
  d_.next     = nullptr;
  d_.failed   = 0;

  const std::size_t usable = chunk_size - sizeof(Pool);
  max_     = std::min(usable, kMaxSmallAlloc);
  current_ = &d_;
}

// order must : cleanup -> large -> chunk chain -> pool itself
void Pool::DestroyArena() noexcept {
  for (auto* c = cleanup_; c != nullptr; c = c->next) {
    if (c->handler != nullptr) c->handler(c->data);
  }

  for (auto* l = large_; l != nullptr; l = l->next) {
    if (l->alloc != nullptr) ::operator delete(l->alloc);
  }

  // Extra chunks embed their ChunkHeader at the beginning of
  // their own allocation block.
  //
  // The first chunk is released separately by Deleter after ~Pool().
  for (auto* c = d_.next; c != nullptr;) {
    auto* next = c->next;
    ::operator delete(c, std::align_val_t{kDefaultAlign});
    c = next;
  }
}

void* Pool::Allocate(std::size_t size) {
  return AllocateAligned(size, kDefaultAlign);
}

void* Pool::AllocateUnaligned(std::size_t size) {
  return AllocateAligned(size, 1);
}

void* Pool::AllocateAligned(std::size_t size, std::size_t align) {
  return size <= max_ ? AllocateSmall(size, align)
                      : AllocateLarge(size);
}

void* Pool::Callocate(std::size_t size) {
  void* p = AllocateAligned(size, kDefaultAlign);
  if (p != nullptr) std::memset(p, 0, size);
  return p;
}

void Pool::Free(void* p) noexcept {
  for (LargeNode* l = large_; l != nullptr; l = l->next) {
    if (l->alloc == p) {
      ::operator delete(l->alloc);
      l->alloc = nullptr;
      return;
    }
  }
}

void Pool::Reset() noexcept {
  for (LargeNode* l = large_; l != nullptr; l = l->next) {
    if (l->alloc != nullptr) {
      ::operator delete(l->alloc);
      l->alloc = nullptr;
    }
  }
  large_   = nullptr;
  cleanup_ = nullptr;

  auto* start = reinterpret_cast<std::byte*>(this);
  d_.last     = start + sizeof(Pool);
  d_.failed   = 0;

  for (ChunkHeader* c = d_.next; c != nullptr; c = c->next) {
    c->last = AlignPtr(
        reinterpret_cast<std::byte*>(c) + sizeof(ChunkHeader),
        kDefaultAlign);
    c->failed = 0;
  }
  current_ = &d_;
}

void* Pool::RegisterCleanup(void (*handler)(void*), std::size_t data_size) {
  const std::size_t total = sizeof(CleanupNode) + data_size;
  auto* mem = static_cast<std::byte*>(
      AllocateAligned(total, kDefaultAlign));
  if (mem == nullptr) return nullptr;

  auto* node    = reinterpret_cast<CleanupNode*>(mem);
  node->handler = handler;
  node->data    = (data_size > 0) ? mem + sizeof(CleanupNode) : nullptr;
  node->next    = cleanup_;
  cleanup_      = node;  // 头插 -> ~Pool 时 LIFO
  return node->data;
}

std::size_t Pool::chunk_count() const noexcept {
  std::size_t n = 1;  // First chunk
  for (auto* c = d_.next; c != nullptr; c = c->next) ++n;
  return n;
}

std::size_t Pool::large_count() const noexcept {
  std::size_t n = 0;
  for (auto* l = large_; l != nullptr; l = l->next) {
    if (l->alloc != nullptr) ++n;
  }
  return n;
}

std::size_t Pool::ByteUsed() const noexcept {
  auto* first_payload =
      reinterpret_cast<const std::byte*>(this) + sizeof(Pool);
  std::size_t used = static_cast<std::size_t>(d_.last - first_payload);

  for (auto* c = d_.next; c != nullptr; c = c->next) {
    auto* buf = AlignPtr(
        reinterpret_cast<std::byte*>(const_cast<ChunkHeader*>(c)) +
            sizeof(ChunkHeader),
        kDefaultAlign);
    used += static_cast<std::size_t>(c->last - buf);
  }
  return used;
}

void* Pool::AllocateSmall(std::size_t size, std::size_t align) {
  for (ChunkHeader* c = current_; /* void */; c = c->next) {
    std::byte* aligned = AlignPtr(c->last, align);
    if (aligned <= c->end &&
        static_cast<std::size_t>(c->end - aligned) >= size) {
      c->last = aligned + size;
      return aligned;
    }
    if (c->next == nullptr) break;
  }

  // No existing chunk has enough space. Allocate a new chunk.
  ChunkHeader* fresh = AllocateChunk();
  std::byte* aligned = AlignPtr(fresh->last, align);
  void* result = aligned;
  fresh->last  = aligned + size;

  // nginx-style heuristic:
  // increment failed counters for skipped chunks and
  // gradually advance current_ toward newer chunks.
  ChunkHeader* walk = current_;
  for (; walk->next != nullptr; walk = walk->next) {
    if (walk->failed++ >= kFailedThreshold) {
      current_ = walk->next;
    }
  }
  walk->next = fresh;
  return result;
}

void* Pool::AllocateLarge(std::size_t size) {
  void* alloc = ::operator new(size);

  std::size_t probe = 0;
  for (LargeNode* l = large_; l != nullptr; l = l->next) {
    if (l->alloc == nullptr) {
      l->alloc = alloc;
      return alloc;
    }
    if (++probe >= kLargeSlotSearch) break;
  }

  auto* node = static_cast<LargeNode*>(
      AllocateSmall(sizeof(LargeNode), alignof(LargeNode)));
  node->alloc = alloc;
  node->next  = large_;
  large_      = node;
  return alloc;
}

Pool::ChunkHeader* Pool::AllocateChunk() {
  const std::size_t psize = static_cast<std::size_t>(
      d_.end - reinterpret_cast<std::byte*>(this));

  auto* raw = static_cast<std::byte*>(
      ::operator new(psize, std::align_val_t{kDefaultAlign}));

  auto* hdr      = reinterpret_cast<ChunkHeader*>(raw);
  hdr->last   = AlignPtr(raw + sizeof(ChunkHeader), kDefaultAlign);
  hdr->end    = raw + psize;
  hdr->next   = nullptr;
  hdr->failed = 0;
  return hdr;
}

}  // namespace vexo::memory
