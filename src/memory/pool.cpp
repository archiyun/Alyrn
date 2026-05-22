// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
// Reference: https://github.com/nginx/nginx/blob/master/src/core/ngx_palloc.c
#include "runtime/memory/pool.h"

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <new>

namespace runtime::memory {

namespace {

constexpr std::size_t kDefaultAlign = alignof(std::max_align_t);
constexpr std::size_t kLargeSlotSearch  = 3;

// AlignUp
inline std::byte* AlignPtr(std::byte* p, std::size_t align) noexcept {
  const auto v = reinterpret_cast<std::uintptr_t>(p);
  const auto mask = static_cast<std::uintptr_t>(align) - 1;
  return reinterpret_cast<std::byte*>((v + mask) & ~ mask);
}

} // namespace

Pool::Pool(std::size_t chunk_size)
  : d_{},
    max_{std::min(chunk_size, kMaxSmallAlloc)},
    chunk_size_{chunk_size},
    current_{&d_},
    large_{nullptr},
    cleanup_{nullptr} {

  auto* buf = static_cast<std::byte*>(
    ::operator new(chunk_size_, std::align_val_t{kDefaultAlign})
  );
  d_.last = buf;
  d_.end = buf + chunk_size_;
  d_.next = nullptr;
  d_.failed = 0;
}

Pool::~Pool() noexcept {
  Destroy();
}

void Pool::Destroy() noexcept {
  for (auto* c = cleanup_; c != nullptr; c = c->next) {
    if (c->handler != nullptr) c->handler(c->data);
  }

  // 与 AllocateLarge 的 ::operator new(size) 对齐: 不能传 align_val_t,
  // 否则 new/delete 重载不匹配 → UB.
  for (auto* l = large_; l != nullptr; l = l->next) {
    if (l->alloc != nullptr) ::operator delete(l->alloc);
  }

  // 第一块 buffer: 通过 d_.end - chunk_size_ 复原起点 (ctor 不变量).
  if (d_.end != nullptr)
    ::operator delete(d_.end - chunk_size_,
                    std::align_val_t{kDefaultAlign});

  for (auto* c = d_.next; c != nullptr; ) {
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

void* Pool::AllocateAligned(std::size_t size, std::size_t alignment) {
  return size <= max_ ? AllocateSmall(size, alignment)
                      : AllocateLarge(size);
}

// AllocateAlignec + memset 
void* Pool::Callocate(std::size_t size) {
  void* p = AllocateAligned(size, kDefaultAlign);
  if (p != nullptr) {
    std::memset(p, 0, size);
  }
  return p;
}

void Pool::Free(void* p) noexcept {
  for (LargeNode* l = large_; l != nullptr; l = l->next) {
    if (l->alloc == p) {
      ::operator delete(p);
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
  // cleanup 节点本身是从 arena bump 出来的, Reset 后整个 arena 被回收,
  // 必须把链表头清掉, 否则下次 ~Pool 会在野指针上跑 handler.
  cleanup_ = nullptr;

  d_.last = d_.end - chunk_size_;
  d_.failed = 0;

  for (ChunkHeader* c = d_.next; c != nullptr; c = c->next) {
    c->last = reinterpret_cast<std::byte*>(c) + sizeof(ChunkHeader);
    c->last = AlignPtr(c->last, kDefaultAlign);
    c->failed = 0;
  }

  current_ = &d_;
}

void* Pool::RegisterCleanup(void (*handler)(void*), std::size_t data_size) {

  CleanupNode* node = static_cast<CleanupNode*>(
    AllocateSmall(sizeof(CleanupNode), alignof(CleanupNode)
  ));
  node->handler = handler;
  node->data = (data_size > 0) ? AllocateAligned(data_size, kDefaultAlign)
                               : nullptr;
  node->next = cleanup_;
  cleanup_   = node;
  return node->data;
}

// -- metrics --

std::size_t Pool::chunk_count() const noexcept {
  std::size_t n = 1;
  for (ChunkHeader* c = d_.next; c != nullptr; c = c->next) ++n;
  return n;
}

std::size_t Pool::large_count() const noexcept {
  std::size_t n = 0;
  for (LargeNode* l = large_; l != nullptr; l = l->next) {
    if (l->alloc != nullptr) ++n;
  }
  return n;
}

std::size_t Pool::bytes_used() const noexcept {
  // 第一块 buffer 起点固定: d_.end - chunk_size_, 无 header padding.
  std::size_t used = static_cast<std::size_t>(d_.last - (d_.end - chunk_size_));

  // 后续 chunk: header 后要按 kDefaultAlign 对齐, 跟 AllocateChunk 一致.
  for (ChunkHeader* c = d_.next; c != nullptr; c = c->next) {
    auto* buf_start = AlignPtr(
        reinterpret_cast<std::byte*>(c) + sizeof(ChunkHeader),
        kDefaultAlign);
    used += static_cast<std::size_t>(c->last - buf_start);
  }
  return used;
}


// -- internals --

void* Pool::AllocateSmall(std::size_t size, std::size_t align) {
  for (ChunkHeader* c = current_; /* void */; c = c->next) {
    std::byte* aligned = AlignPtr(c->last, align);
    if (static_cast<std::size_t>(c->end - aligned) >= size) {
      c->last = aligned + size;
      return aligned;
    }
    if (c->next == nullptr) {
      break;
    }
  }

  ChunkHeader* fresh = AllocateChunk(chunk_size_);
  
  std::byte* aligned = AlignPtr(fresh->last, align);
  void* result = aligned;
  fresh->last = aligned + size;

  ChunkHeader* walk = current_;
  for (; walk->next != nullptr; walk = walk->next) {
    if (walk->failed++ >= kFailedThreshold) {
      current_ = walk->next;
    }
  }
  walk->next = fresh;
  return result;
}

Pool::ChunkHeader* Pool::AllocateChunk(std::size_t size) {
  auto* raw = static_cast<std::byte*>(
    ::operator new(sizeof(ChunkHeader) + size,
                  std::align_val_t{kDefaultAlign})
  );

  auto* hdr      = reinterpret_cast<ChunkHeader*>(raw);
  std::byte* buf = AlignPtr(raw + sizeof(ChunkHeader), kDefaultAlign);

  hdr->last   = buf;
  hdr->end    = raw + sizeof(ChunkHeader) + size;
  hdr->next   = nullptr;
  hdr->failed = 0;
  return hdr;
}
void* Pool::AllocateLarge(std::size_t size) {
  void* alloc = ::operator new(size);
  
  int probe = 0;
  for (LargeNode* l = large_; l != nullptr; l = l->next) {
    if (l->alloc == nullptr) {
      l->alloc = alloc;
      return alloc;
    }
    if (++probe >= kLargeSlotSearch) break;
  }
  auto* node = static_cast<LargeNode*>(
    AllocateSmall(sizeof(LargeNode), alignof(LargeNode))
  );
  node->alloc = alloc;
  node->next  = large_;
  large_      = node;
  return alloc;
}
} // namespace runtime::memory