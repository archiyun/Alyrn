// SPDX-License-Identifier: MIT
//
// Optional coroutine-frame allocation through a std::pmr::memory_resource.
// The default resource is std::pmr::new_delete_resource(), so existing code
// keeps heap-backed frame allocation unless a scope or Scheduler selects a
// different resource.
#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <memory_resource>
#include <new>
#include <thread>

#include "coropact/base/check.h"
#include "coropact/utils/macros.h"

namespace coropact::coro {
namespace detail {

inline thread_local std::pmr::memory_resource* current_frame_resource =
    std::pmr::new_delete_resource();

inline std::pmr::memory_resource* CurrentFrameResource() noexcept { return current_frame_resource; }

inline std::pmr::memory_resource* NormalizeFrameResource(
    std::pmr::memory_resource* resource) noexcept {
  return resource != nullptr ? resource : std::pmr::new_delete_resource();
}

inline void SetCurrentFrameResource(std::pmr::memory_resource* resource) noexcept {
  current_frame_resource = NormalizeFrameResource(resource);
}

// The coroutine frame contains no user-visible pointer to the allocator. Keep
// the allocation metadata immediately before the frame and recover it from a
// marker slot during promise destruction. The header is constructed at the
// original allocation address, so the header pointer is also the address that
// must be passed back to the resource.
//
// A frame allocation only needs the resource, the exact byte count, and the
// exact alignment for deallocation. On the supported 64-bit targets, keep the
// latter two values in one word: the low 60 bits hold bytes and the high four
// bits hold log2(alignment).
static_assert(sizeof(std::size_t) == sizeof(std::uint64_t),
              "packed frame metadata requires 64-bit size_t");

inline constexpr unsigned kFrameMetadataAlignmentBits = 4;
inline constexpr unsigned kFrameMetadataBytesBits =
    std::numeric_limits<std::uint64_t>::digits - kFrameMetadataAlignmentBits;  // 60
inline constexpr std::uint64_t kFrameMetadataBytesMask =
    (std::uint64_t{1} << kFrameMetadataBytesBits) - 1;  // 15
inline constexpr unsigned kFrameMetadataMaxAlignmentExponent =
    (1 << kFrameMetadataAlignmentBits) - 1;  // 15, encoding alignments up to 2^15

inline std::uint64_t PackFrameMetadata(std::size_t bytes, std::size_t alignment) {
  COROPACT_CHECK(bytes <= kFrameMetadataBytesMask,
                 "coroutine frame metadata cannot encode this allocation size");
  COROPACT_CHECK(alignment != 0 && std::has_single_bit(alignment),
                 "coroutine frame metadata requires a power-of-two alignment");
  const auto exponent = static_cast<unsigned>(std::countr_zero(alignment));
  COROPACT_CHECK(exponent <= kFrameMetadataMaxAlignmentExponent,
                 "coroutine frame metadata cannot encode this alignment");
  return static_cast<std::uint64_t>(bytes) |
         (static_cast<std::uint64_t>(exponent) << kFrameMetadataBytesBits);
}

inline std::size_t UnpackFrameBytes(std::uint64_t metadata) noexcept {
  return static_cast<std::size_t>(metadata & kFrameMetadataBytesMask);
}

inline std::size_t UnpackFrameAlignment(std::uint64_t metadata) noexcept {
  const auto exponent = static_cast<unsigned>(metadata >> kFrameMetadataBytesBits);
  return std::size_t{1} << exponent;
}

struct FrameAllocationHeader {
  std::pmr::memory_resource* resource;
  std::uint64_t metadata;
};

static_assert(sizeof(FrameAllocationHeader) == 2 * sizeof(void*));

inline constexpr std::size_t FrameAllocationMarkerSize = sizeof(FrameAllocationHeader*);

// Heap and over-aligned frames keep a prefix header. Pooled frames are raw
// size-class slots; their definitions live after CoroFramePoolResource so they
// can consult the slab map.
[[nodiscard]]
inline void* AllocateFrame(std::size_t frame_size, std::size_t frame_alignment);
inline void DeallocateFrame(void* frame) noexcept;

// Inherited by every promise type that owns a coroutine frame. The allocation
// operators are deliberately centralized so Task, SpawnRoot, DetachedTask,
// and SyncWaitRoot all honor the same selected resource.
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

// A worker-local size-class allocator for coroutine frames. Allocate and
// same-thread free stay on a plain intrusive list. A foreign thread only CAS
// onto that class's remote stack; the owner later steals the whole chain.
//
// Each chunk is a 64 KiB-aligned slab. Owner and size class live on the slab
// header, shared by every slot. Deallocate looks the pointer up in a two-level
// slab map (never by masking and dereferencing): heap frames can share the
// process with pooled slots, and a masked address is not guaranteed to be
// mapped. Pooled coroutine frames therefore carry no per-frame header.
class CoroFramePoolResource final : public std::pmr::memory_resource {
public:
  static constexpr std::size_t kSizeClassCount = 13;
  static constexpr std::size_t kChunkBytes = 64 * 1024;
  static constexpr std::size_t kSlabHeaderBytes = 64;
  static constexpr std::size_t kMaxPooledBytes = 16 * 1024;
  inline static constexpr std::array<std::size_t, kSizeClassCount> kSizeClasses{
      64, 96, 128, 192, 256, 384, 512, 768, 1024, 2048, 4096, 8192, 16384};

  static_assert(std::has_single_bit(kChunkBytes), "slab lookup requires a power-of-two chunk");
  static_assert(kSlabHeaderBytes < kChunkBytes);

  [[nodiscard]]
  static constexpr std::size_t SlotsPerChunk(std::size_t block_size) noexcept {
    return (kChunkBytes - kSlabHeaderBytes) / block_size;
  }

  CoroFramePoolResource() noexcept : CoroFramePoolResource(*std::pmr::new_delete_resource()) {}

  explicit CoroFramePoolResource(std::pmr::memory_resource& upstream) noexcept
      : upstream_(&upstream) {}

  ~CoroFramePoolResource() override {
    if (current_pool_ == this) {
      current_pool_ = nullptr;
    }
    DrainRemote();
    ReleaseChunks();
  }

  COROPACT_DELETE_COPY_MOVE(CoroFramePoolResource);

  void DrainRemote() noexcept {
    for (auto& size_class : classes_) {
      DrainRemote(size_class);
    }
  }

  static void DrainCurrent() noexcept {
    if (current_pool_ != nullptr) {
      current_pool_->DrainRemote();
    }
  }

  [[nodiscard]]
  static void* TryAllocateCurrent(std::size_t frame_size, std::size_t alignment) {
    auto* pool = dynamic_cast<CoroFramePoolResource*>(detail::CurrentFrameResource());
    if (pool == nullptr) {
      return nullptr;
    }
    return pool->TryAllocateFrame(frame_size, alignment);
  }

  static bool TryDeallocate(void* frame) noexcept {
    if (frame == nullptr) {
      return false;
    }
    SlabHeader* slab = LookupSlab(frame);
    if (slab == nullptr || !IsSlot(slab, frame)) {
      return false;
    }
    slab->owner->FreeSlot(frame, slab);
    return true;
  }

private:
  struct FreeNode {
    FreeNode* next;
  };

  struct SlabHeader {
    SlabHeader* next;
    CoroFramePoolResource* owner;
    std::uint32_t size_class_index;
    std::uint32_t slot_size;
    std::uint32_t slot_count;
    std::uint32_t pad{0};
    std::byte reserved[kSlabHeaderBytes - 32];
  };

  static_assert(sizeof(SlabHeader) == kSlabHeaderBytes);
  static_assert(alignof(SlabHeader) <= alignof(std::max_align_t));

  // Remote CAS traffic stays on its own cache line so a foreign free does not
  // bounce the owner's local list or chunk pointer.
  struct SizeClass {
    FreeNode* free_list{nullptr};
    SlabHeader* slabs{nullptr};
    alignas(64) std::atomic<FreeNode*> remote{nullptr};
  };

  static constexpr std::size_t kSlotAlignment = alignof(std::max_align_t);
  static_assert(kSizeClassCount <= 255, "size-class table uses uint8_t indices");

  static constexpr std::array<std::uint8_t, kMaxPooledBytes + 1> kClassByBytes = [] {
    std::array<std::uint8_t, kMaxPooledBytes + 1> table{};
    for (auto& slot : table) {
      slot = static_cast<std::uint8_t>(kSizeClassCount);
    }
    std::size_t start = 1;
    for (std::uint8_t i = 0; i < kSizeClassCount; ++i) {
      for (std::size_t n = start; n <= kSizeClasses[i]; ++n) {
        table[n] = i;
      }
      start = kSizeClasses[i] + 1;
    }
    return table;
  }();

  static_assert(kClassByBytes[64] == 0);
  static_assert(kClassByBytes[96] == 1);
  static_assert(kClassByBytes[97] == 2);
  static_assert(kClassByBytes[192] == 3);
  static_assert(kClassByBytes[16384] == kSizeClassCount - 1);
  static constexpr unsigned kSlabShift = 16;
  static constexpr unsigned kL2Bits = 16;
  static constexpr unsigned kL1Bits = 16;
  static constexpr std::size_t kL2Size = std::size_t{1} << kL2Bits;
  static constexpr std::size_t kL1Size = std::size_t{1} << kL1Bits;
  static constexpr std::uintptr_t kCanonicalMask = (std::uintptr_t{1} << 48) - 1;

  struct SlabDirectory {
    std::atomic<SlabHeader*> entries[kL2Size];
  };

  inline static thread_local CoroFramePoolResource* current_pool_{nullptr};

  static void SlabMapIndex(std::uintptr_t addr, std::size_t* l1, std::size_t* l2) noexcept {
    const auto canonical = addr & kCanonicalMask;
    *l1 = static_cast<std::size_t>(canonical >> 32);
    *l2 = static_cast<std::size_t>((canonical >> kSlabShift) & (kL2Size - 1));
  }

  static std::atomic<SlabDirectory*>* Directories() {
    static auto* tables = new std::atomic<SlabDirectory*>[kL1Size]();
    return tables;
  }

  static void RegisterSlab(SlabHeader* slab) {
    const auto addr = reinterpret_cast<std::uintptr_t>(slab);
    COROPACT_CHECK((addr & (kChunkBytes - 1)) == 0, "frame slab must be chunk-aligned");
    std::size_t l1 = 0;
    std::size_t l2 = 0;
    SlabMapIndex(addr, &l1, &l2);
    COROPACT_CHECK(l1 < kL1Size, "frame slab address exceeds the slab map");

    auto& slot = Directories()[l1];
    SlabDirectory* directory = slot.load(std::memory_order_acquire);
    if (directory == nullptr) {
      auto* created = new SlabDirectory{};
      SlabDirectory* expected = nullptr;
      if (slot.compare_exchange_strong(expected, created, std::memory_order_release,
                                       std::memory_order_acquire)) {
        directory = created;
      } else {
        delete created;
        directory = expected;
      }
    }

    SlabHeader* previous = nullptr;
    COROPACT_CHECK(directory->entries[l2].compare_exchange_strong(
                       previous, slab, std::memory_order_release, std::memory_order_relaxed),
                   "frame slab already registered");
  }

  static void UnregisterSlab(SlabHeader* slab) noexcept {
    const auto addr = reinterpret_cast<std::uintptr_t>(slab);
    std::size_t l1 = 0;
    std::size_t l2 = 0;
    SlabMapIndex(addr, &l1, &l2);
    if (l1 >= kL1Size) {
      return;
    }
    SlabDirectory* directory = Directories()[l1].load(std::memory_order_acquire);
    if (directory == nullptr) {
      return;
    }
    directory->entries[l2].store(nullptr, std::memory_order_release);
  }

  [[nodiscard]]
  static SlabHeader* LookupSlab(void* pointer) noexcept {
    const auto addr = reinterpret_cast<std::uintptr_t>(pointer);
    std::size_t l1 = 0;
    std::size_t l2 = 0;
    SlabMapIndex(addr, &l1, &l2);
    if (l1 >= kL1Size) {
      return nullptr;
    }
    SlabDirectory* directory = Directories()[l1].load(std::memory_order_acquire);
    if (directory == nullptr) {
      return nullptr;
    }
    return directory->entries[l2].load(std::memory_order_acquire);
  }

  [[nodiscard]]
  static bool IsSlot(const SlabHeader* slab, void* pointer) noexcept {
    auto* base = reinterpret_cast<const std::byte*>(slab);
    auto* slot = static_cast<const std::byte*>(pointer);
    auto* first = base + kSlabHeaderBytes;
    if (slot < first) {
      return false;
    }
    const auto offset = static_cast<std::size_t>(slot - first);
    if (slab->slot_size == 0 || offset % slab->slot_size != 0) {
      return false;
    }
    return (offset / slab->slot_size) < slab->slot_count;
  }

  [[nodiscard]]
  std::size_t FindSizeClass(std::size_t bytes, std::size_t alignment) const noexcept {
    if (bytes == 0 || bytes > kMaxPooledBytes || alignment > kSlotAlignment) {
      return kSizeClassCount;
    }
    return kClassByBytes[bytes];
  }

  [[nodiscard]]
  bool IsOwnerThread() const noexcept {
    return owner_thread_ == std::this_thread::get_id();
  }

  void BindOwnerThread() noexcept {
    if (owner_thread_ == std::thread::id{}) {
      owner_thread_ = std::this_thread::get_id();
    }
    if (IsOwnerThread()) {
      current_pool_ = this;
    }
  }

  void AddChunk(SizeClass& size_class, std::size_t index) {
    const std::size_t block_size = kSizeClasses[index];
    const std::size_t slot_count = SlotsPerChunk(block_size);
    COROPACT_CHECK(slot_count > 0, "frame size class does not fit in a slab");

    void* allocation = upstream_->allocate(kChunkBytes, kChunkBytes);
    COROPACT_CHECK((reinterpret_cast<std::uintptr_t>(allocation) & (kChunkBytes - 1)) == 0,
                   "upstream did not honor slab alignment");

    auto* slab =
        ::new (allocation) SlabHeader{.next = size_class.slabs,
                                      .owner = this,
                                      .size_class_index = static_cast<std::uint32_t>(index),
                                      .slot_size = static_cast<std::uint32_t>(block_size),
                                      .slot_count = static_cast<std::uint32_t>(slot_count),
                                      .pad = 0,
                                      .reserved = {}};
    RegisterSlab(slab);
    size_class.slabs = slab;

    auto* slot = static_cast<std::byte*>(allocation) + kSlabHeaderBytes;
    for (std::size_t i = 0; i < slot_count; ++i) {
      auto* node = reinterpret_cast<FreeNode*>(slot + i * block_size);
      node->next = size_class.free_list;
      size_class.free_list = node;
    }
  }

  void DrainRemote(SizeClass& size_class) noexcept {
    if (size_class.remote.load(std::memory_order_relaxed) == nullptr) {
      return;
    }
    FreeNode* list = size_class.remote.exchange(nullptr, std::memory_order_acquire);
    while (list != nullptr) {
      FreeNode* next = list->next;
      list->next = size_class.free_list;
      size_class.free_list = list;
      list = next;
    }
  }

  void PushRemote(SizeClass& size_class, FreeNode* node) noexcept {
    FreeNode* old = size_class.remote.load(std::memory_order_relaxed);
    do {
      node->next = old;
    } while (!size_class.remote.compare_exchange_weak(old, node, std::memory_order_release,
                                                      std::memory_order_relaxed));
  }

  void FreeSlot(void* pointer, SlabHeader* slab) noexcept {
    auto* node = static_cast<FreeNode*>(pointer);
    SizeClass& size_class = classes_[slab->size_class_index];
    if (IsOwnerThread()) {
      node->next = size_class.free_list;
      size_class.free_list = node;
      return;
    }
    PushRemote(size_class, node);
  }

  void ReleaseChunks() noexcept {
    for (auto& size_class : classes_) {
      for (SlabHeader* slab = size_class.slabs; slab != nullptr;) {
        SlabHeader* next = slab->next;
        UnregisterSlab(slab);
        void* allocation = slab;
        slab->~SlabHeader();
        upstream_->deallocate(allocation, kChunkBytes, kChunkBytes);
        slab = next;
      }
      size_class.free_list = nullptr;
      size_class.slabs = nullptr;
      size_class.remote.store(nullptr, std::memory_order_relaxed);
    }
  }

  [[nodiscard]]
  void* TryAllocateFrame(std::size_t frame_size, std::size_t alignment) {
    return AllocatePooled(frame_size, alignment);
  }

  [[nodiscard]]
  void* AllocatePooled(std::size_t bytes, std::size_t alignment) {
    BindOwnerThread();
    const std::size_t index = FindSizeClass(bytes, alignment);
    if (index == kSizeClassCount) {
      return nullptr;
    }

    SizeClass& size_class = classes_[index];
    if (size_class.free_list == nullptr) {
      DrainRemote(size_class);
    }
    if (size_class.free_list == nullptr) {
      AddChunk(size_class, index);
    }
    FreeNode* node = size_class.free_list;
    size_class.free_list = node->next;
    return node;
  }

  void* do_allocate(std::size_t bytes, std::size_t alignment) override {
    if (void* pooled = AllocatePooled(bytes, alignment)) {
      return pooled;
    }
    return upstream_->allocate(bytes, alignment);
  }

  void do_deallocate(void* pointer, std::size_t bytes, std::size_t alignment) noexcept override {
    if (SlabHeader* slab = LookupSlab(pointer); slab != nullptr && IsSlot(slab, pointer)) {
      slab->owner->FreeSlot(pointer, slab);
      return;
    }
    upstream_->deallocate(pointer, bytes, alignment);
  }

  bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override {
    return this == &other;
  }

  std::pmr::memory_resource* upstream_;
  std::thread::id owner_thread_{};
  std::array<SizeClass, kSizeClassCount> classes_{};
};

namespace detail {

inline void* AllocateHeapFrame(std::size_t frame_size, std::size_t frame_alignment) {
  const std::size_t alignment =
      std::max({frame_alignment, alignof(FrameAllocationHeader), alignof(FrameAllocationHeader*)});
  const std::size_t prefix = sizeof(FrameAllocationHeader) + FrameAllocationMarkerSize;
  if (frame_size > std::numeric_limits<std::size_t>::max() - prefix ||
      alignment > std::numeric_limits<std::size_t>::max() - (prefix + frame_size)) {
    throw std::bad_alloc();
  }

  const std::size_t bytes = prefix + frame_size + alignment - 1;
  const auto alignment_exponent = static_cast<unsigned>(std::countr_zero(alignment));
  if (bytes > kFrameMetadataBytesMask || !std::has_single_bit(alignment) ||
      alignment_exponent > kFrameMetadataMaxAlignmentExponent) {
    throw std::bad_alloc();
  }

  auto* resource = CurrentFrameResource();
  void* allocation = resource->allocate(bytes, alignment);

  auto* header =
      ::new (allocation) FrameAllocationHeader{resource, PackFrameMetadata(bytes, alignment)};
  void* frame = static_cast<std::byte*>(allocation) + prefix;
  std::size_t available = bytes - prefix;
  if (std::align(frame_alignment, frame_size, frame, available) == nullptr) {
    header->~FrameAllocationHeader();
    resource->deallocate(allocation, bytes, alignment);
    throw std::bad_alloc();
  }

  auto** marker = reinterpret_cast<FrameAllocationHeader**>(static_cast<std::byte*>(frame) -
                                                            FrameAllocationMarkerSize);
  *marker = header;
  return frame;
}

inline void* AllocateFrame(std::size_t frame_size, std::size_t frame_alignment) {
  if (void* pooled = CoroFramePoolResource::TryAllocateCurrent(frame_size, frame_alignment)) {
    return pooled;
  }
  return AllocateHeapFrame(frame_size, frame_alignment);
}

inline void DeallocateFrame(void* frame) noexcept {
  if (CoroFramePoolResource::TryDeallocate(frame)) {
    return;
  }
  if (frame == nullptr) {
    return;
  }

  auto** marker = reinterpret_cast<FrameAllocationHeader**>(static_cast<std::byte*>(frame) -
                                                            FrameAllocationMarkerSize);
  FrameAllocationHeader* header = *marker;
  auto* resource = header->resource;
  void* allocation = header;
  const std::size_t bytes = UnpackFrameBytes(header->metadata);
  const std::size_t alignment = UnpackFrameAlignment(header->metadata);
  header->~FrameAllocationHeader();
  resource->deallocate(allocation, bytes, alignment);
}

}  // namespace detail

// Selects the resource used while coroutine frames are created. Heap frames
// store that resource in a prefix header. Pooled frames recover owner and size
// class from their slab, so destruction stays correct after this scope ends.
class FrameAllocatorScope {
public:
  COROPACT_DELETE_COPY(FrameAllocatorScope);

  explicit FrameAllocatorScope(std::pmr::memory_resource& resource) noexcept
      : FrameAllocatorScope(&resource) {}

  explicit FrameAllocatorScope(std::pmr::memory_resource* resource) noexcept
      : previous_(detail::CurrentFrameResource()),
        changed_(previous_ != detail::NormalizeFrameResource(resource)) {
    if (changed_) {
      detail::SetCurrentFrameResource(resource);
    }
  }

  ~FrameAllocatorScope() {
    if (changed_) {
      detail::SetCurrentFrameResource(previous_);
    }
  }

  [[nodiscard]]
  static std::pmr::memory_resource* TryCurrent() noexcept {
    return detail::CurrentFrameResource();
  }

private:
  std::pmr::memory_resource* previous_;
  bool changed_;
};

}  // namespace coropact::coro
