// SPDX-License-Identifier: MIT
#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <new>

#include "alyrn/detail/check.h"
#include "alyrn/detail/macros.h"

namespace alyrn::detail {

template <typename T, std::size_t kCapacity, typename MutexPolicy>
class ObjectPool;

// NullMutex satisfies BasicLockable with zero overhead.
// Use as MutexPolicy for single-threaded or benchmark scenarios.
struct NullMutex {
  void lock() noexcept {}
  void unlock() noexcept {}
};

// Thread-safe fixed-size raw memory pool.
//
// Template parameters:
//   BlockSize   - Size in bytes of each allocation slot.
//   Alignment   - Required alignment of each slot (default: max platform alignment).
//   kCapacity   - Maximum number of slots (default: 1024).
//   MutexPolicy - Lock type; must satisfy BasicLockable (default: std::mutex).
//                 Use NullMutex for single-threaded use.
//
// The pool only manages raw bytes — object construction and destruction are
// the responsibility of the caller (or ObjectPool).
//
// Example:
//   using Pool = MemoryPool<sizeof(MyClass), alignof(MyClass), 128>;
//   Pool pool;
//   void* mem = pool.Allocate();
//   auto* obj = new (mem) MyClass(args...);
//   obj->~MyClass();
//   pool.Deallocate(mem);
template <std::size_t BlockSize, std::size_t Alignment = alignof(std::max_align_t),
          std::size_t kCapacity = 1024, typename MutexPolicy = std::mutex>
class MemoryPool {
  static_assert(BlockSize > 0, "MemoryPool: BlockSize must be > 0");
  static_assert(kCapacity > 0, "MemoryPool: kCapacity must be > 0");
  static_assert(Alignment > 0 && (Alignment & (Alignment - 1)) == 0,
                "MemoryPool: Alignment must be a power of two");

public:
  MemoryPool() { Initialize(); }

  ~MemoryPool() { ::operator delete(buffer_, std::align_val_t{kAlignment}); }

  ALYRN_DELETE_COPY_MOVE(MemoryPool);

  // Allocates one slot from the pool.
  // Returns nullptr if the pool is exhausted.
  void* Allocate() noexcept {
    std::lock_guard<MutexPolicy> lock{mutex_};
    if (free_list_head_ == nullptr) {
      return nullptr;
    }

    void* ptr = free_list_head_;
    free_list_head_ = *static_cast<void**>(free_list_head_);
    const std::size_t slot = SlotIndex(ptr);
    ALYRN_CHECK(!SlotAllocated(slot) && !SlotReleasing(slot),
                "MemoryPool::Allocate: duplicate free-list slot");
    MarkSlotAllocated(slot);
    --free_count_;
    return ptr;
  }

  // Returns a slot back to the pool. Passing nullptr is a no-op. A foreign or
  // already-free pointer is a memory-safety violation and terminates.
  void Deallocate(void* ptr) noexcept {
    if (ptr == nullptr) {
      return;
    }
    ALYRN_CHECK(Owns(ptr), "MemoryPool::Deallocate: foreign pointer");

    std::lock_guard<MutexPolicy> lock{mutex_};
    ClaimDeallocationLocked(ptr);
    FinishDeallocationLocked(ptr);
  }

  // Returns the maximum number of slots in the pool.
  constexpr std::size_t Capacity() const noexcept { return kCapacity; }

  // Returns the number of currently free slots.
  std::size_t FreeCount() const noexcept {
    std::lock_guard<MutexPolicy> lock(mutex_);
    return free_count_;
  }

  // Returns the number of currently allocated slots.
  std::size_t UsedCount() const noexcept {
    std::lock_guard<MutexPolicy> lock(mutex_);
    return kCapacity - free_count_;
  }

  // Returns true if ptr was allocated from this pool.
  bool Owns(const void* ptr) const noexcept {
    if (ptr == nullptr || buffer_ == nullptr) {
      return false;
    }

    const auto begin = reinterpret_cast<std::uintptr_t>(buffer_);
    const auto end = static_cast<std::uintptr_t>(begin + (kSlotSize * kCapacity));
    const auto p = reinterpret_cast<std::uintptr_t>(ptr);

    if (p < begin || p >= end) {
      return false;
    }

    return (p - begin) % kSlotSize == 0;
  }

private:
  template <typename, std::size_t, typename>
  friend class ObjectPool;

  // Effective alignment: must fit at least one void* for the intrusive free list.
  static constexpr std::size_t kAlignment = std::max(Alignment, alignof(void*));

  // Effective slot size: must be large enough for the free-list void* pointer,
  // then rounded up to the alignment boundary.
  static constexpr std::size_t kRawSlotSize = std::max(BlockSize, sizeof(void*));
  static_assert(kRawSlotSize <= std::numeric_limits<std::size_t>::max() - (kAlignment - 1),
                "MemoryPool: slot-size alignment would overflow");
  static constexpr std::size_t kSlotSize =
      ((kRawSlotSize + kAlignment - 1) / kAlignment) * kAlignment;
  static_assert(kSlotSize <= std::numeric_limits<std::size_t>::max() / kCapacity,
                "MemoryPool: total storage size would overflow");
  static constexpr std::size_t kStateWordBits = 64;
  static constexpr std::size_t kStateWordCount = (kCapacity + kStateWordBits - 1) / kStateWordBits;

  std::size_t SlotIndex(const void* ptr) const noexcept {
    const auto begin = reinterpret_cast<std::uintptr_t>(buffer_);
    const auto address = reinterpret_cast<std::uintptr_t>(ptr);
    return static_cast<std::size_t>((address - begin) / kSlotSize);
  }

  bool SlotAllocated(std::size_t slot) const noexcept {
    const std::size_t word = slot / kStateWordBits;
    const std::size_t bit = slot % kStateWordBits;
    return (allocated_slots_[word] & (std::uint64_t{1} << bit)) != 0;
  }

  bool SlotReleasing(std::size_t slot) const noexcept {
    const std::size_t word = slot / kStateWordBits;
    const std::size_t bit = slot % kStateWordBits;
    return (releasing_slots_[word] & (std::uint64_t{1} << bit)) != 0;
  }

  void MarkSlotAllocated(std::size_t slot) noexcept {
    const std::size_t word = slot / kStateWordBits;
    const std::size_t bit = slot % kStateWordBits;
    ALYRN_CHECK(!SlotReleasing(slot), "MemoryPool::Allocate: releasing slot reused");
    allocated_slots_[word] |= std::uint64_t{1} << bit;
  }

  void MarkSlotReleasing(std::size_t slot) noexcept {
    const std::size_t word = slot / kStateWordBits;
    const std::size_t bit = slot % kStateWordBits;
    allocated_slots_[word] &= ~(std::uint64_t{1} << bit);
    releasing_slots_[word] |= std::uint64_t{1} << bit;
  }

  void MarkSlotFree(std::size_t slot) noexcept {
    const std::size_t word = slot / kStateWordBits;
    const std::size_t bit = slot % kStateWordBits;
    releasing_slots_[word] &= ~(std::uint64_t{1} << bit);
  }

  void ClaimDeallocationLocked(void* ptr) noexcept {
    const std::size_t slot = SlotIndex(ptr);
    ALYRN_CHECK(SlotAllocated(slot), "MemoryPool::Deallocate: double free");
    MarkSlotReleasing(slot);
  }

  void FinishDeallocationLocked(void* ptr) noexcept {
    const std::size_t slot = SlotIndex(ptr);
    ALYRN_CHECK(SlotReleasing(slot), "MemoryPool::Deallocate: missing release claim");
    MarkSlotFree(slot);
    *static_cast<void**>(ptr) = free_list_head_;
    free_list_head_ = ptr;
    ++free_count_;
  }

  // ObjectPool reserves the slot before invoking an arbitrary object
  // destructor, then returns it only after destruction completes. That makes
  // duplicate Release() fail before a second destructor call.
  void ClaimDeallocation(void* ptr) noexcept {
    ALYRN_CHECK(Owns(ptr), "MemoryPool::ClaimDeallocation: foreign pointer");
    std::lock_guard<MutexPolicy> lock{mutex_};
    ClaimDeallocationLocked(ptr);
  }

  void FinishDeallocation(void* ptr) noexcept {
    ALYRN_CHECK(Owns(ptr), "MemoryPool::FinishDeallocation: foreign pointer");
    std::lock_guard<MutexPolicy> lock{mutex_};
    FinishDeallocationLocked(ptr);
  }

  void Initialize() {
    buffer_ = static_cast<std::byte*>(
        ::operator new(kSlotSize * kCapacity, std::align_val_t{kAlignment}));

    std::byte* current = buffer_;
    for (std::size_t i = 0; i < kCapacity - 1; ++i) {
      std::byte* next = current + kSlotSize;
      *reinterpret_cast<void**>(current) = next;
      current = next;
    }

    *reinterpret_cast<void**>(current) = nullptr;
    free_list_head_ = buffer_;
    free_count_ = kCapacity;
  }

  std::byte* buffer_{nullptr};
  void* free_list_head_{nullptr};
  std::size_t free_count_{0};
  std::array<std::uint64_t, kStateWordCount> allocated_slots_{};
  std::array<std::uint64_t, kStateWordCount> releasing_slots_{};

  mutable MutexPolicy mutex_;
};

}  // namespace alyrn::detail
