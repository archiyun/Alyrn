#pragma once

#include "runtime/base/noncopyable.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <new>

namespace runtime::memory {

// NullMutex satisfies BasicLockable with zero overhead.
// Use as MutexPolicy for single-threaded or benchmark scenarios.
struct NullMutex {
  void lock()   noexcept {}
  void unlock() noexcept {}
};

// Thread-safe fixed-size raw memory pool.
//
// Template parameters:
//   BlockSize   - Size in bytes of each allocation slot.
//   Alignment   - Required alignment of each slot (default: max platform alignment).
//   Capacity    - Maximum number of slots (default: 1024).
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
template <
    std::size_t BlockSize,
    std::size_t Alignment   = alignof(std::max_align_t),
    std::size_t Capacity    = 1024,
    typename    MutexPolicy = std::mutex
>
class MemoryPool : public runtime::base::NonCopyable {
  static_assert(BlockSize > 0,  "MemoryPool: BlockSize must be > 0");
  static_assert(Capacity  > 0,  "MemoryPool: Capacity must be > 0");
  static_assert(Alignment > 0 && (Alignment & (Alignment - 1)) == 0,
                "MemoryPool: Alignment must be a power of two");

 public:
  MemoryPool() { Initialize(); }

  ~MemoryPool() {
    ::operator delete(buffer_, std::align_val_t{kAlignment});
  }

  // Allocates one slot from the pool.
  // Returns nullptr if the pool is exhausted.
  void* Allocate() noexcept {
    std::lock_guard<MutexPolicy> lock(mutex_);
    if (free_list_head_ == nullptr) {
      return nullptr;
    }

    void* ptr = free_list_head_;
    free_list_head_ = *static_cast<void**>(free_list_head_);
    --free_count_;
    return ptr;
  }

  // Returns a slot back to the pool.
  // Passing nullptr is a no-op.
  void Deallocate(void* ptr) noexcept {
    if (ptr == nullptr) {
      return;
    }

    std::lock_guard<MutexPolicy> lock(mutex_);
    *static_cast<void**>(ptr) = free_list_head_;
    free_list_head_ = ptr;
    ++free_count_;
  }

  // Returns the maximum number of slots in the pool.
  constexpr std::size_t capacity() const noexcept { return Capacity; }

  // Returns the number of currently free slots.
  std::size_t free_count() const noexcept {
    std::lock_guard<MutexPolicy> lock(mutex_);
    return free_count_;
  }

  // Returns the number of currently allocated slots.
  std::size_t used_count() const noexcept {
    std::lock_guard<MutexPolicy> lock(mutex_);
    return Capacity - free_count_;
  }

  // Returns true if ptr was allocated from this pool.
  bool owns(const void* ptr) const noexcept {
    if (ptr == nullptr || buffer_ == nullptr) {
      return false;
    }

    const std::uintptr_t begin = reinterpret_cast<std::uintptr_t>(buffer_);
    const std::uintptr_t end   = begin + kSlotSize * Capacity;
    const std::uintptr_t p     = reinterpret_cast<std::uintptr_t>(ptr);

    if (p < begin || p >= end) {
      return false;
    }

    return (p - begin) % kSlotSize == 0;
  }

 private:
  // Effective alignment: must fit at least one void* for the intrusive free list.
  static constexpr std::size_t kAlignment =
      std::max(Alignment, alignof(void*));

  // Effective slot size: must be large enough for the free-list void* pointer,
  // then rounded up to the alignment boundary.
  static constexpr std::size_t kSlotSize =
      ((std::max(BlockSize, sizeof(void*)) + kAlignment - 1) / kAlignment) *
      kAlignment;

  void Initialize() {
    buffer_ = static_cast<std::byte*>(
        ::operator new(kSlotSize * Capacity, std::align_val_t{kAlignment}));

    std::byte* current = buffer_;
    for (std::size_t i = 0; i < Capacity - 1; ++i) {
      std::byte* next = current + kSlotSize;
      *reinterpret_cast<void**>(current) = next;
      current = next;
    }

    *reinterpret_cast<void**>(current) = nullptr;
    free_list_head_ = buffer_;
    free_count_     = Capacity;
  }

  std::byte*  buffer_{nullptr};
  void*       free_list_head_{nullptr};
  std::size_t free_count_{0};

  mutable MutexPolicy mutex_;
};

}  // namespace runtime::memory
