// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <new>
#include <utility>

#include "alyrn/detail/base/check.h"
#include "alyrn/detail/memory/memory_pool.h"
#include "alyrn/detail/utils/macros.h"

namespace alyrn::detail::memory {

// ObjectPool manages typed object allocation on top of MemoryPool.
//
// Template parameters:
//   T           - Type of objects to allocate.
//   kCapacity   - Maximum number of objects (default: 1024).
//   MutexPolicy - Forwarded to the underlying MemoryPool (default: std::mutex).
//
// Responsibilities:
//   - Placement-new on Acquire (exception-safe: returns slot on constructor throw).
//   - Destructor call on Release.
//   - ScopedPtr for automatic return-to-pool on scope exit.
//
// Example:
//   ObjectPool<MyType, 128> pool;
//   auto ptr = pool.AcquireScoped(arg1, arg2);
//   if (ptr) {
//     ptr->DoSomething();
//   }  // automatically returned to the pool here
template <
    typename    T,
    std::size_t kCapacity   = 1024,
    typename    MutexPolicy = std::mutex
>
class ObjectPool {
 public:
  class Deleter {
   public:
    Deleter() = default;
    explicit Deleter(ObjectPool* pool) noexcept : pool_(pool) {}

    void operator()(T* ptr) const noexcept {
      if (pool_ != nullptr && ptr != nullptr) {
        pool_->Release(ptr);
      }
    }

   private:
    ObjectPool* pool_{nullptr};
  };

  using ScopedPtr = std::unique_ptr<T, Deleter>;

  ObjectPool() = default;
  ~ObjectPool() {
    ALYRN_CHECK(pool_.UsedCount() == 0,
                   "ObjectPool: destroyed with live pool-owned objects");
    std::lock_guard<MutexPolicy> lock{overflow_mutex_};
    ALYRN_CHECK(overflow_head_ == nullptr,
                   "ObjectPool: destroyed with live overflow objects");
  }

  ALYRN_DELETE_COPY_MOVE(ObjectPool);

  // Constructs an object of type T using forwarded arguments. When the pool
  // is exhausted, falls back to a registered heap allocation so callers never
  // observe nullptr. The fallback path is slower than a pool hit; a sustained
  // non-zero OverflowCount() indicates Capacity is undersized for the load.
  // Throws: any exception thrown by T's constructor (slot is returned on throw).
  template <typename... Args>
  T* Acquire(Args&&... args) {
    if (void* mem = pool_.Allocate()) {
      try {
        return new (mem) T(std::forward<Args>(args)...);
      } catch (...) {
        pool_.Deallocate(mem);
        throw;
      }
    }

    auto* overflow = new OverflowNode;
    try {
      T* object = std::construct_at(overflow->Storage(), std::forward<Args>(args)...);
      {
        std::lock_guard<MutexPolicy> lock{overflow_mutex_};
        overflow->next = overflow_head_;
        overflow_head_ = overflow;
      }
      overflow_count_.fetch_add(1, std::memory_order_relaxed);
      return object;
    } catch (...) {
      delete overflow;
      throw;
    }
  }

  // Constructs an object and wraps it in a ScopedPtr that automatically
  // returns the object to the pool on scope exit.
  template <typename... Args>
  ScopedPtr AcquireScoped(Args&&... args) {
    return ScopedPtr(Acquire(std::forward<Args>(args)...), Deleter(this));
  }

  // Destroys an object returned by this ObjectPool and returns its storage.
  // Pool-owned pointers go back to the free list; registered heap-overflow
  // pointers are deleted. Passing nullptr is a no-op. A pointer acquired from
  // another ObjectPool or by another allocation mechanism is a memory-safety
  // violation and terminates.
  void Release(T* ptr) noexcept {
    if (ptr == nullptr) {
      return;
    }
    if (pool_.Owns(ptr)) {
      pool_.ClaimDeallocation(static_cast<void*>(ptr));
      std::destroy_at(ptr);
      pool_.FinishDeallocation(static_cast<void*>(ptr));
    } else {
      OverflowNode* overflow = TakeOverflow(ptr);
      ALYRN_CHECK(overflow != nullptr, "ObjectPool::Release: foreign pointer");
      std::destroy_at(overflow->Object());
      delete overflow;
    }
  }

  // Returns true if the pointer is backed by this pool's fixed-capacity
  // storage. Heap-overflow objects intentionally return false.
  bool Owns(const T* ptr) const noexcept {
    return pool_.Owns(ptr);
  }

  constexpr std::size_t Capacity() const noexcept { return pool_.Capacity(); }
  std::size_t FreeCount() const noexcept { return pool_.FreeCount(); }
  std::size_t UsedCount() const noexcept { return pool_.UsedCount(); }

  // Number of times Acquire() spilled to the heap because the pool was full.
  // Monotonically increasing; never reset. Useful for sizing Capacity in prod.
  std::size_t OverflowCount() const noexcept {
    return overflow_count_.load(std::memory_order_relaxed);
  }

 private:
  struct OverflowNode {
    OverflowNode* next{nullptr};
    alignas(T) std::byte storage[sizeof(T)];

    [[nodiscard]] T* Storage() noexcept {
      return reinterpret_cast<T*>(storage);
    }

    [[nodiscard]] T* Object() noexcept {
      return std::launder(Storage());
    }
  };

  [[nodiscard]] OverflowNode* TakeOverflow(T* object) noexcept {
    std::lock_guard<MutexPolicy> lock{overflow_mutex_};
    OverflowNode** link = &overflow_head_;
    while (*link != nullptr) {
      OverflowNode* current = *link;
      if (current->Object() == object) {
        *link = current->next;
        current->next = nullptr;
        return current;
      }
      link = &current->next;
    }
    return nullptr;
  }

  MemoryPool<sizeof(T), alignof(T), kCapacity, MutexPolicy> pool_;
  std::atomic<std::size_t> overflow_count_{0};
  OverflowNode* overflow_head_{nullptr};
  mutable MutexPolicy overflow_mutex_;
};

}  // namespace alyrn::detail::memory
