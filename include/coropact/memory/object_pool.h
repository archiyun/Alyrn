// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <utility>

#include "coropact/memory/memory_pool.h"
#include "coropact/utils/macros.h"

namespace coropact::memory {

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

  ObjectPool()  = default;
  ~ObjectPool() = default;

  COROPACT_DELETE_COPY_MOVE(ObjectPool);

  // Constructs an object of type T using forwarded arguments. When the pool
  // is exhausted, falls back to heap allocation so callers never observe
  // nullptr. The fallback path is slower than a pool hit; a sustained
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
    overflow_count_.fetch_add(1, std::memory_order_relaxed);
    return new T(std::forward<Args>(args)...);
  }

  // Constructs an object and wraps it in a ScopedPtr that automatically
  // returns the object to the pool on scope exit.
  template <typename... Args>
  ScopedPtr AcquireScoped(Args&&... args) {
    return ScopedPtr(Acquire(std::forward<Args>(args)...), Deleter(this));
  }

  // Destroys the object and returns its storage. Pool-owned pointers go back
  // to the free list; heap-overflow pointers are deleted. Passing nullptr is
  // a no-op.
  void Release(T* ptr) noexcept {
    if (ptr == nullptr) {
      return;
    }
    if (pool_.Owns(ptr)) {
      ptr->~T();
      pool_.Deallocate(static_cast<void*>(ptr));
    } else {
      delete ptr;
    }
  }

  // Returns true if the pointer belongs to this pool.
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
  MemoryPool<sizeof(T), alignof(T), kCapacity, MutexPolicy> pool_;
  std::atomic<std::size_t> overflow_count_{0};
};

}  // namespace coropact::memory
