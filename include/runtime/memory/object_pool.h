#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/memory/memory_pool.h"

#include <cassert>
#include <cstddef>
#include <memory>
#include <utility>

namespace runtime::memory {

// ObjectPool manages typed object allocation on top of MemoryPool.
//
// Template parameters:
//   T           - Type of objects to allocate.
//   Capacity    - Maximum number of objects (default: 1024).
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
    std::size_t Capacity    = 1024,
    typename    MutexPolicy = std::mutex
>
class ObjectPool : public runtime::base::NonCopyable {
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

  // Constructs an object of type T in the pool using forwarded arguments.
  // Returns nullptr if the pool is exhausted.
  // Throws: any exception thrown by T's constructor (slot is returned on throw).
  template <typename... Args>
  T* Acquire(Args&&... args) {
    void* mem = pool_.Allocate();
    if (mem == nullptr) {
      return nullptr;
    }

    try {
      return new (mem) T(std::forward<Args>(args)...);
    } catch (...) {
      pool_.Deallocate(mem);
      throw;
    }
  }

  // Constructs an object and wraps it in a ScopedPtr that automatically
  // returns the object to the pool on scope exit.
  template <typename... Args>
  ScopedPtr AcquireScoped(Args&&... args) {
    return ScopedPtr(Acquire(std::forward<Args>(args)...), Deleter(this));
  }

  // Destroys the object and returns its slot to the pool.
  // Passing nullptr is a no-op.
  void Release(T* ptr) noexcept {
    if (ptr == nullptr) {
      return;
    }

    assert(pool_.owns(ptr) && "pointer does not belong to this ObjectPool");
    ptr->~T();
    pool_.Deallocate(static_cast<void*>(ptr));
  }

  // Returns true if the pointer belongs to this pool.
  bool owns(const T* ptr) const noexcept {
    return pool_.owns(ptr);
  }

  constexpr std::size_t capacity()   const noexcept { return pool_.capacity();   }
  std::size_t           free_count() const noexcept { return pool_.free_count(); }
  std::size_t           used_count() const noexcept { return pool_.used_count(); }

 private:
  MemoryPool<sizeof(T), alignof(T), Capacity, MutexPolicy> pool_;
};

}  // namespace runtime::memory
