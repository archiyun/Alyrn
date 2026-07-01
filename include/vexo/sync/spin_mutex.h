// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <cstdint>
#include <thread>

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#endif

#include "vexo/utils/macros.h"

namespace vexo::sync {

namespace detail {

// Architecture-specific pause/yield hint used inside busy-wait loops.
//
// These instructions do not release the CPU or provide synchronization. They
// only tell the processor that the current thread is in a spin loop, which can
// reduce pipeline pressure and improve behavior on SMT/hyper-threaded cores.
inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  _mm_pause();
#elif defined(__aarch64__) || defined(__arm__)
  __asm__ __volatile__("yield");
#else
  std::this_thread::yield();
#endif
}

}  // namespace detail

// Small non-recursive spin mutex.
//
// SpinMutex satisfies the BasicLockable and Lockable requirements, so it can be
// used with std::lock_guard, std::unique_lock, and policy-based code that
// expects lock(), try_lock(), and unlock().
//
// This mutex never parks a waiting thread in the kernel. Waiters spin in
// userspace for a short period and then call std::this_thread::yield() to give
// the scheduler a chance to run another thread. That makes the uncontended path
// very small, but it also means contention can burn CPU.
//
// Use SpinMutex only for very short critical sections where the holder will not
// block, allocate, perform I/O, log synchronously, or call into code that may
// sleep. Prefer std::mutex for general-purpose synchronization, long or unknown
// critical sections, oversubscribed thread pools, or user-facing latency paths.
//
// Properties:
//   - non-recursive: locking twice from the same thread deadlocks
//   - unfair: no FIFO ordering or starvation guarantee
//   - process-local: only intended for threads in the same process
//   - noexcept: suitable for low-level runtime code
class SpinMutex {
public:
  SpinMutex() noexcept = default;
  ~SpinMutex() = default;

  VEXO_DELETE_COPY_MOVE(SpinMutex);

  void lock() noexcept {
    for (;;) {
      // Fast path: try to acquire the lock.
      if (!flag_.test_and_set(std::memory_order_acquire)) {
        return;
      }

      // Slow path: spin using ordinary loads until the lock looks free.
      // This test-test-and-set pattern avoids repeatedly writing the cache line
      // while another core owns the mutex.
      std::uint32_t spins = 0;
      while (flag_.test(std::memory_order_relaxed)) {
        if (spins < kSpinCount) {
          detail::cpu_relax();
          ++spins;
        } else {
          std::this_thread::yield();
          spins = 0;
        }
      }
    }
  }

  bool try_lock() noexcept { return !flag_.test_and_set(std::memory_order_acquire); }

  void unlock() noexcept { flag_.clear(std::memory_order_release); }

private:
  // Keep the bounded spin short. If the owner was descheduled, pure spinning
  // would waste a full time slice; yield() is a pragmatic fallback before
  // retrying the userspace fast path.
  static constexpr std::uint32_t kSpinCount{64};

  std::atomic_flag flag_{ATOMIC_FLAG_INIT};
};

}  // namespace vexo::sync
