// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <sys/syscall.h>
#include <unistd.h>

namespace vexo::base {

// Caches the OS thread id for the current thread.
//
// The value is stored in thread-local storage so repeated tid() calls do not
// need to query the kernel every time.
inline thread_local int t_cached_tid = 0;

// Populates the cached thread id for the current thread.
//
// This is typically called on the first tid() access when the cache is empty.
inline void cacheTid() {
  if (t_cached_tid == 0) {
    // Query the kernel once per thread and keep the result in TLS so later
    // tid() calls can use the cached value directly.
    t_cached_tid = static_cast<int>(::syscall(SYS_gettid));
  }
}

// Returns the cached OS thread id for the current thread.
//
// On the fast path, this only reads a thread-local integer. If the cache is
// still empty, it initializes it first.
inline int tid() {
  if (t_cached_tid == 0) [[unlikely]] {
    cacheTid();
  }
  return t_cached_tid;
}

}  // namespace vexo::base
