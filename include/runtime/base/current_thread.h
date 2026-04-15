#pragma once

namespace runtime::base {

// Caches the OS thread id for the current thread.
//
// The value is stored in thread-local storage so repeated tid() calls do not
// need to query the kernel every time.
extern thread_local int t_cached_tid;

// Populates the cached thread id for the current thread.
//
// This is typically called on the first tid() access when the cache is empty.
void cacheTid();

// Returns the cached OS thread id for the current thread.
//
// On the fast path, this only reads a thread-local integer. If the cache is
// still empty, it initializes it first.
inline int tid() {
  if (__builtin_expect(t_cached_tid == 0, 0)) {
    cacheTid();
  }
  return t_cached_tid;
}

}  // namespace runtime::base
