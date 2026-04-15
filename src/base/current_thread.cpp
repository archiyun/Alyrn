#include "runtime/base/current_thread.h"

#include <sys/syscall.h>
#include <unistd.h>

namespace runtime::base {

thread_local int t_cached_tid = 0;

void cacheTid() {
  if (t_cached_tid == 0) {
    // Query the kernel once per thread and keep the result in TLS so later
    // tid() calls can use the cached value directly.
    t_cached_tid = static_cast<int>(::syscall(SYS_gettid));
  }
}

}  // namespace runtime::base
