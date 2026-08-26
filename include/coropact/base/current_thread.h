// SPDX-License-Identifier: MIT
#pragma once

#include <thread>

namespace coropact::base {

using ThreadId = std::thread::id;

// Loop ownership needs a stable C++ thread identity, not Linux's SYS_gettid
// ABI. std::thread::id is comparable across all supported POSIX backends.
[[nodiscard]]
inline ThreadId CurrentThreadId() noexcept {
  return std::this_thread::get_id();
}

}  // namespace coropact::base
