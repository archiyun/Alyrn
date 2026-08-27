// SPDX-License-Identifier: MIT
//
// SyncWait(Task<T>) drives a task from a blocking thread. It is for tests and
// pure-computation coroutines only; never call it on an I/O loop thread.
#pragma once

#include <optional>
#include <semaphore>
#include <type_traits>
#include <utility>

#include "alyrn/detail/coro/sync_wait_root.h"

namespace alyrn::coro {

template <Returnable T>
  requires(!std::is_void_v<T>)
[[nodiscard]]
T SyncWait(Task<T> task) {
  std::optional<T> out;
  std::binary_semaphore done{0};
  detail::SyncWaitRun(std::move(task), &out, &done);
  done.acquire();
  return std::move(*out);
}

inline void SyncWait(Task<void> task) {
  std::binary_semaphore done{0};
  detail::SyncWaitRun(std::move(task), &done);
  done.acquire();
}

}  // namespace alyrn::coro
