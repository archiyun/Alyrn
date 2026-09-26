// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>
#include <stop_token>
#include <type_traits>
#include <utility>

#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/coro/task.h"
#include "alyrn/detail/check.h"
#include "alyrn/io/loop.h"

namespace alyrn::io {

// Runs task as the root of loop on the calling thread and returns its result.
// This is the entry point for clients and tools that are not Runtime servers:
//
//   alyrn::epoll::Loop loop;
//   int status = alyrn::io::BlockOn(loop, ClientMain(loop));
//
// The calling thread must own the loop (a uring Loop must also be
// initialized). The loop stops as soon as the task completes; a Loop runs only
// once, so it cannot be reused afterwards. If the loop is stopped from
// elsewhere, its pending I/O is canceled and the task normally completes with
// those errors; a task that still has not completed once the loop has drained
// (for example one parked on a Channel nobody closes) is a contract violation
// and terminates.
template <class L, coro::Returnable T>
  requires ManagedLoop<L> && std::derived_from<L, coro::Scheduler>
T BlockOn(L& loop, coro::Task<T> task) {
  // Captureless: the coroutine frame owns copies of both arguments, so the
  // temporary closure may be destroyed before the root first runs.
  auto root = coro::Spawn(loop, [](L& owner, coro::Task<T> body) -> coro::Task<T> {
    if constexpr (std::is_void_v<T>) {
      co_await std::move(body);
      owner.RequestStop();
    } else {
      T value = co_await std::move(body);
      owner.RequestStop();
      co_return value;
    }
  }(loop, std::move(task)));
  loop.Run(std::stop_token{});
  ALYRN_CHECK(root.IsFinished(), "io::BlockOn: the loop stopped before its root task completed");
  return root.Wait();
}

}  // namespace alyrn::io
