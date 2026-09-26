// SPDX-License-Identifier: MIT
// Runtime startup guarantees shared by the epoll and io_uring worker groups.
#pragma once

#include <atomic>
#include <iostream>
#include <system_error>

#include "alyrn/backend/loop.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/net/endpoint.h"

namespace alyrn::test::contracts {

struct StartupFailure {};

inline bool CheckStartup(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << std::endl;
  }
  return condition;
}

template <class Group, class Options, class ConfigureFactory>
bool CheckFactoryFailureRollsBack(Options options, ConfigureFactory configure_factory) {
  std::atomic_size_t initialized{0};
  std::atomic_size_t exited{0};
  bool fail = true;
  options.worker_num = 2;
  configure_factory(options, fail);
  Group group(
      net::Endpoint(0), options, [&](auto&) { ++initialized; }, {}, [&](auto&) { ++exited; });

  bool caught = false;
  try {
    (void)group.Start();
  } catch (const StartupFailure&) {
    caught = true;
  }
  if (!CheckStartup(caught, "startup must propagate the original factory exception") ||
      !CheckStartup(!group.Started() && group.Size() == 0,
                    "factory failure must leave an empty, stopped group") ||
      !CheckStartup(initialized == 1 && exited == 1,
                    "factory failure must join the previously started worker")) {
    return false;
  }

  fail = false;
  auto retry = group.Start();
  if (!CheckStartup(retry.HasValue() && group.Started() && group.Size() == 2,
                    "retry after factory failure must start exactly two workers") ||
      !CheckStartup(initialized == 3, "retry must initialize each worker exactly once")) {
    return false;
  }
  auto repeated = group.Start();
  if (!CheckStartup(
          !repeated.HasValue() && repeated.Error() == std::errc::connection_already_in_progress,
          "starting an active group must still return EALREADY") ||
      !CheckStartup(group.Started() && group.Size() == 2 && exited == 1,
                    "EALREADY must preserve the running group")) {
    return false;
  }
  group.Stop();
  group.Stop();
  return CheckStartup(!group.Started() && group.Size() == 0 && exited == 3,
                      "stop after retry must join every worker exactly once");
}

template <class Context>
coro::DetachedTask CheckInitWorkDrained(Context* context, std::atomic_size_t* drained,
                                        std::atomic_bool* bad_cleanup) {
  if (!context->loop.IsInLoopThread() || context->loop.State() != backend::LoopState::kStopping) {
    *bad_cleanup = true;
  }
  auto accepted = co_await context->listener.Accept();
  if (accepted.HasValue() || (accepted.Error() != std::errc::operation_canceled &&
                              accepted.Error() != std::errc::bad_file_descriptor)) {
    *bad_cleanup = true;
  }
  ++*drained;
}

template <class Group, class Options>
bool CheckInitFailureRollsBack(Options options, bool queue_work) {
  std::atomic_size_t initialized{0};
  std::atomic_size_t exited{0};
  std::atomic_size_t drained{0};
  std::atomic_bool bad_cleanup{false};
  bool fail = true;
  options.worker_num = 2;
  Group group(
      net::Endpoint(0), options,
      [&](auto& context) {
        ++initialized;
        if (fail && context.index == 1) {
          if (queue_work) {
            coro::SpawnDetach(context.loop, CheckInitWorkDrained(&context, &drained, &bad_cleanup));
          }
          throw StartupFailure{};
        }
      },
      {},
      [&](auto& context) {
        if (!context.loop.IsInLoopThread() ||
            context.loop.State() != backend::LoopState::kStopped ||
            (fail && queue_work && context.index == 1 && drained != 1)) {
          bad_cleanup = true;
        }
        ++exited;
      });

  auto result = group.Start();
  if (!CheckStartup(!result.HasValue() && result.Error() == std::errc::bad_address,
                    "init exception must return EFAULT") ||
      !CheckStartup(!group.Started() && group.Size() == 0,
                    "init failure must leave an empty, stopped group") ||
      !CheckStartup(initialized == 2 && exited == 2,
                    "init failure must run exit cleanup for both workers") ||
      !CheckStartup(drained == (queue_work ? 1 : 0) && !bad_cleanup,
                    "failed init work must drain on its owner before exit cleanup")) {
    return false;
  }

  fail = false;
  auto retry = group.Start();
  if (!CheckStartup(retry.HasValue() && group.Started() && group.Size() == 2,
                    "retry after init failure must start exactly two workers")) {
    return false;
  }
  group.Stop();
  return CheckStartup(initialized == 4 && exited == 4 && !bad_cleanup,
                      "retry after init failure must preserve worker cleanup");
}

}  // namespace alyrn::test::contracts
