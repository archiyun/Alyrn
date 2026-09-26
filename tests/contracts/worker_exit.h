// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <cstddef>
#include <exception>
#include <iostream>
#include <memory_resource>

#include "alyrn/net/endpoint.h"
#include "alyrn/result.h"

namespace alyrn::test::contracts {

struct ExitFailure {
  std::size_t index;
};

inline bool CheckExit(bool condition, const char* message) {
  if (!condition) std::cout << "FAIL: " << message << std::endl;
  return condition;
}

inline bool IsExitFailure(const Result<void, std::exception_ptr>& result, std::size_t index) {
  if (result.HasValue() || !result.Error()) return false;
  try {
    std::rethrow_exception(result.Error());
  } catch (const ExitFailure& failure) {
    return failure.index == index;
  } catch (...) {
    return false;
  }
}

template <class Group, class Options>
bool CheckGroupExitFailure(Options options) {
  std::atomic_size_t exited{0};
  bool fail = true;
  options.worker_num = 3;
  Group group(net::Endpoint(0), options, {}, {}, [&](auto& context) {
    ++exited;
    if (fail && context.index < 2) throw ExitFailure{context.index};
  });
  if (!CheckExit(group.Start().HasValue(), "exit test group should start")) return false;
  auto result = group.Stop();
  if (!CheckExit(IsExitFailure(result, 0), "Stop must return the original first exit exception") ||
      !CheckExit(exited == 3 && !group.Started() && group.Size() == 0,
                 "exit failure must not prevent joining and releasing every worker"))
    return false;
  auto repeated = group.Stop();
  if (!CheckExit(!repeated.HasValue() && repeated.Error() == result.Error(),
                 "repeated Stop must preserve the same exit exception"))
    return false;
  fail = false;
  if (!CheckExit(group.Start().HasValue(), "group should restart after an exit failure"))
    return false;
  return CheckExit(group.Stop().HasValue() && exited == 6,
                   "new startup must clear the previous exit failure");
}

template <class Group, class Options>
bool CheckRollbackExitFailure(Options options, bool factory_failure) {
  std::atomic_size_t exited{0};
  options.worker_num = 2;
  options.frame_resource_factory =
      [factory_failure](std::size_t index) -> std::pmr::memory_resource* {
    if (factory_failure && index == 1) throw 42;
    return nullptr;
  };
  Group group(
      net::Endpoint(0), options,
      [](auto& context) {
        if (context.index == 1) throw 42;
      },
      {},
      [&](auto& context) {
        ++exited;
        throw ExitFailure{context.index};
      });
  if (factory_failure) {
    bool caught = false;
    try {
      (void)group.Start();
    } catch (int value) {
      caught = value == 42;
    }
    if (!CheckExit(caught, "rollback must not replace the startup exception")) return false;
  } else {
    auto started = group.Start();
    if (!CheckExit(!started.HasValue() && started.Error() == std::errc::bad_address,
                   "rollback must preserve the init error"))
      return false;
  }
  return CheckExit(IsExitFailure(group.Stop(), 0),
                   "startup rollback must retain the exit exception for Stop") &&
         CheckExit(exited == (factory_failure ? 1 : 2) && group.Size() == 0,
                   "rollback must join all started workers despite exit exceptions");
}

template <class Worker, class Options>
bool CheckWorkerExitFailure(Options options) {
  bool fail = true;
  Worker worker(0, net::Endpoint(0), options, {}, {}, [&](auto&) {
    if (fail) throw ExitFailure{0};
  });
  if (!CheckExit(worker.Start().HasValue(), "exit test worker should start")) return false;
  worker.Stop();
  auto result = worker.Join();
  if (!CheckExit(IsExitFailure(result, 0), "Join must return the original exit exception"))
    return false;
  auto repeated = worker.Join();
  if (!CheckExit(!repeated.HasValue() && repeated.Error() == result.Error(),
                 "repeated Join must preserve the same exit exception"))
    return false;
  fail = false;
  if (!CheckExit(worker.Start().HasValue(), "joined worker should restart")) return false;
  worker.Stop();
  return CheckExit(worker.Join().HasValue(), "worker restart must clear the previous exit error");
}

}  // namespace alyrn::test::contracts
