// SPDX-License-Identifier: MIT

#include <unistd.h>

#include <chrono>
#include <iostream>
#include <optional>

#include "alyrn/result.h"
#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/coro/task.h"
#include "alyrn/kqueue/connector.h"
#include "alyrn/kqueue/listener.h"
#include "alyrn/kqueue/loop.h"
#include "alyrn/kqueue/stream.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/time/clock.h"

namespace {

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

alyrn::coro::DetachedTask ConnectOnce(alyrn::kqueue::Connector* connector,
                                         alyrn::net::Endpoint peer,
                                         alyrn::kqueue::Loop* loop,
                                         std::optional<alyrn::Result<alyrn::kqueue::Stream>>* out) {
  out->emplace(co_await connector->Connect(peer));
  loop->RequestStop();
}

bool CheckConnectSuccess() {
  alyrn::kqueue::Loop loop;
  auto listener = alyrn::kqueue::Listener::Create(&loop, alyrn::net::Endpoint(0));
  if (!Check(listener.has_value(), "listener create failed")) {
    return false;
  }
  auto address = listener->LocalAddress();
  if (!Check(address.has_value(), "listener address failed")) {
    return false;
  }

  alyrn::kqueue::Connector connector(&loop);
  std::optional<alyrn::Result<alyrn::kqueue::Stream>> result;
  alyrn::coro::SpawnDetach(loop, ConnectOnce(&connector, *address, &loop, &result));
  loop.Run();

  return Check(result.has_value() && result->has_value(), "Connect did not succeed");
}

bool CheckConnectRejectsInvalidHost() {
  alyrn::kqueue::Loop loop;
  alyrn::kqueue::Connector connector(&loop);
  std::optional<alyrn::Result<alyrn::kqueue::Stream>> result;

  alyrn::coro::SpawnDetach(loop, ConnectOnce(&connector, alyrn::net::Endpoint(1), &loop, &result));
  loop.Run();

  return Check(result.has_value() && !result->has_value(),
               "Connect to a closed port should fail");
}

bool CheckConnectNullFactory() {
  auto connector = alyrn::kqueue::Connector::Create(nullptr);
  return Check(!connector.has_value() && connector.error() == std::errc::invalid_argument,
               "connector factory accepted a null loop");
}

alyrn::coro::DetachedTask SleepThenStop(alyrn::kqueue::Connector* connector,
                                           alyrn::kqueue::Loop* loop, bool* resumed,
                                           bool* scheduler_ok) {
  co_await connector->SleepFor(alyrn::time::Milliseconds(10));
  *scheduler_ok = alyrn::coro::Scheduler::TryCurrent() == loop;
  *resumed = true;
  loop->RequestStop();
}

bool CheckSleepForResumes() {
  alyrn::kqueue::Loop loop;
  alyrn::kqueue::Connector connector(&loop);
  bool resumed = false;
  bool scheduler_ok = false;
  bool timed_out = false;

  alyrn::coro::SpawnDetach(loop, SleepThenStop(&connector, &loop, &resumed, &scheduler_ok));
  loop.RunAfter(alyrn::time::Milliseconds(500), [&] {
    timed_out = true;
    loop.RequestStop();
  });
  loop.Run();

  return Check(!timed_out, "SleepFor should resume before watchdog") &&
         Check(resumed, "SleepFor should resume the coroutine") &&
         Check(scheduler_ok, "SleepFor should resume on its loop scheduler");
}

}  // namespace

int main() {
  if (!CheckConnectNullFactory()) return 1;
  if (!CheckConnectSuccess()) return 1;
  if (!CheckConnectRejectsInvalidHost()) return 1;
  if (!CheckSleepForResumes()) return 1;
  std::cout << "kqueue connector smoke: PASS\n";
  return 0;
}
