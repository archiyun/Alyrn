// SPDX-License-Identifier: MIT

#include <unistd.h>

#include <chrono>
#include <iostream>
#include <optional>

#include "coropact/result.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/spawn.h"
#include "coropact/coro/task.h"
#include "coropact/kqueue/connector.h"
#include "coropact/kqueue/listener.h"
#include "coropact/kqueue/loop.h"
#include "coropact/kqueue/stream.h"
#include "coropact/net/endpoint.h"
#include "coropact/time/clock.h"

namespace {

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

coropact::coro::DetachedTask ConnectOnce(coropact::kqueue::KqueueConnector* connector,
                                         coropact::net::Endpoint peer,
                                         coropact::kqueue::KqueueLoop* loop,
                                         std::optional<coropact::Result<coropact::kqueue::KqueueStream>>* out) {
  out->emplace(co_await connector->Connect(peer));
  loop->RequestStop();
}

bool CheckConnectSuccess() {
  coropact::kqueue::KqueueLoop loop;
  auto listener = coropact::kqueue::KqueueListener::Create(&loop, coropact::net::Endpoint(0));
  if (!Check(listener.has_value(), "listener create failed")) {
    return false;
  }
  auto address = listener->LocalAddress();
  if (!Check(address.has_value(), "listener address failed")) {
    return false;
  }

  coropact::kqueue::KqueueConnector connector(&loop);
  std::optional<coropact::Result<coropact::kqueue::KqueueStream>> result;
  coropact::coro::SpawnDetach(loop, ConnectOnce(&connector, *address, &loop, &result));
  loop.Run();

  return Check(result.has_value() && result->has_value(), "Connect did not succeed");
}

bool CheckConnectRejectsInvalidHost() {
  coropact::kqueue::KqueueLoop loop;
  coropact::kqueue::KqueueConnector connector(&loop);
  std::optional<coropact::Result<coropact::kqueue::KqueueStream>> result;

  coropact::coro::SpawnDetach(loop, ConnectOnce(&connector, coropact::net::Endpoint(1), &loop, &result));
  loop.Run();

  return Check(result.has_value() && !result->has_value(),
               "Connect to a closed port should fail");
}

bool CheckConnectNullFactory() {
  auto connector = coropact::kqueue::KqueueConnector::Create(nullptr);
  return Check(!connector.has_value() && connector.error() == std::errc::invalid_argument,
               "connector factory accepted a null loop");
}

coropact::coro::DetachedTask SleepThenStop(coropact::kqueue::KqueueConnector* connector,
                                           coropact::kqueue::KqueueLoop* loop, bool* resumed,
                                           bool* scheduler_ok) {
  co_await connector->SleepFor(coropact::time::Milliseconds(10));
  *scheduler_ok = coropact::coro::Scheduler::TryCurrent() == loop;
  *resumed = true;
  loop->RequestStop();
}

bool CheckSleepForResumes() {
  coropact::kqueue::KqueueLoop loop;
  coropact::kqueue::KqueueConnector connector(&loop);
  bool resumed = false;
  bool scheduler_ok = false;
  bool timed_out = false;

  coropact::coro::SpawnDetach(loop, SleepThenStop(&connector, &loop, &resumed, &scheduler_ok));
  loop.RunAfter(coropact::time::Milliseconds(500), [&] {
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
