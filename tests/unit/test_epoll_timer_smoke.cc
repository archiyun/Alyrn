// SPDX-License-Identifier: MIT
#include <chrono>
#include <iostream>
#include <thread>
#include <type_traits>

#include "alyrn/coro/work.h"
#include "alyrn/epoll.h"

namespace {
using namespace std::chrono_literals;
using alyrn::Task;
using alyrn::epoll::Loop;

static_assert(std::is_same_v<decltype(alyrn::epoll::SleepFor(std::declval<Loop&>(), 1ms)),
                             Task<alyrn::Result<void>>>);

bool Check(bool condition, const char* message) {
  if (!condition) std::cerr << "FAIL: " << message << '\n';
  return condition;
}

struct Observation {
  int resumes{0};
  bool success{false};
  bool canceled{false};
  bool owner{false};
};

Task<void> Sleep(Loop& loop, alyrn::time::Duration delay, Observation& observed) {
  auto result = co_await alyrn::epoll::SleepFor(loop, delay);
  ++observed.resumes;
  observed.success = result.HasValue();
  observed.canceled = !result && result.Error() == std::errc::operation_canceled;
  observed.owner = loop.IsInLoopThread() && alyrn::coro::Scheduler::TryCurrent() == &loop;
}

Task<void> LegacySleep(alyrn::epoll::Connector& connector, Observation& observed) {
  co_await connector.SleepFor(1h);
  ++observed.resumes;
}

// Own the root explicitly so the test can exercise destruction before expiry.
class RunningTask {
public:
  RunningTask(Loop& loop, Task<void> task) : handle_(task.Release()) {
    alyrn::coro::ResumeWork work{handle_};
    loop.alyrn::coro::Scheduler::Run(&work);
  }
  ~RunningTask() { handle_.destroy(); }

private:
  Task<void>::Handle handle_;
};

bool NonpositiveAndStopped() {
  Loop loop;
  for (auto delay : {0ms, -1ms}) {
    Observation observed;
    RunningTask task(loop, Sleep(loop, delay, observed));
    if (!Check(observed.resumes == 1 && observed.success && observed.owner,
               "nonpositive sleep must complete inline on owner"))
      return false;
  }
  loop.RequestStop();
  for (auto delay : {0ms, -1ms, 1ms}) {
    Observation observed;
    RunningTask task(loop, Sleep(loop, delay, observed));
    if (!Check(observed.resumes == 1 && observed.canceled, "stopping loop must reject every delay"))
      return false;
  }
  loop.Run();
  Observation stopped;
  RunningTask task(loop, Sleep(loop, 1ms, stopped));
  return Check(stopped.resumes == 1 && stopped.canceled, "stopped loop must reject sleep");
}

bool ShutdownCancelsPending() {
  Loop loop;
  Observation observed;
  RunningTask task(loop, Sleep(loop, 1h, observed));
  if (!Check(observed.resumes == 0, "positive sleep must suspend")) return false;
  std::thread stopper([&loop] { loop.RequestStop(); });
  stopper.join();
  loop.Run();
  return Check(observed.resumes == 1 && observed.canceled && observed.owner,
               "shutdown must cancel pending sleep once on owner scheduler");
}

bool ExpiryBeforeShutdown() {
  Loop loop;
  Observation observed;
  RunningTask task(loop, Sleep(loop, 1ms, observed));
  // Both callbacks are overdue before Run: expiry queues the continuation,
  // then shutdown runs before that continuation gets a scheduler turn.
  loop.RunAfter(2ms, [&loop] { loop.RequestStop(); });
  std::this_thread::sleep_for(4ms);
  loop.Run();
  return Check(observed.resumes == 1 && observed.success && observed.owner,
               "expiry result must survive later shutdown before resume");
}

bool StopBeforeExpiryInSameBatch() {
  Loop loop;
  Observation observed;
  loop.RunAfter(1ms, [&loop] { loop.RequestStop(); });
  RunningTask task(loop, Sleep(loop, 2ms, observed));
  std::this_thread::sleep_for(4ms);
  loop.Run();
  return Check(observed.resumes == 1 && observed.canceled && observed.owner,
               "timer callback seeing stop must cancel instead of reporting elapsed");
}

bool DestroyPendingSleep() {
  Loop loop;
  Observation abandoned;
  {
    RunningTask task(loop, Sleep(loop, 1ms, abandoned));
  }
  Observation live;
  RunningTask task(loop, Sleep(loop, 2ms, live));
  loop.RunAfter(3ms, [&loop] { loop.RequestStop(); });
  std::this_thread::sleep_for(5ms);
  loop.Run();
  return Check(abandoned.resumes == 0 && live.resumes == 1 && live.success,
               "destroyed pending sleep must remove its callback and shutdown participant");
}

bool LegacySleepCancels() {
  Loop loop;
  alyrn::epoll::Connector connector(&loop);
  Observation observed;
  RunningTask task(loop, LegacySleep(connector, observed));
  loop.RequestStop();
  loop.Run();
  return Check(observed.resumes == 1, "legacy sleep must resume without exposing cancel result");
}
}  // namespace

int main() {
  if (!NonpositiveAndStopped() || !ShutdownCancelsPending() || !ExpiryBeforeShutdown() ||
      !StopBeforeExpiryInSameBatch() || !DestroyPendingSleep() || !LegacySleepCancels())
    return 1;
  std::cout << "epoll timer smoke: PASS\n";
}
