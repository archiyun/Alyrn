// SPDX-License-Identifier: MIT

#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <iostream>
#include <optional>
#include <thread>

#include "alyrn/coro/channel.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/coro/task.h"
#include "alyrn/coro/work.h"
#include "alyrn/detail/macros.h"
#include "alyrn/io/loop.h"
#include "alyrn/uring/connector.h"
#include "alyrn/uring/detail/loop_access.h"
#include "alyrn/uring/loop.h"
#include "alyrn/uring/options.h"
#include "alyrn/uring/timer.h"

namespace {

using namespace std::chrono_literals;

static_assert(requires(alyrn::uring::Connector& connector) { connector.SleepFor(1ms); });

bool Check(bool condition, const char* message) {
  if (!condition) std::cout << "FAIL: " << message << '\n';
  return condition;
}

bool ExpectChildAbort(void (*entry)(), const char* message) {
  const pid_t child = ::fork();
  if (child < 0) {
    return Check(false, "fork failed for luring loop affinity test");
  }
  if (child == 0) {
    (void)::freopen("/dev/null", "w", stderr);
    entry();
    ::_exit(0);
  }

  int status = 0;
  while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
  }
  return Check(WIFSIGNALED(status), message) &&
         Check(WTERMSIG(status) == SIGABRT, "loop-affinity invariant must terminate with SIGABRT");
}

void RunAfterFromForeignThread() {
  alyrn::uring::Loop loop;
  std::thread foreign([&loop] { (void)loop.RunAfter(0ms, [] noexcept {}); });
  foreign.join();
}

void DestroyLoopFromForeignThread() {
  auto* loop = new alyrn::uring::Loop;
  std::thread foreign([loop] { delete loop; });
  foreign.join();
}

bool TestLoopAffinityIsEnforcedInRelease() {
  return ExpectChildAbort(&RunAfterFromForeignThread,
                          "RunAfter from a foreign thread must terminate in Release") &&
         ExpectChildAbort(&DestroyLoopFromForeignThread,
                          "Loop destruction from a foreign thread must terminate in Release");
}

bool IsEnvironmentSkip(alyrn::Error error) {
  return error == std::errc::operation_not_supported || error == std::errc::operation_not_permitted;
}

bool StopAndDrain(alyrn::uring::Loop& loop) {
  loop.RequestStop();
  loop.Run();

  return Check(loop.State() == alyrn::io::LoopState::kStopped,
               "manual timer loop cleanup should stop the loop") &&
         Check(alyrn::uring::detail::LoopAccess::IsDrained(loop),
               "manual timer loop cleanup should drain user operation work");
}

alyrn::coro::DetachedTask SleepTask(alyrn::uring::Loop* loop, bool* resumed,
                                       bool* scheduler_ok) {
  auto result = co_await alyrn::uring::SleepFor(*loop, 1ms);
  *resumed = true;
  *scheduler_ok = alyrn::coro::Scheduler::TryCurrent() == loop;
  if (!result.HasValue()) co_return;
}

bool TestTimers() {
  alyrn::uring::Loop loop;
  alyrn::uring::Options options;
  options.entries = 16;

  auto init = loop.Init(options);
  if (!init.HasValue()) {
    if (IsEnvironmentSkip(init.Error())) {
      std::cout << "SKIP: io_uring unavailable: " << init.Error().message() << '\n';
      return true;
    }
    return Check(false, "Loop initialization failed");
  }

  bool early_fired = false;
  bool late_fired = false;
  auto late = loop.RunAfter(100ms, [&late_fired] noexcept { late_fired = true; });
  if (!Check(late.HasValue(), "late timer should be accepted")) {
    (void)StopAndDrain(loop);
    return false;
  }

  auto early = loop.RunAfter(2ms, [&early_fired] noexcept { early_fired = true; });
  if (!Check(early.HasValue(), "early timer should be accepted")) {
    (void)StopAndDrain(loop);
    return false;
  }

  // Updating an already armed timeout may produce one or more control CQEs
  // before the updated timer itself expires.
  while (!early_fired && !late_fired) {
    auto completed = alyrn::uring::detail::LoopAccess::WaitCompletions(loop);
    if (!Check(completed.HasValue(), "timer completion should be received")) {
      (void)StopAndDrain(loop);
      return false;
    }
  }

  if (!Check(early_fired, "earlier timer should fire first") ||
      !Check(!late_fired, "later timer should not fire early")) {
    (void)StopAndDrain(loop);
    return false;
  }

  if (!Check(loop.CancelTimer(*late).HasValue(), "later timer should be cancellable")) {
    (void)StopAndDrain(loop);
    return false;
  }

  bool resumed = false;
  bool scheduler_ok = false;
  alyrn::coro::SpawnDetach(loop, SleepTask(&loop, &resumed, &scheduler_ok));
  alyrn::uring::detail::LoopAccess::RunReady(loop);

  // A successful timeout update may retire the replaced physical request with
  // an ECANCELED CQE before the re-armed driver reaches ETIME. Keep driving
  // physical completions until the logical SleepFor continuation is ready.
  bool completion_received = false;
  for (int attempt = 0; attempt != 3 && !resumed; ++attempt) {
    auto completed = alyrn::uring::detail::LoopAccess::WaitCompletions(loop);
    if (!completed.HasValue()) {
      break;
    }
    completion_received = true;
    alyrn::uring::detail::LoopAccess::RunReady(loop);
  }

  const bool passed = Check(completion_received, "sleep should complete") &&
                      Check(resumed, "SleepFor should resume the coroutine") &&
                      Check(scheduler_ok, "SleepFor should resume on its loop scheduler");
  return StopAndDrain(loop) && passed;
}

alyrn::coro::DetachedTask ReceiveTick(alyrn::uring::Loop* loop, alyrn::coro::Channel<int>* ticks,
                                      int* received) {
  std::optional<int> value;
  (void)co_await (*ticks >> value);
  *received = value.value_or(-1);
  ticks->Close();
  loop->RequestStop();
}

// A timer callback must observe its Loop as the current scheduler, so it can
// feed an owner-affine Channel.
bool TestTimerCallbackUsesOwnerChannel() {
  alyrn::uring::Loop loop;
  alyrn::uring::Options options;
  options.entries = 16;

  auto init = loop.Init(options);
  if (!init.HasValue()) {
    if (IsEnvironmentSkip(init.Error())) {
      return true;
    }
    return Check(false, "Loop initialization failed for channel callback test");
  }

  alyrn::coro::Channel<int> ticks(loop, 1);
  bool owner_context = false;
  int received = 0;
  auto timer = loop.RunAfter(1ms, [&] {
    owner_context = alyrn::coro::Scheduler::TryCurrent() == &loop;
    (void)ticks.TrySend(42);
  });
  if (!Check(timer.HasValue(), "channel timer should be accepted")) {
    (void)StopAndDrain(loop);
    return false;
  }
  alyrn::coro::SpawnDetach(loop, ReceiveTick(&loop, &ticks, &received));
  loop.Run();

  return Check(owner_context, "timer callback must run in its Loop scheduling context") &&
         Check(received == 42, "timer callback must feed an owner-affine Channel") &&
         Check(loop.State() == alyrn::io::LoopState::kStopped, "channel test loop should stop");
}

bool TestStopDiscardsUnexpiredTimer() {
  bool fired = false;
  {
    alyrn::uring::Loop loop;
    alyrn::uring::Options options;
    options.entries = 8;

    auto init = loop.Init(options);
    if (!init.HasValue()) {
      if (IsEnvironmentSkip(init.Error())) {
        return true;
      }
      return Check(false, "Loop initialization failed for stop test");
    }

    auto timer = loop.RunAfter(1h, [&fired] noexcept { fired = true; });
    if (!Check(timer.HasValue(), "unexpired timer should be accepted")) {
      return false;
    }

    std::jthread stopper([&loop] {
      std::this_thread::sleep_for(2ms);
      loop.RequestStop();
    });
    loop.Run();
    stopper.join();

    if (!Check(loop.State() == alyrn::io::LoopState::kStopped,
               "loop with an unexpired timer should stop")) {
      return false;
    }
  }

  return Check(!fired, "loop shutdown must discard an unexpired timer without running it");
}

// --- SleepFor lifecycle: the same contract as epoll::SleepFor ---

using UringLoop = alyrn::uring::Loop;

struct Observation {
  int resumes{0};
  bool success{false};
  bool canceled{false};
  bool owner{false};
};

alyrn::coro::Task<void> Sleep(UringLoop& loop, alyrn::time::Duration delay, Observation& observed) {
  auto result = co_await alyrn::uring::SleepFor(loop, delay);
  ++observed.resumes;
  observed.success = result.HasValue();
  observed.canceled = !result && result.Error() == std::errc::operation_canceled;
  observed.owner = loop.IsInLoopThread() && alyrn::coro::Scheduler::TryCurrent() == &loop;
}

// Owns the root explicitly so a test can destroy a sleeping frame.
class RunningTask {
public:
  RunningTask(UringLoop& loop, alyrn::coro::Task<void> task) : handle_(task.Release()) {
    alyrn::coro::ResumeWork work{handle_};
    loop.alyrn::coro::Scheduler::Run(&work);
  }
  ~RunningTask() { handle_.destroy(); }

  ALYRN_DELETE_COPY_MOVE(RunningTask);

private:
  alyrn::coro::Task<void>::Handle handle_;
};

// Returns false and sets `skipped` when io_uring is unavailable.
bool InitSmallLoop(UringLoop& loop, bool& skipped) {
  alyrn::uring::Options options;
  options.entries = 16;
  auto init = loop.Init(options);
  skipped = !init.HasValue() && IsEnvironmentSkip(init.Error());
  return init.HasValue();
}

bool SleepNonpositiveAndStopped() {
  UringLoop loop;
  bool skipped = false;
  if (!InitSmallLoop(loop, skipped)) return skipped;
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

bool SleepShutdownCancelsPending() {
  UringLoop loop;
  bool skipped = false;
  if (!InitSmallLoop(loop, skipped)) return skipped;
  Observation observed;
  RunningTask task(loop, Sleep(loop, 1h, observed));
  if (!Check(observed.resumes == 0, "positive sleep must suspend")) return false;
  std::thread stopper([&loop] { loop.RequestStop(); });
  stopper.join();
  loop.Run();
  return Check(observed.resumes == 1 && observed.canceled && observed.owner,
               "shutdown must cancel pending sleep once on owner scheduler");
}

bool SleepExpiryBeforeShutdown() {
  UringLoop loop;
  bool skipped = false;
  if (!InitSmallLoop(loop, skipped)) return skipped;
  Observation observed;
  RunningTask task(loop, Sleep(loop, 1ms, observed));
  // Both timers are overdue before Run: expiry queues the continuation, then
  // shutdown starts before that continuation gets a turn.
  (void)loop.RunAfter(2ms, [&loop] { loop.RequestStop(); });
  std::this_thread::sleep_for(4ms);
  loop.Run();
  return Check(observed.resumes == 1 && observed.success && observed.owner,
               "expiry result must survive later shutdown before resume");
}

bool SleepStopBeforeExpiryInSameBatch() {
  UringLoop loop;
  bool skipped = false;
  if (!InitSmallLoop(loop, skipped)) return skipped;
  Observation observed;
  (void)loop.RunAfter(1ms, [&loop] { loop.RequestStop(); });
  RunningTask task(loop, Sleep(loop, 2ms, observed));
  std::this_thread::sleep_for(4ms);
  loop.Run();
  return Check(observed.resumes == 1 && observed.canceled && observed.owner,
               "timer callback seeing stop must cancel instead of reporting elapsed");
}

bool SleepDestroyedBeforeExpiry() {
  UringLoop loop;
  bool skipped = false;
  if (!InitSmallLoop(loop, skipped)) return skipped;
  Observation abandoned;
  {
    RunningTask task(loop, Sleep(loop, 1ms, abandoned));
  }
  Observation live;
  RunningTask task(loop, Sleep(loop, 2ms, live));
  (void)loop.RunAfter(3ms, [&loop] { loop.RequestStop(); });
  std::this_thread::sleep_for(5ms);
  loop.Run();
  return Check(abandoned.resumes == 0 && live.resumes == 1 && live.success,
               "destroyed pending sleep must remove its timer and shutdown participant");
}

struct FrameTracker {
  bool* destroyed;
  ~FrameTracker() { *destroyed = true; }
};

alyrn::coro::DetachedTask DetachedSleeper(UringLoop& loop, bool& resumed_canceled,
                                          bool& destroyed) {
  FrameTracker tracker{&destroyed};
  auto slept = co_await alyrn::uring::SleepFor(loop, 10s);
  resumed_canceled = !slept && slept.Error() == std::errc::operation_canceled;
}

// A detached coroutine sleeping when the loop stops must resume and release
// its frame instead of being abandoned.
bool DetachedSleepReleasedOnStop() {
  bool resumed_canceled = false;
  bool destroyed = false;
  {
    UringLoop loop;
    bool skipped = false;
    if (!InitSmallLoop(loop, skipped)) return skipped;
    alyrn::coro::SpawnDetach(loop, DetachedSleeper(loop, resumed_canceled, destroyed));
    (void)loop.RunAfter(20ms, [&loop] { loop.RequestStop(); });
    loop.Run();
  }
  return Check(resumed_canceled, "a sleep pending at stop must resume with operation_canceled") &&
         Check(destroyed, "a detached sleeper must release its frame when the loop stops");
}

alyrn::coro::Task<void> LegacySleep(alyrn::uring::Connector& connector, int& resumes) {
  co_await connector.SleepFor(1h);
  ++resumes;
}

bool LegacySleepResumesOnStop() {
  UringLoop loop;
  bool skipped = false;
  if (!InitSmallLoop(loop, skipped)) return skipped;
  alyrn::uring::Connector connector(&loop);
  int resumes = 0;
  RunningTask task(loop, LegacySleep(connector, resumes));
  loop.RequestStop();
  loop.Run();
  return Check(resumes == 1, "legacy sleep must resume when the loop stops");
}

void DestroyLoopWithSleepingFrame() {
  auto* loop = new UringLoop;
  bool skipped = false;
  if (!InitSmallLoop(*loop, skipped)) ::_exit(0);
  Observation observed;
  // Leak the sleeping frame so the Loop is destroyed while it is registered.
  new RunningTask(*loop, Sleep(*loop, 1h, observed));
  delete loop;
}

// uring::Loop spells its timers like epoll::Loop: RunAt, RunAfter, RunEvery,
// and Cancel, with CancelTimer kept as an alias.
bool TestTimerApiParity() {
  UringLoop loop;
  bool skipped = false;
  if (!InitSmallLoop(loop, skipped)) return skipped;

  bool at_fired = false;
  auto at = loop.RunAt(alyrn::time::SteadyNow() + 2ms, [&] { at_fired = true; });

  int every_count = 0;
  auto every = loop.RunEvery(1ms, [&] { ++every_count; });

  int self_count = 0;
  alyrn::time::TimerId self_id{};
  auto self = loop.RunEvery(1ms, [&] {
    if (++self_count == 3) {
      (void)loop.Cancel(self_id);
    }
  });
  if (self.HasValue()) self_id = *self;

  int count_at_cancel = -1;
  std::optional<alyrn::Result<void>> cancel_every;
  (void)loop.RunAfter(15ms, [&] {
    count_at_cancel = every_count;
    cancel_every.emplace(loop.Cancel(*every));
  });

  std::optional<alyrn::Result<void>> cancel_fired;
  std::optional<alyrn::Result<void>> cancel_alias;
  int count_at_stop = -1;
  (void)loop.RunAfter(30ms, [&] {
    count_at_stop = every_count;
    cancel_fired.emplace(loop.Cancel(*at));
    cancel_alias.emplace(loop.CancelTimer(*at));
    loop.RequestStop();
  });
  loop.Run();

  auto no_entry = [](const std::optional<alyrn::Result<void>>& result) {
    return result.has_value() && !result->HasValue() &&
           result->Error() == std::errc::no_such_file_or_directory;
  };
  return Check(at.HasValue() && every.HasValue() && self.HasValue(),
               "RunAt and RunEvery must register timers") &&
         Check(at_fired, "RunAt must fire at its deadline") &&
         Check(count_at_cancel >= 3, "RunEvery must fire repeatedly") &&
         Check(cancel_every.has_value() && cancel_every->HasValue(),
               "Cancel must stop a repeating timer") &&
         Check(count_at_stop == count_at_cancel, "a canceled repeating timer must not run again") &&
         Check(self_count == 3, "a repeating timer must be able to cancel itself") &&
         Check(no_entry(cancel_fired) && no_entry(cancel_alias),
               "Cancel and CancelTimer must report ENOENT for a timer that already ran");
}

bool TestSleepLifecycle() {
  return SleepNonpositiveAndStopped() && SleepShutdownCancelsPending() &&
         SleepExpiryBeforeShutdown() && SleepStopBeforeExpiryInSameBatch() &&
         SleepDestroyedBeforeExpiry() && DetachedSleepReleasedOnStop() &&
         LegacySleepResumesOnStop() &&
         ExpectChildAbort(&DestroyLoopWithSleepingFrame,
                          "destroying a Loop with a registered sleep must terminate");
}

}  // namespace

int main() {
  if (!TestLoopAffinityIsEnforcedInRelease()) return 1;
  if (!TestTimers()) return 1;
  if (!TestStopDiscardsUnexpiredTimer()) return 1;
  if (!TestTimerCallbackUsesOwnerChannel()) return 1;
  if (!TestSleepLifecycle()) return 1;
  if (!TestTimerApiParity()) return 1;
  std::cout << "luring timer smoke: PASS\n";
  return 0;
}
