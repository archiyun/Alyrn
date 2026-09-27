// SPDX-License-Identifier: MIT
// Timer and Ticker deliver loop timers through a Channel, for every backend.

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <system_error>

#include "alyrn/coro/channel.h"
#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/select.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/coro/task.h"
#include "alyrn/epoll.h"

#if defined(ALYRN_ENABLE_URING)
#include "alyrn/uring.h"
#endif

namespace {

using namespace std::chrono_literals;
using alyrn::coro::Channel;
using alyrn::coro::Select;
using alyrn::coro::SelectDefault;
using alyrn::coro::Task;
using alyrn::time::Deadline;

bool Check(bool condition, const char* backend, const char* message) {
  if (!condition) std::cerr << "FAIL [" << backend << "]: " << message << '\n';
  return condition;
}

struct EpollBackend {
  using Loop = alyrn::epoll::Loop;
  using Timer = alyrn::epoll::Timer;
  using Ticker = alyrn::epoll::Ticker;
  static constexpr const char* kName = "epoll";
  static bool Init(Loop&, bool&) { return true; }
};

#if defined(ALYRN_ENABLE_URING)
struct UringBackend {
  using Loop = alyrn::uring::Loop;
  using Timer = alyrn::uring::Timer;
  using Ticker = alyrn::uring::Ticker;
  static constexpr const char* kName = "uring";
  static bool Init(Loop& loop, bool& skipped) {
    alyrn::uring::Options options;
    options.entries = 64;
    auto initialized = loop.Init(options);
    skipped =
        !initialized.HasValue() && (initialized.Error() == std::errc::operation_not_supported ||
                                    initialized.Error() == std::errc::operation_not_permitted);
    return initialized.HasValue();
  }
};
#endif

// Found by argument-dependent lookup for each backend.
template <class Loop>
auto Nap(Loop& loop, alyrn::time::Duration delay) {
  return SleepFor(loop, delay);
}

struct Outcome {
  std::size_t selected{99};
  bool fired{false};
  Deadline started{};
  Deadline received_at{};
};

// Nothing arrives on `values`, so the timer case wins after its delay.
template <class B>
Task<void> SelectTimesOut(typename B::Loop& loop, Channel<int>& values, Outcome& outcome) {
  outcome.started = alyrn::time::SteadyNow();
  typename B::Timer timeout(loop, 20ms);
  std::optional<int> value;
  std::optional<Deadline> fired;
  outcome.selected = co_await Select(values >> value, timeout >> fired);
  outcome.fired = fired.has_value();
  outcome.received_at = alyrn::time::SteadyNow();
  values.Close();
  loop.RequestStop();
}

template <class B>
Task<void> SendLater(typename B::Loop& loop, Channel<int>& values) {
  (void)co_await Nap(loop, 5ms);
  (void)co_await (values << 7);
}

// A value that arrives first wins; the timer is dropped with its frame.
template <class B>
Task<void> SelectGetsValueFirst(typename B::Loop& loop, Channel<int>& values, Outcome& outcome) {
  typename B::Timer timeout(loop, 1s);
  std::optional<int> value;
  std::optional<Deadline> fired;
  outcome.selected = co_await Select(values >> value, timeout >> fired);
  outcome.fired = value == 7;
  values.Close();
  loop.RequestStop();
}

struct TickerOutcome {
  int ticks{0};
  bool increasing{true};
  alyrn::time::Duration elapsed{};
  bool one_backlogged{false};
  bool no_second_backlogged{false};
};

template <class B>
Task<void> CountTicks(typename B::Loop& loop, TickerOutcome& outcome) {
  typename B::Ticker ticker(loop, 5ms);
  const Deadline start = alyrn::time::SteadyNow();
  Deadline previous = start;
  for (int i = 0; i < 5; ++i) {
    std::optional<Deadline> tick;
    (void)co_await (ticker >> tick);
    if (!tick || *tick <= previous) outcome.increasing = false;
    if (tick) previous = *tick;
    ++outcome.ticks;
  }
  outcome.elapsed = alyrn::time::SteadyNow() - start;

  // A receiver that falls behind finds one tick waiting, not a backlog.
  (void)co_await Nap(loop, 30ms);
  std::optional<Deadline> tick;
  outcome.one_backlogged = co_await Select(ticker >> tick, SelectDefault()) == 0 && tick;
  outcome.no_second_backlogged = co_await Select(ticker >> tick, SelectDefault()) == 1;
  loop.RequestStop();
}

struct StopResetOutcome {
  bool stop_reported_pending{false};
  bool silent_after_stop{false};
  bool stop_after_fire_reported_idle{false};
  bool fired_after_reset{false};
};

template <class B>
Task<void> StopAndReset(typename B::Loop& loop, StopResetOutcome& outcome) {
  typename B::Timer timer(loop, 10ms);
  outcome.stop_reported_pending = timer.Stop();
  (void)co_await Nap(loop, 30ms);
  std::optional<Deadline> fired;
  outcome.silent_after_stop = co_await Select(timer >> fired, SelectDefault()) == 1;

  timer.Reset(5ms);
  (void)co_await (timer >> fired);
  outcome.fired_after_reset = fired.has_value();
  outcome.stop_after_fire_reported_idle = !timer.Stop();
  loop.RequestStop();
}

struct StopWakeOutcome {
  bool resumed{false};
  bool empty{false};
};

// A receive waiting on a ticker when the loop stops completes with an empty
// optional instead of leaving the coroutine parked forever.
template <class B>
alyrn::coro::DetachedTask WaitForever(typename B::Loop& loop, StopWakeOutcome& outcome) {
  typename B::Ticker ticker(loop, 1h);
  std::optional<Deadline> tick;
  (void)co_await (ticker >> tick);
  outcome.resumed = true;
  outcome.empty = !tick.has_value();
}

template <class B>
bool RunBackend() {
  bool ok = true;
  const char* name = B::kName;
  bool skipped = false;
  {
    typename B::Loop loop;
    if (!B::Init(loop, skipped)) {
      if (skipped) std::cout << "SKIP [" << name << "]: io_uring unavailable\n";
      return skipped;
    }
    Channel<int> values{loop, 0};
    Outcome outcome;
    auto root = alyrn::Spawn(loop, SelectTimesOut<B>(loop, values, outcome));
    loop.Run();
    ok = Check(root.IsFinished() && outcome.selected == 1 && outcome.fired, name,
               "an idle Select must take the timer case") &&
         Check(outcome.received_at - outcome.started >= 20ms, name,
               "the timer must not fire before its delay") &&
         ok;
  }
  {
    typename B::Loop loop;
    (void)B::Init(loop, skipped);
    Channel<int> values{loop, 0};
    Outcome outcome;
    auto root = alyrn::Spawn(loop, SelectGetsValueFirst<B>(loop, values, outcome));
    auto sender = alyrn::Spawn(loop, SendLater<B>(loop, values));
    loop.Run();
    ok = Check(root.IsFinished() && outcome.selected == 0 && outcome.fired, name,
               "a value that arrives first must win over the timer") &&
         ok;
  }
  {
    typename B::Loop loop;
    (void)B::Init(loop, skipped);
    TickerOutcome outcome;
    auto root = alyrn::Spawn(loop, CountTicks<B>(loop, outcome));
    loop.Run();
    ok = Check(root.IsFinished() && outcome.ticks == 5 && outcome.increasing, name,
               "a ticker must deliver increasing firing times") &&
         Check(outcome.elapsed >= 20ms, name, "five ticks must span at least four periods") &&
         Check(outcome.one_backlogged && outcome.no_second_backlogged, name,
               "a slow receiver must find exactly one pending tick") &&
         ok;
  }
  {
    typename B::Loop loop;
    (void)B::Init(loop, skipped);
    StopResetOutcome outcome;
    auto root = alyrn::Spawn(loop, StopAndReset<B>(loop, outcome));
    loop.Run();
    ok = Check(root.IsFinished() && outcome.stop_reported_pending, name,
               "Stop must report a pending timer") &&
         Check(outcome.silent_after_stop, name, "a stopped timer must not fire") &&
         Check(outcome.fired_after_reset, name, "Reset must re-arm a stopped timer") &&
         Check(outcome.stop_after_fire_reported_idle, name,
               "Stop after the firing was received must report an idle timer") &&
         ok;
  }
  {
    StopWakeOutcome outcome;
    {
      typename B::Loop loop;
      (void)B::Init(loop, skipped);
      alyrn::coro::SpawnDetach(loop, WaitForever<B>(loop, outcome));
      (void)loop.RunAfter(10ms, [&loop] { loop.RequestStop(); });
      loop.Run();
    }
    ok = Check(outcome.resumed && outcome.empty, name,
               "a receive pending at loop stop must complete with an empty optional") &&
         ok;
  }
  {
    // Created before Run() and destroyed after it, outside the scheduler:
    // reaching the end without a channel-owner panic is the check.
    typename B::Loop loop;
    (void)B::Init(loop, skipped);
    typename B::Timer idle(loop, 1h);
    typename B::Ticker ticker(loop, 1h);
    loop.RequestStop();
    loop.Run();
  }
  if (ok) std::cout << "timer channel [" << name << "]: PASS\n";
  return ok;
}

}  // namespace

int main() {
  bool ok = RunBackend<EpollBackend>();
#if defined(ALYRN_ENABLE_URING)
  ok = RunBackend<UringBackend>() && ok;
#endif
  return ok ? EXIT_SUCCESS : EXIT_FAILURE;
}
