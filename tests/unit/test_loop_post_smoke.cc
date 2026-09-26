// SPDX-License-Identifier: MIT
// Loop::Post hands work to a Loop from any thread, for every backend.

#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <system_error>
#include <thread>
#include <vector>

#include "alyrn/coro/scheduler.h"
#include "alyrn/epoll.h"

#if defined(ALYRN_ENABLE_URING)
#include "alyrn/uring.h"
#endif

namespace {

using namespace std::chrono_literals;

bool Check(bool condition, const char* message) {
  if (!condition) std::cerr << "FAIL: " << message << '\n';
  return condition;
}

// Stops a stuck scenario instead of letting the test hang.
void ArmWatchdog(alyrn::epoll::Loop& loop) {
  (void)loop.RunAfter(10s, [&loop] { loop.RequestStop(); });
}

#if defined(ALYRN_ENABLE_URING)
void ArmWatchdog(alyrn::uring::Loop& loop) {
  (void)loop.RunAfter(10s, [&loop] { loop.RequestStop(); });
}
#endif

// Several producer threads post while the loop runs: every callback runs
// exactly once, on the owner thread in its scheduling context, and callbacks
// from one producer keep their order.
template <class Loop>
bool CheckCrossThreadPosts(Loop& loop, const char* name) {
  constexpr int kProducers = 4;
  constexpr int kPostsPerProducer = 2000;
  constexpr int kTotal = kProducers * kPostsPerProducer;

  std::vector<int> next_sequence(kProducers, 0);  // touched only on the owner
  int executed = 0;
  bool ordered = true;
  bool on_owner = true;
  std::atomic<int> rejected{0};

  ArmWatchdog(loop);
  std::vector<std::jthread> producers;
  for (int producer = 0; producer < kProducers; ++producer) {
    producers.emplace_back([&, producer] {
      for (int sequence = 0; sequence < kPostsPerProducer; ++sequence) {
        auto posted = loop.Post([&, producer, sequence] {
          if (!loop.IsInLoopThread() || alyrn::coro::Scheduler::TryCurrent() != &loop) {
            on_owner = false;
          }
          if (next_sequence[producer] != sequence) {
            ordered = false;
          }
          next_sequence[producer] = sequence + 1;
          if (++executed == kTotal) {
            loop.RequestStop();
          }
        });
        if (!posted.HasValue()) {
          ++rejected;
        }
      }
    });
  }
  loop.Run();
  producers.clear();

  std::cerr << name << ": " << executed << " of " << kTotal << " posts ran\n";
  return Check(rejected.load() == 0, "posts made while the loop runs must be accepted") &&
         Check(executed == kTotal, "every accepted post must run exactly once") &&
         Check(ordered, "posts from one thread must run in order") &&
         Check(on_owner, "posts must run on the owner thread in its scheduling context");
}

// Posts made before Run() run once it starts; posts made while stopping run
// during the final drain; posts after Run() returns are rejected and their
// callback is destroyed on the calling thread without running.
template <class Loop>
bool CheckPostLifecycle(Loop& loop) {
  bool early_ran = false;
  bool stopping_ran = false;
  ArmWatchdog(loop);
  auto early = loop.Post([&] {
    early_ran = true;
    loop.RequestStop();
    (void)loop.Post([&] { stopping_ran = true; });
  });
  loop.Run();

  auto token = std::make_shared<int>(0);
  std::weak_ptr<int> watched = token;
  bool late_ran = false;
  auto late = loop.Post([token = std::move(token), &late_ran] { late_ran = true; });

  return Check(early.HasValue() && early_ran, "a post made before Run() must run") &&
         Check(stopping_ran, "a post made while stopping must run during the final drain") &&
         Check(!late.HasValue() && late.Error() == std::errc::operation_canceled,
               "a post after Run() returns must be rejected") &&
         Check(!late_ran && watched.expired(),
               "a rejected callback must be destroyed on the caller without running");
}

bool CheckEpoll() {
  bool ok = true;
  {
    alyrn::epoll::Loop loop;
    ok = CheckCrossThreadPosts(loop, "epoll") && ok;
  }
  {
    alyrn::epoll::Loop loop;
    ok = CheckPostLifecycle(loop) && ok;
  }
  return ok;
}

#if defined(ALYRN_ENABLE_URING)
bool InitUring(alyrn::uring::Loop& loop, bool& skipped) {
  alyrn::uring::Options options;
  options.entries = 64;
  auto initialized = loop.Init(options);
  if (initialized.HasValue()) {
    return true;
  }
  skipped = initialized.Error() == std::errc::operation_not_supported ||
            initialized.Error() == std::errc::operation_not_permitted;
  if (skipped) {
    std::cout << "SKIP: io_uring unavailable: " << initialized.Error().message() << '\n';
  }
  return false;
}

bool CheckUring() {
  bool ok = true;
  bool skipped = false;
  {
    alyrn::uring::Loop loop;
    if (!InitUring(loop, skipped)) {
      return skipped;
    }
    ok = CheckCrossThreadPosts(loop, "uring") && ok;
  }
  {
    alyrn::uring::Loop loop;
    if (!InitUring(loop, skipped)) {
      return skipped;
    }
    ok = CheckPostLifecycle(loop) && ok;
  }
  return ok;
}
#endif

}  // namespace

int main() {
  bool ok = CheckEpoll();
#if defined(ALYRN_ENABLE_URING)
  ok = CheckUring() && ok;
#endif
  if (!ok) {
    return 1;
  }
  std::cout << "loop post smoke: PASS\n";
  return 0;
}
