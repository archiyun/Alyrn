// SPDX-License-Identifier: MIT
//
// Measures owner-thread Channel TrySend/TryReceive pairs. This deliberately
// excludes cross-thread handoff and coroutine scheduling: those are backend
// mailbox/scheduler costs, not channel-buffer costs.
//
// Build:
//   cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_BENCHMARKS=ON
//   cmake --build build --target coro_channel_microbenchmark -j
// Run:
//   ITERATIONS=1000000 build/benchmarks/coro_channel_microbenchmark

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>

#include "alyrn/detail/check.h"
#include "alyrn/coro/channel.h"
#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/work.h"

namespace {

using alyrn::coro::Channel;
using alyrn::coro::Scheduler;
using alyrn::coro::Work;

class InlineScheduler final : public Scheduler {
public:
  void Schedule(Work*) noexcept override {
    ALYRN_CHECK(false, "channel microbenchmark must not schedule coroutine work");
  }
};

class ChannelWork final : public Work {
public:
  ChannelWork(Channel<std::uint64_t>& channel, std::uint64_t iterations) noexcept
      : channel_(&channel), iterations_(iterations) {
    SetRun([](Work* work) noexcept { static_cast<ChannelWork*>(work)->RunPairs(); });
  }

  std::uint64_t checksum() const noexcept {
    return checksum_;
  }

private:
  void RunPairs() noexcept {
    for (std::uint64_t value = 0; value < iterations_; ++value) {
      ALYRN_CHECK(channel_->TrySend(std::move(value)).HasValue(),
                     "channel microbenchmark send failed");
      auto received = channel_->TryReceive();
      ALYRN_CHECK(received.HasValue() && received->has_value(),
                     "channel microbenchmark receive failed");
      checksum_ += **received;
    }
  }

  Channel<std::uint64_t>* channel_;
  std::uint64_t iterations_;
  std::uint64_t checksum_{0};
};

std::uint64_t ReadIterations() {
  constexpr std::uint64_t kDefaultIterations = 1'000'000;
  const char* value = std::getenv("ITERATIONS");
  if (value == nullptr) return kDefaultIterations;
  char* end = nullptr;
  const unsigned long long parsed = std::strtoull(value, &end, 10);
  return end != value && *end == '\0' && parsed != 0 ? parsed : kDefaultIterations;
}

}  // namespace

int main() {
  const std::uint64_t iterations = ReadIterations();
  InlineScheduler scheduler;
  Channel<std::uint64_t> channel{scheduler, 1};
  ChannelWork work{channel, iterations};

  const auto started = std::chrono::steady_clock::now();
  scheduler.Run(&work);
  const auto elapsed = std::chrono::steady_clock::now() - started;
  const auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();
  const auto expected = (iterations - 1) * iterations / 2;
  ALYRN_CHECK(work.checksum() == expected, "channel microbenchmark checksum mismatch");

  std::cout << "record,iterations,elapsed_ns,ns_per_pair,checksum\n"
            << "channel_try_send_receive," << iterations << ',' << nanos << ','
            << static_cast<double>(nanos) / static_cast<double>(iterations) << ','
            << work.checksum() << '\n';
}
