// SPDX-License-Identifier: MIT
//
// Measures owner-thread Channel send/receive pairs. This deliberately excludes
// cross-thread handoff: that is a backend mailbox concern, not channel cost.
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
#include <optional>

#include "alyrn/coro/channel.h"
#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/coro/task.h"
#include "alyrn/coro/work.h"
#include "alyrn/detail/check.h"

namespace {

using alyrn::coro::Channel;
using alyrn::coro::Scheduler;
using alyrn::coro::Spawn;
using alyrn::coro::Task;
using alyrn::coro::Work;
using alyrn::coro::WorkQueue;

class DrainScheduler final : public Scheduler {
public:
  void Schedule(Work* work) noexcept override {
    ALYRN_CHECK(queue_.PushBack(work), "channel microbenchmark scheduler queue overflow");
  }

  void Drain() noexcept {
    while (Work* work = queue_.PopFront()) {
      Run(work);
    }
  }

private:
  WorkQueue queue_;
};

Task<void> RunBufferedPairs(Channel<std::uint64_t>& channel, std::uint64_t iterations,
                            std::uint64_t& checksum) {
  for (std::uint64_t value = 0; value < iterations; ++value) {
    ALYRN_CHECK((co_await (channel << value)).HasValue(), "channel microbenchmark send failed");
    std::optional<std::uint64_t> received;
    ALYRN_CHECK((co_await (channel >> received)).HasValue() && received.has_value(),
                "channel microbenchmark receive failed");
    checksum += *received;
  }
  channel.Close();
  co_return;
}

Task<void> SendRendezvousValues(Channel<std::uint64_t>& channel, std::uint64_t iterations) {
  for (std::uint64_t value = 0; value < iterations; ++value) {
    ALYRN_CHECK((co_await (channel << value)).HasValue(),
                "channel rendezvous benchmark send failed");
  }
  channel.Close();
  co_return;
}

Task<void> ReceiveRendezvousValues(Channel<std::uint64_t>& channel, std::uint64_t iterations,
                                   std::uint64_t& checksum) {
  for (std::uint64_t value = 0; value < iterations; ++value) {
    std::optional<std::uint64_t> received;
    ALYRN_CHECK((co_await (channel >> received)).HasValue() && received.has_value(),
                "channel rendezvous benchmark receive failed");
    checksum += *received;
  }
  co_return;
}

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
  const auto expected = (iterations - 1) * iterations / 2;
  std::cout << "record,iterations,elapsed_ns,ns_per_pair,checksum\n";

  {
    DrainScheduler scheduler;
    Channel<std::uint64_t> channel{scheduler, 1};
    std::uint64_t checksum = 0;
    auto task = Spawn(scheduler, RunBufferedPairs(channel, iterations, checksum));

    const auto started = std::chrono::steady_clock::now();
    scheduler.Drain();
    const auto elapsed = std::chrono::steady_clock::now() - started;
    const auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();
    task.Wait();
    ALYRN_CHECK(checksum == expected, "buffered channel benchmark checksum mismatch");

    std::cout << "channel_buffered_send_receive," << iterations << ',' << nanos << ','
              << static_cast<double>(nanos) / static_cast<double>(iterations) << ',' << checksum
              << '\n';
  }

  {
    DrainScheduler scheduler;
    Channel<std::uint64_t> channel{scheduler, 0};
    std::uint64_t checksum = 0;
    auto sender = Spawn(scheduler, SendRendezvousValues(channel, iterations));
    auto receiver = Spawn(scheduler, ReceiveRendezvousValues(channel, iterations, checksum));

    const auto started = std::chrono::steady_clock::now();
    scheduler.Drain();
    const auto elapsed = std::chrono::steady_clock::now() - started;
    const auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();
    sender.Wait();
    receiver.Wait();
    ALYRN_CHECK(checksum == expected, "rendezvous channel benchmark checksum mismatch");

    std::cout << "channel_rendezvous_send_receive," << iterations << ',' << nanos << ','
              << static_cast<double>(nanos) / static_cast<double>(iterations) << ',' << checksum
              << '\n';
  }
}
