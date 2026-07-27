// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Lifecycle benchmark for SpawnRoot ownership and join scheduling. It keeps
// the scheduler deterministic so the measurements cover coroutine and state
// transitions rather than an executor implementation.
//
// Build:
//   cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_BENCHMARKS=ON
//   cmake --build build --target coro_spawn_join_microbenchmark -j
//
// Run both frame-resource modes (the default):
//   build/benchmarks/coro_spawn_join_microbenchmark
//
// Run one mode or change the iteration count:
//   FRAME_POOL=0 ITERATIONS=100000 build/benchmarks/coro_spawn_join_microbenchmark
//   FRAME_POOL=1 ITERATIONS=100000 build/benchmarks/coro_spawn_join_microbenchmark
//
// wait_sum_ms measures the synchronous Wait call or the async co_await window.
// For detach scenarios it is zero because no wait is requested. completion_sum_ms
// measures spawn-to-child-completion latency summed over all iterations.

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <memory_resource>
#include <optional>
#include <string_view>
#include <utility>

#include "coropact/coro/detail/spawn_stats.h"
#include "coropact/coro/frame_allocator.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/spawn.h"
#include "coropact/coro/task.h"
#include "coropact/coro/work.h"
#include "coropact/memory/pmr_pool_resource.h"

namespace {

using coropact::coro::CoroFramePoolResource;
using coropact::coro::DetachedTask;
using coropact::coro::FrameAllocatorScope;
using coropact::coro::JoinHandle;
using coropact::coro::Scheduler;
using coropact::coro::Spawn;
using coropact::coro::Task;
using coropact::coro::Work;
using coropact::coro::WorkQueue;
using coropact::coro::detail::SpawnAllocationScope;
using coropact::coro::detail::SpawnAllocationStats;
using coropact::memory::CountingMemoryResource;
using coropact::memory::MemoryResourceStats;

using Clock = std::chrono::steady_clock;

enum class Scenario {
  kSpawnWait,
  kSpawnAsyncJoin,
  kSpawnDetachBeforeCompletion,
  kSpawnDetachAfterCompletion,
  kCrossSchedulerAsyncJoin,
};

const char* ScenarioName(Scenario scenario) noexcept {
  switch (scenario) {
    case Scenario::kSpawnWait:
      return "Spawn+Wait";
    case Scenario::kSpawnAsyncJoin:
      return "Spawn+async co_await JoinHandle";
    case Scenario::kSpawnDetachBeforeCompletion:
      return "Spawn+Detach before completion";
    case Scenario::kSpawnDetachAfterCompletion:
      return "Spawn+Detach after completion";
    case Scenario::kCrossSchedulerAsyncJoin:
      return "cross Scheduler async join";
  }
  return "unknown";
}

class DrainScheduler final : public Scheduler {
public:
  explicit DrainScheduler(std::pmr::memory_resource* resource) noexcept : Scheduler(resource) {}

  void Schedule(Work* work) noexcept override {
    const bool queued = queue_.PushBack(work);
    assert(queued);
    (void)queued;
  }

  bool DrainOne() noexcept {
    Work* work = queue_.PopFront();
    if (work == nullptr) {
      return false;
    }
    Run(work);
    return true;
  }

  void Drain() noexcept {
    while (DrainOne()) {
    }
  }

private:
  WorkQueue queue_;
};

struct ManualGate {
  struct Awaiter {
    ManualGate* gate;

    [[nodiscard]]
    bool await_ready() const noexcept {
      return false;
    }

    [[nodiscard]]
    bool await_suspend(std::coroutine_handle<> waiter) noexcept {
      gate->resume_work.SetHandle(waiter);
      return true;
    }

    void await_resume() const noexcept {}
  };

  Awaiter Wait() noexcept { return Awaiter{this}; }

  void Open(DrainScheduler& scheduler) noexcept {
    assert(resume_work.HasHandle());
    scheduler.Schedule(&resume_work);
  }

  coropact::coro::ResumeWork resume_work;
};

struct Probe {
  Clock::time_point started;
  std::uint64_t completion_ns{0};
  std::uint64_t wait_ns{0};
  std::uint64_t completed{0};

  void MarkCompleted() noexcept {
    completion_ns += static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - started).count());
    ++completed;
  }
};

std::uint64_t ElapsedNs(Clock::time_point begin, Clock::time_point end) noexcept {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count());
}

Task<void> GatedChild(ManualGate* gate, Probe* probe) {
  co_await gate->Wait();
  probe->MarkCompleted();
}

Task<void> ImmediateChild(Probe* probe) {
  probe->MarkCompleted();
  co_return;
}

Task<void> AsyncJoinChild(JoinHandle<void> child, Probe* probe) {
  const auto wait_begin = Clock::now();
  (void)co_await std::move(child);
  probe->wait_ns += ElapsedNs(wait_begin, Clock::now());
}

class FrameResourceContext {
public:
  explicit FrameResourceContext(bool pooled) {
    if (pooled) {
      pool_.emplace();
    }
    std::pmr::memory_resource* base =
        pooled ? static_cast<std::pmr::memory_resource*>(std::addressof(*pool_))
               : std::pmr::new_delete_resource();
    counted_ = std::make_unique<CountingMemoryResource>(*base, stats_);
  }

  std::pmr::memory_resource* resource() noexcept { return counted_.get(); }

  const MemoryResourceStats& stats() const noexcept { return stats_; }

private:
  std::optional<CoroFramePoolResource> pool_;
  MemoryResourceStats stats_;
  std::unique_ptr<CountingMemoryResource> counted_;
};

template <class Factory>
JoinHandle<void> SpawnWithResource(FrameResourceContext& resource, Scheduler& scheduler,
                                   Factory&& factory) {
  FrameAllocatorScope frame_scope{*resource.resource()};
  return Spawn(scheduler, std::invoke(std::forward<Factory>(factory)));
}

void Require(bool condition, const char* message) {
  if (!condition) {
    std::cerr << "benchmark invariant failed: " << message << '\n';
    std::exit(1);
  }
}

struct TimingTotals {
  std::uint64_t wait_ns{0};
  std::uint64_t completion_ns{0};
  std::uint64_t completed{0};
};

TimingTotals RunSpawnWait(FrameResourceContext& resource, std::uint64_t iterations) {
  DrainScheduler scheduler{resource.resource()};
  TimingTotals totals;
  for (std::uint64_t i = 0; i < iterations; ++i) {
    ManualGate gate;
    Probe probe{.started = Clock::now()};
    auto child = SpawnWithResource(resource, scheduler, [&] { return GatedChild(&gate, &probe); });
    Require(scheduler.DrainOne(), "Spawn+Wait child should start");
    gate.Open(scheduler);
    scheduler.Drain();

    const auto wait_begin = Clock::now();
    (void)child.Wait();
    probe.wait_ns += ElapsedNs(wait_begin, Clock::now());
    totals.wait_ns += probe.wait_ns;
    totals.completion_ns += probe.completion_ns;
    totals.completed += probe.completed;
  }
  return totals;
}

TimingTotals RunSpawnAsyncJoin(FrameResourceContext& resource, std::uint64_t iterations) {
  DrainScheduler scheduler{resource.resource()};
  TimingTotals totals;
  for (std::uint64_t i = 0; i < iterations; ++i) {
    ManualGate gate;
    Probe probe{.started = Clock::now()};
    auto child = SpawnWithResource(resource, scheduler, [&] { return GatedChild(&gate, &probe); });
    auto parent = SpawnWithResource(resource, scheduler,
                                    [&] { return AsyncJoinChild(std::move(child), &probe); });

    Require(scheduler.DrainOne(), "async join child should start");
    Require(scheduler.DrainOne(), "async join parent should park");
    gate.Open(scheduler);
    scheduler.Drain();
    (void)parent.Wait();

    totals.wait_ns += probe.wait_ns;
    totals.completion_ns += probe.completion_ns;
    totals.completed += probe.completed;
  }
  return totals;
}

TimingTotals RunSpawnDetachBeforeCompletion(FrameResourceContext& resource,
                                            std::uint64_t iterations) {
  DrainScheduler scheduler{resource.resource()};
  TimingTotals totals;
  for (std::uint64_t i = 0; i < iterations; ++i) {
    ManualGate gate;
    Probe probe{.started = Clock::now()};
    auto child = SpawnWithResource(resource, scheduler, [&] { return GatedChild(&gate, &probe); });
    child.Detach();
    Require(scheduler.DrainOne(), "detach-before child should start");
    gate.Open(scheduler);
    scheduler.Drain();

    totals.completion_ns += probe.completion_ns;
    totals.completed += probe.completed;
  }
  return totals;
}

TimingTotals RunSpawnDetachAfterCompletion(FrameResourceContext& resource,
                                           std::uint64_t iterations) {
  DrainScheduler scheduler{resource.resource()};
  TimingTotals totals;
  for (std::uint64_t i = 0; i < iterations; ++i) {
    Probe probe{.started = Clock::now()};
    auto child = SpawnWithResource(resource, scheduler, [&] { return ImmediateChild(&probe); });
    scheduler.Drain();
    child.Detach();

    totals.completion_ns += probe.completion_ns;
    totals.completed += probe.completed;
  }
  return totals;
}

TimingTotals RunCrossSchedulerAsyncJoin(FrameResourceContext& parent_resource,
                                        FrameResourceContext& child_resource,
                                        std::uint64_t iterations) {
  DrainScheduler parent_scheduler{parent_resource.resource()};
  DrainScheduler child_scheduler{child_resource.resource()};
  TimingTotals totals;
  for (std::uint64_t i = 0; i < iterations; ++i) {
    ManualGate gate;
    Probe probe{.started = Clock::now()};
    auto child = SpawnWithResource(child_resource, child_scheduler,
                                   [&] { return GatedChild(&gate, &probe); });
    auto parent = SpawnWithResource(parent_resource, parent_scheduler,
                                    [&] { return AsyncJoinChild(std::move(child), &probe); });

    Require(child_scheduler.DrainOne(), "cross-scheduler child should start");
    Require(parent_scheduler.DrainOne(), "cross-scheduler parent should park");
    gate.Open(child_scheduler);
    child_scheduler.Drain();
    parent_scheduler.Drain();
    (void)parent.Wait();

    totals.wait_ns += probe.wait_ns;
    totals.completion_ns += probe.completion_ns;
    totals.completed += probe.completed;
  }
  return totals;
}

MemoryResourceStats AddStats(const MemoryResourceStats& first,
                             const MemoryResourceStats& second) noexcept {
  return MemoryResourceStats{
      .allocate_calls = first.allocate_calls + second.allocate_calls,
      .deallocate_calls = first.deallocate_calls + second.deallocate_calls,
      .allocated_bytes = first.allocated_bytes + second.allocated_bytes,
      .deallocated_bytes = first.deallocated_bytes + second.deallocated_bytes,
      .outstanding_allocations = first.outstanding_allocations + second.outstanding_allocations,
      .outstanding_bytes = first.outstanding_bytes + second.outstanding_bytes,
      .peak_outstanding_allocations =
          first.peak_outstanding_allocations + second.peak_outstanding_allocations,
      .peak_outstanding_bytes = first.peak_outstanding_bytes + second.peak_outstanding_bytes,
  };
}

struct BenchmarkResult {
  Scenario scenario;
  bool frame_pool;
  std::uint64_t iterations;
  double wall_ms;
  std::uint64_t wait_ns;
  std::uint64_t completion_ns;
  MemoryResourceStats frame_stats;
  SpawnAllocationStats spawn_stats;
  std::uint64_t completed;
};

BenchmarkResult RunScenario(Scenario scenario, bool frame_pool, std::uint64_t iterations) {
  FrameResourceContext parent_resource{frame_pool};
  FrameResourceContext child_resource{frame_pool};
  SpawnAllocationStats spawn_stats;
  SpawnAllocationScope spawn_scope{spawn_stats};
  const auto started = Clock::now();

  TimingTotals totals;
  switch (scenario) {
    case Scenario::kSpawnWait:
      totals = RunSpawnWait(parent_resource, iterations);
      break;
    case Scenario::kSpawnAsyncJoin:
      totals = RunSpawnAsyncJoin(parent_resource, iterations);
      break;
    case Scenario::kSpawnDetachBeforeCompletion:
      totals = RunSpawnDetachBeforeCompletion(parent_resource, iterations);
      break;
    case Scenario::kSpawnDetachAfterCompletion:
      totals = RunSpawnDetachAfterCompletion(parent_resource, iterations);
      break;
    case Scenario::kCrossSchedulerAsyncJoin:
      totals = RunCrossSchedulerAsyncJoin(parent_resource, child_resource, iterations);
      break;
  }

  Require(totals.completed == iterations, "all scenario children should complete");
  const auto ended = Clock::now();
  return BenchmarkResult{
      .scenario = scenario,
      .frame_pool = frame_pool,
      .iterations = iterations,
      .wall_ms = std::chrono::duration<double, std::milli>(ended - started).count(),
      .wait_ns = totals.wait_ns,
      .completion_ns = totals.completion_ns,
      .frame_stats = AddStats(parent_resource.stats(), child_resource.stats()),
      .spawn_stats = spawn_stats,
      .completed = totals.completed,
  };
}

template <class T>
T Median(std::array<T, 5> values) {
  std::sort(values.begin(), values.end());
  return values[2];
}

BenchmarkResult MedianResult(const std::array<BenchmarkResult, 5>& samples) {
  BenchmarkResult result = samples[0];
  std::array<double, 5> wall_ms{};
  std::array<std::uint64_t, 5> wait_ns{};
  std::array<std::uint64_t, 5> completion_ns{};
  for (std::size_t i = 0; i < samples.size(); ++i) {
    wall_ms[i] = samples[i].wall_ms;
    wait_ns[i] = samples[i].wait_ns;
    completion_ns[i] = samples[i].completion_ns;
  }
  result.wall_ms = Median(wall_ms);
  result.wait_ns = Median(wait_ns);
  result.completion_ns = Median(completion_ns);
  return result;
}

void PrintResult(const char* record, std::size_t round, const BenchmarkResult& result) {
  const auto& frame = result.frame_stats;
  const auto& spawn = result.spawn_stats;
  std::cout << record << ',' << (result.frame_pool ? 1 : 0) << ',' << result.iterations << ','
            << round << ',' << ScenarioName(result.scenario) << ',' << result.wall_ms << ','
            << static_cast<double>(result.wait_ns) / 1'000'000.0 << ','
            << static_cast<double>(result.completion_ns) / 1'000'000.0 << ','
            << frame.allocate_calls << ',' << frame.allocated_bytes << ',' << spawn.allocate_calls
            << ',' << spawn.allocated_bytes << ',' << result.completed << '\n';
}

bool ParseFramePoolSelection(bool* values, std::size_t* count) {
  const char* raw = std::getenv("FRAME_POOL");
  if (raw == nullptr || std::string_view(raw) == "both" || std::string_view(raw) == "all") {
    values[0] = false;
    values[1] = true;
    *count = 2;
    return true;
  }
  if (std::string_view(raw) == "0") {
    values[0] = false;
    *count = 1;
    return true;
  }
  if (std::string_view(raw) == "1") {
    values[0] = true;
    *count = 1;
    return true;
  }
  std::cerr << "FRAME_POOL must be 0, 1, both, or all\n";
  return false;
}

std::uint64_t ParseIterations() {
  const char* raw = std::getenv("ITERATIONS");
  if (raw == nullptr) {
    return 100'000;
  }
  char* end = nullptr;
  const auto value = std::strtoull(raw, &end, 10);
  if (end == raw || *end != '\0' || value == 0) {
    std::cerr << "ITERATIONS must be a positive integer\n";
    std::exit(2);
  }
  return static_cast<std::uint64_t>(value);
}

}  // namespace

int main() {
  constexpr std::array<Scenario, 5> kScenarios{
      Scenario::kSpawnWait,
      Scenario::kSpawnAsyncJoin,
      Scenario::kSpawnDetachBeforeCompletion,
      Scenario::kSpawnDetachAfterCompletion,
      Scenario::kCrossSchedulerAsyncJoin,
  };
  constexpr std::size_t kRounds = 5;

  bool frame_pool_values[2]{};
  std::size_t frame_pool_count = 0;
  if (!ParseFramePoolSelection(frame_pool_values, &frame_pool_count)) {
    return 2;
  }
  const std::uint64_t iterations = ParseIterations();

  std::cout << "record,frame_pool,iterations,round,scenario,wall_ms,wait_sum_ms,"
               "completion_sum_ms,frame_alloc_calls,frame_allocated_bytes,"
               "spawn_state_alloc_calls,spawn_state_allocated_bytes,completed\n";
  std::cout << std::fixed << std::setprecision(3);

  for (std::size_t pool_index = 0; pool_index < frame_pool_count; ++pool_index) {
    for (const Scenario scenario : kScenarios) {
      std::array<BenchmarkResult, kRounds> samples{};
      for (std::size_t round = 0; round < kRounds; ++round) {
        samples[round] = RunScenario(scenario, frame_pool_values[pool_index], iterations);
        PrintResult("sample", round + 1, samples[round]);
      }
      PrintResult("median", 0, MedianResult(samples));
    }
  }
}
