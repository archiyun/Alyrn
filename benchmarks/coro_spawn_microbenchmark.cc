// SPDX-License-Identifier: MIT
//
// Pure coroutine microbenchmark for the joinable-then-detach path versus the
// dedicated fire-and-forget path. It does not use a network backend, a timer,
// or io_uring.
//
// Build:
//   cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_BENCHMARKS=ON
//   cmake --build build --target coro_spawn_microbenchmark -j
//
// Run both frame-resource modes (the default):
//   build/benchmarks/coro_spawn_microbenchmark
//
// Run one frame-resource mode:
//   FRAME_POOL=0 build/benchmarks/coro_spawn_microbenchmark
//   FRAME_POOL=1 build/benchmarks/coro_spawn_microbenchmark
//
// Each scenario uses a fresh scheduler and frame resource and runs five
// rounds. Odd/even rounds alternate which Spawn path runs first. Tasks are
// submitted in batches and drained before the next batch so one million
// operations do not create an unrelated one-million-frame live set.
// frame_allocated_bytes is the byte count passed to the selected frame
// resource, including the frame allocator's header/alignment overhead.
// spawn_state_allocated_bytes is retained as a regression counter for a
// standalone SpawnState allocation; the embedded SpawnRoot path reports zero.

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory_resource>
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
using coropact::coro::Scheduler;
using coropact::coro::Spawn;
using coropact::coro::SpawnDetach;
using coropact::coro::Task;
using coropact::coro::Work;
using coropact::coro::WorkQueue;
using coropact::coro::detail::SpawnAllocationScope;
using coropact::coro::detail::SpawnAllocationStats;
using coropact::memory::CountingMemoryResource;
using coropact::memory::MemoryResourceStats;

enum class SpawnMode { kSpawnThenDetach, kSpawnDetach };

struct BenchmarkResult {
  SpawnMode mode;
  bool frame_pool;
  std::uint64_t iterations;
  double elapsed_ms;
  MemoryResourceStats frame_stats;
  SpawnAllocationStats spawn_stats;
  std::uint64_t completed;
};

class DrainScheduler final : public Scheduler {
public:
  explicit DrainScheduler(std::pmr::memory_resource& frame_resource) noexcept
      : Scheduler(&frame_resource) {}

  void Schedule(Work* work) noexcept override {
    const bool queued = queue_.PushBack(work);
    assert(queued);
    (void)queued;
  }

  void Drain() noexcept {
    while (Work* work = queue_.PopFront()) {
      Run(work);
    }
  }

private:
  WorkQueue queue_;
};

Task<void> MicroTask(std::uint64_t* completed) {
  ++*completed;
  co_return;
}

DetachedTask MicroDetachedTask(std::uint64_t* completed) {
  ++*completed;
  co_return;
}

template <typename FrameResource>
BenchmarkResult RunBenchmark(FrameResource& frame_resource, SpawnMode mode, bool frame_pool,
                             std::uint64_t iterations) {
  constexpr std::uint64_t kBatchSize = 1024;

  DrainScheduler scheduler{frame_resource};
  std::uint64_t completed = 0;
  const auto start = std::chrono::steady_clock::now();

  for (std::uint64_t begin = 0; begin < iterations; begin += kBatchSize) {
    const std::uint64_t batch_size = std::min(kBatchSize, iterations - begin);
    {
      FrameAllocatorScope frame_scope{frame_resource};
      for (std::uint64_t i = 0; i < batch_size; ++i) {
        if (mode == SpawnMode::kSpawnThenDetach) {
          Spawn(scheduler, MicroTask(&completed)).Detach();
        } else {
          SpawnDetach(scheduler, MicroDetachedTask(&completed));
        }
      }
    }
    scheduler.Drain();
  }

  const auto elapsed = std::chrono::steady_clock::now() - start;
  const double elapsed_ms = std::chrono::duration<double, std::milli>(elapsed).count();

  return BenchmarkResult{
      .mode = mode,
      .frame_pool = frame_pool,
      .iterations = iterations,
      .elapsed_ms = elapsed_ms,
      .frame_stats = {},
      .completed = completed,
  };
}

template <typename FrameResource>
BenchmarkResult RunCountedBenchmark(FrameResource& frame_resource, SpawnMode mode, bool frame_pool,
                                    std::uint64_t iterations, MemoryResourceStats& frame_stats,
                                    SpawnAllocationStats& spawn_stats) {
  CountingMemoryResource counted_resource{frame_resource, frame_stats};
  SpawnAllocationScope spawn_scope{spawn_stats};
  BenchmarkResult result = RunBenchmark(counted_resource, mode, frame_pool, iterations);
  result.frame_stats = frame_stats;
  result.spawn_stats = spawn_stats;
  return result;
}

BenchmarkResult RunOne(SpawnMode mode, bool frame_pool, std::uint64_t iterations) {
  MemoryResourceStats frame_stats;
  SpawnAllocationStats spawn_stats;
  if (frame_pool) {
    CoroFramePoolResource pool;
    return RunCountedBenchmark(pool, mode, true, iterations, frame_stats, spawn_stats);
  }

  std::pmr::memory_resource& heap = *std::pmr::new_delete_resource();
  return RunCountedBenchmark(heap, mode, false, iterations, frame_stats, spawn_stats);
}

const char* ModeName(SpawnMode mode) noexcept {
  if (mode == SpawnMode::kSpawnThenDetach) {
    return "Spawn(...).Detach()";
  }
  return "SpawnDetach(...)";
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

constexpr std::size_t kRounds = 5;

template <typename T>
T Median(std::array<T, kRounds> values) {
  std::sort(values.begin(), values.end());
  return values[kRounds / 2];
}

std::size_t ModeIndex(SpawnMode mode) noexcept {
  return mode == SpawnMode::kSpawnThenDetach ? 0 : 1;
}

BenchmarkResult MedianResult(const std::array<std::array<BenchmarkResult, 2>, kRounds>& results,
                             std::size_t mode_index) {
  BenchmarkResult median = results[0][mode_index];
  std::array<double, kRounds> elapsed_ms{};
  std::array<std::uint64_t, kRounds> frame_allocate_calls{};
  std::array<std::uint64_t, kRounds> frame_allocated_bytes{};
  std::array<std::uint64_t, kRounds> frame_deallocate_calls{};
  std::array<std::uint64_t, kRounds> frame_deallocated_bytes{};
  std::array<std::uint64_t, kRounds> spawn_allocate_calls{};
  std::array<std::uint64_t, kRounds> spawn_allocated_bytes{};
  std::array<std::uint64_t, kRounds> spawn_deallocate_calls{};
  std::array<std::uint64_t, kRounds> spawn_deallocated_bytes{};

  for (std::size_t round = 0; round < kRounds; ++round) {
    const auto& result = results[round][mode_index];
    elapsed_ms[round] = result.elapsed_ms;
    frame_allocate_calls[round] = result.frame_stats.allocate_calls;
    frame_allocated_bytes[round] = result.frame_stats.allocated_bytes;
    frame_deallocate_calls[round] = result.frame_stats.deallocate_calls;
    frame_deallocated_bytes[round] = result.frame_stats.deallocated_bytes;
    spawn_allocate_calls[round] = result.spawn_stats.allocate_calls;
    spawn_allocated_bytes[round] = result.spawn_stats.allocated_bytes;
    spawn_deallocate_calls[round] = result.spawn_stats.deallocate_calls;
    spawn_deallocated_bytes[round] = result.spawn_stats.deallocated_bytes;
  }

  median.elapsed_ms = Median(elapsed_ms);
  median.frame_stats.allocate_calls = Median(frame_allocate_calls);
  median.frame_stats.allocated_bytes = Median(frame_allocated_bytes);
  median.frame_stats.deallocate_calls = Median(frame_deallocate_calls);
  median.frame_stats.deallocated_bytes = Median(frame_deallocated_bytes);
  median.spawn_stats.allocate_calls = Median(spawn_allocate_calls);
  median.spawn_stats.allocated_bytes = Median(spawn_allocated_bytes);
  median.spawn_stats.deallocate_calls = Median(spawn_deallocate_calls);
  median.spawn_stats.deallocated_bytes = Median(spawn_deallocated_bytes);
  return median;
}

void PrintResult(const char* record, std::size_t round, const BenchmarkResult& result) {
  const auto& frame = result.frame_stats;
  const auto& spawn = result.spawn_stats;
  std::cout << record << ',' << (result.frame_pool ? 1 : 0) << ',' << result.iterations << ','
            << round << ',' << ModeName(result.mode) << ',' << result.elapsed_ms << ','
            << frame.allocate_calls << ',' << frame.allocated_bytes << ',' << frame.deallocate_calls
            << ',' << frame.deallocated_bytes << ',' << spawn.allocate_calls << ','
            << spawn.allocated_bytes << ',' << spawn.deallocate_calls << ','
            << spawn.deallocated_bytes << ',' << result.completed << '\n';
}

}  // namespace

int main() {
  constexpr std::array<std::uint64_t, 3> kIterationCounts{10'000, 100'000, 1'000'000};
  constexpr std::array<SpawnMode, 2> kModes{SpawnMode::kSpawnThenDetach, SpawnMode::kSpawnDetach};

  bool frame_pool_values[2]{};
  std::size_t frame_pool_count = 0;
  if (!ParseFramePoolSelection(frame_pool_values, &frame_pool_count)) {
    return 2;
  }

  std::cout << "record,frame_pool,iterations,round,mode,total_ms,frame_alloc_calls,"
               "frame_allocated_bytes,frame_dealloc_calls,frame_deallocated_bytes,"
               "spawn_state_alloc_calls,spawn_state_allocated_bytes,"
               "spawn_state_dealloc_calls,spawn_state_deallocated_bytes,completed\n";
  std::cout << std::fixed << std::setprecision(3);

  for (std::size_t pool_index = 0; pool_index < frame_pool_count; ++pool_index) {
    for (const std::uint64_t iterations : kIterationCounts) {
      std::array<std::array<BenchmarkResult, 2>, kRounds> results{};
      for (std::size_t round = 0; round < kRounds; ++round) {
        for (std::size_t position = 0; position < kModes.size(); ++position) {
          const std::size_t mode_index = round % 2 == 0 ? position : 1 - position;
          const SpawnMode mode = kModes[mode_index];
          results[round][mode_index] = RunOne(mode, frame_pool_values[pool_index], iterations);
          if (results[round][mode_index].completed != iterations) {
            std::cerr << "benchmark completed " << results[round][mode_index].completed << " of "
                      << iterations << " tasks\n";
            return 1;
          }
        }
        PrintResult("sample", round + 1, results[round][0]);
        PrintResult("sample", round + 1, results[round][1]);
      }

      for (std::size_t mode_index = 0; mode_index < kModes.size(); ++mode_index) {
        PrintResult("median", 0, MedianResult(results, mode_index));
      }
    }
  }
}
