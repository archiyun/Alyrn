// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <cstdint>

namespace alyrn::coro::detail {

#if defined(ALYRN_ENABLE_SPAWN_STATS)

struct SpawnAllocationStats {
  std::uint64_t allocate_calls{0};
  std::uint64_t allocated_bytes{0};
  std::uint64_t deallocate_calls{0};
  std::uint64_t deallocated_bytes{0};
};

inline thread_local SpawnAllocationStats* current_spawn_stats = nullptr;

class SpawnAllocationScope {
public:
  explicit SpawnAllocationScope(SpawnAllocationStats& stats) noexcept
      : previous_(current_spawn_stats) {
    current_spawn_stats = &stats;
  }

  SpawnAllocationScope(const SpawnAllocationScope&) = delete;
  SpawnAllocationScope& operator=(const SpawnAllocationScope&) = delete;

  ~SpawnAllocationScope() { current_spawn_stats = previous_; }

private:
  SpawnAllocationStats* previous_;
};

inline void RecordSpawnStateAllocation(std::size_t bytes) noexcept {
  if (current_spawn_stats == nullptr) {
    return;
  }
  ++current_spawn_stats->allocate_calls;
  current_spawn_stats->allocated_bytes += bytes;
}

inline void RecordSpawnStateDeallocation(std::size_t bytes) noexcept {
  if (current_spawn_stats == nullptr) {
    return;
  }
  ++current_spawn_stats->deallocate_calls;
  current_spawn_stats->deallocated_bytes += bytes;
}

#endif

}  // namespace alyrn::coro::detail
