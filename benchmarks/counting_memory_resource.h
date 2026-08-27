// SPDX-License-Identifier: MIT
#pragma once

#include <algorithm>
#include <cstdint>
#include <memory_resource>

namespace {

struct MemoryResourceStats {
  std::uint64_t allocate_calls{0};
  std::uint64_t deallocate_calls{0};
  std::uint64_t allocated_bytes{0};
  std::uint64_t deallocated_bytes{0};
  std::uint64_t outstanding_allocations{0};
  std::uint64_t outstanding_bytes{0};
  std::uint64_t peak_outstanding_allocations{0};
  std::uint64_t peak_outstanding_bytes{0};
};

class CountingMemoryResource final : public std::pmr::memory_resource {
public:
  CountingMemoryResource(std::pmr::memory_resource& upstream, MemoryResourceStats& stats) noexcept
      : upstream_(&upstream), stats_(&stats) {}

  CountingMemoryResource(const CountingMemoryResource&) = delete;
  CountingMemoryResource& operator=(const CountingMemoryResource&) = delete;
  CountingMemoryResource(CountingMemoryResource&&) = delete;
  CountingMemoryResource& operator=(CountingMemoryResource&&) = delete;

private:
  void* do_allocate(std::size_t bytes, std::size_t alignment) override {
    void* result = upstream_->allocate(bytes, alignment);
    ++stats_->allocate_calls;
    stats_->allocated_bytes += bytes;
    ++stats_->outstanding_allocations;
    stats_->outstanding_bytes += bytes;
    stats_->peak_outstanding_allocations =
        std::max(stats_->peak_outstanding_allocations, stats_->outstanding_allocations);
    stats_->peak_outstanding_bytes =
        std::max(stats_->peak_outstanding_bytes, stats_->outstanding_bytes);
    return result;
  }

  void do_deallocate(void* pointer, std::size_t bytes, std::size_t alignment) noexcept override {
    upstream_->deallocate(pointer, bytes, alignment);
    ++stats_->deallocate_calls;
    stats_->deallocated_bytes += bytes;
    if (stats_->outstanding_allocations > 0) {
      --stats_->outstanding_allocations;
    }
    if (stats_->outstanding_bytes >= bytes) {
      stats_->outstanding_bytes -= bytes;
    } else {
      stats_->outstanding_bytes = 0;
    }
  }

  bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override {
    return this == &other;
  }

  std::pmr::memory_resource* upstream_;
  MemoryResourceStats* stats_;
};

}  // namespace
