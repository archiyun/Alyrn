// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <algorithm>
#include <cstdint>
#include <memory_resource>

#include "vexo/memory/pool.h"

namespace vexo::memory {

class PoolResource final : public std::pmr::memory_resource {
public:
  explicit PoolResource(Pool& pool) noexcept : pool_(&pool) {}

  Pool& pool() noexcept { return *pool_; }

private:
  void* do_allocate(std::size_t bytes, std::size_t align) override {
    return pool_->AllocateAligned(bytes, align);
  }

  // Intentionally empty: nginx-style arena cannot release a single
  // allocation; storage is reclaimed in bulk via Pool::Reset / ~Pool.
  void do_deallocate(void*, std::size_t, std::size_t) override {}

  bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override {
    auto* p = dynamic_cast<const PoolResource*>(&other);
    return p != nullptr && p->pool_ == pool_;
  }
  Pool* pool_;
};

// Low-overhead counters for a memory_resource. The resource and its counters
// are intended to share one owner thread unless the wrapped resource itself is
// synchronized. It is useful for distinguishing allocation requests made by a
// caller from chunks obtained by a pooling resource from its upstream resource.
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

  VEXO_DELETE_COPY_MOVE(CountingMemoryResource);

  [[nodiscard]] const MemoryResourceStats& stats() const noexcept { return *stats_; }

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

}  // namespace vexo::memory
