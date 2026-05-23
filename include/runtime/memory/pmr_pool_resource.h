// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
#pragma once

#include "runtime/memory/pool.h"

#include <memory_resource>

namespace runtime::memory {

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

  bool do_is_equal(
    const std::pmr::memory_resource& other
  ) const noexcept override {
    auto* p = dynamic_cast<const PoolResource*>(&other);
    return p != nullptr && p->pool_ == pool_;
  }
  Pool* pool_;
};

}  // namespace runtime::memory