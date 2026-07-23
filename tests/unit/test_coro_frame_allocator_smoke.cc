// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <cassert>
#include <cstddef>
#include <iostream>
#include <memory_resource>

#include "vexo/coro/frame_allocator.h"
#include "vexo/coro/scheduler.h"
#include "vexo/coro/spawn.h"
#include "vexo/coro/sync_wait.h"
#include "vexo/coro/task.h"
#include "vexo/coro/work.h"

namespace {

bool Check(bool condition, const char* message) {
  if (condition) return true;
  std::cerr << "FAIL: " << message << '\n';
  return false;
}

class RecordingResource final : public std::pmr::memory_resource {
public:
  std::size_t allocations() const noexcept { return allocations_; }
  std::size_t deallocations() const noexcept { return deallocations_; }

private:
  void* do_allocate(std::size_t bytes, std::size_t alignment) override {
    ++allocations_;
    return std::pmr::new_delete_resource()->allocate(bytes, alignment);
  }

  void do_deallocate(void* ptr, std::size_t bytes, std::size_t alignment) override {
    ++deallocations_;
    std::pmr::new_delete_resource()->deallocate(ptr, bytes, alignment);
  }

  bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override {
    return this == &other;
  }

  std::size_t allocations_{0};
  std::size_t deallocations_{0};
};

struct alignas(64) OverAlignedBlock {
  std::byte data[64];
};

class DrainScheduler final : public vexo::coro::Scheduler {
public:
  explicit DrainScheduler(std::pmr::memory_resource* resource) noexcept : Scheduler(resource) {}

  void Schedule(vexo::coro::Work* work) noexcept override {
    const bool queued = queue_.PushBack(work);
    assert(queued);
    (void)queued;
  }

  void Drain() noexcept {
    while (vexo::coro::Work* work = queue_.PopFront()) {
      Run(work);
    }
  }

private:
  vexo::coro::WorkQueue queue_;
};

vexo::coro::Task<int> Immediate() { co_return 42; }

vexo::coro::Task<int> Nested() {
  const int value = co_await Immediate();
  co_return value + 1;
}

void TestSizeClassReuseAndFallback() {
  RecordingResource upstream;
  {
    vexo::coro::CoroFramePoolResource pool{upstream};
    void* first = pool.allocate(128, alignof(std::max_align_t));
    const std::size_t chunk_allocations = upstream.allocations();
    pool.deallocate(first, 128, alignof(std::max_align_t));

    void* second = pool.allocate(128, alignof(std::max_align_t));
    Check(upstream.allocations() == chunk_allocations,
          "same size class should reuse a returned slot");
    pool.deallocate(second, 128, alignof(std::max_align_t));

    void* large = pool.allocate(vexo::coro::CoroFramePoolResource::kMaxPooledBytes + 1,
                                alignof(std::max_align_t));
    Check(upstream.allocations() == chunk_allocations + 1,
          "large allocations should bypass size classes");
    pool.deallocate(large, vexo::coro::CoroFramePoolResource::kMaxPooledBytes + 1,
                    alignof(std::max_align_t));

    void* over_aligned = pool.allocate(sizeof(OverAlignedBlock), alignof(OverAlignedBlock));
    Check(upstream.allocations() == chunk_allocations + 2,
          "over-aligned allocations should bypass size classes");
    pool.deallocate(over_aligned, sizeof(OverAlignedBlock), alignof(OverAlignedBlock));
  }
  Check(upstream.allocations() == upstream.deallocations(),
        "size-class chunks and fallback allocations should be released");
}

}  // namespace

int main() {
  TestSizeClassReuseAndFallback();

  RecordingResource resource;

  // The default path remains independent of the recording resource.
  if (!Check(vexo::coro::SyncWait(Immediate()) == 42,
             "default frame allocation should still work")) {
    return 1;
  }
  if (!Check(resource.allocations() == 0, "default frames should not use custom resource")) {
    return 1;
  }

  // The scope covers argument evaluation, so both the leaf Task and the
  // eager SyncWait root are allocated from the selected resource.
  {
    vexo::coro::FrameAllocatorScope frame_scope{resource};
    if (!Check(vexo::coro::SyncWait(Immediate()) == 42,
               "SyncWait should run with a custom frame resource")) {
      return 1;
    }
  }
  if (!Check(resource.allocations() >= 2, "custom resource should see coroutine frames")) {
    return 1;
  }
  if (!Check(resource.allocations() == resource.deallocations(),
             "completed SyncWait frames should be returned")) {
    return 1;
  }

  // The dedicated worker-local size-class resource must support coroutine
  // frames of different sizes and return every frame before destruction.
  {
    vexo::coro::CoroFramePoolResource pool;
    vexo::coro::FrameAllocatorScope frame_scope{pool};
    if (!Check(vexo::coro::SyncWait(Nested()) == 43,
               "size-class frame pool should preserve nested results")) {
      return 1;
    }
  }

  // Scheduler::Run re-enters the selected resource while a coroutine resumes,
  // so a child frame created inside Nested also uses the pool after the outer
  // creation scope has ended.
  {
    DrainScheduler scheduler{&resource};
    vexo::coro::JoinHandle<int> handle{nullptr};
    {
      vexo::coro::FrameAllocatorScope frame_scope{resource};
      handle = vexo::coro::Spawn(scheduler, Nested());
    }

    scheduler.Drain();
    if (!Check(handle.Wait() == 43, "pooled Spawn should preserve the result")) return 1;
  }

  if (!Check(resource.allocations() >= 5,
             "Task, SpawnRoot, SyncWaitRoot, and nested frames should be pooled")) {
    return 1;
  }
  if (!Check(resource.allocations() == resource.deallocations(),
             "all pooled coroutine frames should be returned")) {
    return 1;
  }

  std::cout << "coro frame allocator smoke: PASS\n";
  return 0;
}
