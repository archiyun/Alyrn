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

}  // namespace

int main() {
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

  // The public option accepts a real standard-library pool resource, not only
  // a tracking test resource.
  {
    std::pmr::unsynchronized_pool_resource pool;
    vexo::coro::FrameAllocatorScope frame_scope{pool};
    if (!Check(vexo::coro::SyncWait(Immediate()) == 42,
               "unsynchronized_pool_resource should back coroutine frames")) {
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
