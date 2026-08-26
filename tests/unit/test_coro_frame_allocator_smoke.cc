// SPDX-License-Identifier: MIT

#include <sys/wait.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <memory_resource>
#include <set>
#include <thread>
#include <vector>

#include "coropact/coro/frame_allocator.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/spawn.h"
#include "coropact/coro/sync_wait.h"
#include "coropact/coro/task.h"
#include "coropact/coro/work.h"

namespace {

bool Check(bool condition, const char* message) {
  if (condition) return true;
  std::cerr << "FAIL: " << message << '\n';
  return false;
}

bool ExpectChildAbort(void (*entry)(), const char* message) {
  const pid_t child = ::fork();
  if (child < 0) {
    return Check(false, "fork failed for frame metadata invariant test");
  }
  if (child == 0) {
    (void)::freopen("/dev/null", "w", stderr);
    entry();
    ::_exit(0);
  }

  int status = 0;
  while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
  }
  return Check(WIFSIGNALED(status), message) &&
         Check(WTERMSIG(status) == SIGABRT, "frame metadata invariant must terminate with SIGABRT");
}

void PackOversizedFrameMetadata() {
  (void)coropact::coro::detail::PackFrameMetadata(
      static_cast<std::size_t>(coropact::coro::detail::kFrameMetadataBytesMask) + 1, 16);
}

void PackMisalignedFrameMetadata() { (void)coropact::coro::detail::PackFrameMetadata(128, 3); }

void PackUnencodableFrameAlignment() {
  (void)coropact::coro::detail::PackFrameMetadata(
      128, std::size_t{1} << (coropact::coro::detail::kFrameMetadataMaxAlignmentExponent + 1));
}

bool TestPackedMetadataRejectsInvalidValues() {
  return ExpectChildAbort(&PackOversizedFrameMetadata,
                          "oversized frame metadata must terminate in Release") &&
         ExpectChildAbort(&PackMisalignedFrameMetadata,
                          "non-power-of-two frame alignment must terminate in Release") &&
         ExpectChildAbort(&PackUnencodableFrameAlignment,
                          "unencodable frame alignment must terminate in Release");
}

void TestPackedFrameMetadata() {
  constexpr std::size_t kBytes = 123456;
  const auto metadata = coropact::coro::detail::PackFrameMetadata(kBytes, 64);
  Check(coropact::coro::detail::UnpackFrameBytes(metadata) == kBytes,
        "packed metadata should preserve frame bytes");
  Check(coropact::coro::detail::UnpackFrameAlignment(metadata) == 64,
        "packed metadata should preserve frame alignment");
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

bool TestNestedFrameAllocatorScopesRestoreSelection() {
  RecordingResource first;
  RecordingResource second;
  auto* const original = coropact::coro::FrameAllocatorScope::TryCurrent();

  {
    coropact::coro::FrameAllocatorScope first_scope{first};
    if (!Check(coropact::coro::FrameAllocatorScope::TryCurrent() == &first,
               "outer frame allocator scope should select its resource")) {
      return false;
    }

    {
      coropact::coro::FrameAllocatorScope same_scope{first};
      if (!Check(coropact::coro::FrameAllocatorScope::TryCurrent() == &first,
                 "same-resource frame allocator scope should preserve selection")) {
        return false;
      }
    }

    if (!Check(coropact::coro::FrameAllocatorScope::TryCurrent() == &first,
               "same-resource frame allocator scope should restore outer selection")) {
      return false;
    }

    {
      coropact::coro::FrameAllocatorScope second_scope{second};
      if (!Check(coropact::coro::FrameAllocatorScope::TryCurrent() == &second,
                 "nested frame allocator scope should select its resource")) {
        return false;
      }
    }

    if (!Check(coropact::coro::FrameAllocatorScope::TryCurrent() == &first,
               "nested frame allocator scope should restore outer resource")) {
      return false;
    }
  }

  return Check(coropact::coro::FrameAllocatorScope::TryCurrent() == original,
               "outer frame allocator scope should restore original resource");
}

struct alignas(64) OverAlignedBlock {
  std::byte data[64];
};

class DrainScheduler final : public coropact::coro::Scheduler {
public:
  explicit DrainScheduler(std::pmr::memory_resource* resource) noexcept : Scheduler(resource) {}

  void Schedule(coropact::coro::Work* work) noexcept override {
    const bool queued = queue_.PushBack(work);
    assert(queued);
    (void)queued;
  }

  void Drain() noexcept {
    while (coropact::coro::Work* work = queue_.PopFront()) {
      Run(work);
    }
  }

private:
  coropact::coro::WorkQueue queue_;
};

coropact::coro::Task<int> Immediate() { co_return 42; }

coropact::coro::Task<int> Nested() {
  const int value = co_await Immediate();
  co_return value + 1;
}

void TestSizeClassReuseAndFallback() {
  RecordingResource upstream;
  {
    coropact::coro::CoroFramePoolResource pool{upstream};
    void* first = pool.allocate(128, alignof(std::max_align_t));
    const std::size_t chunk_allocations = upstream.allocations();
    pool.deallocate(first, 128, alignof(std::max_align_t));

    void* second = pool.allocate(128, alignof(std::max_align_t));
    Check(upstream.allocations() == chunk_allocations,
          "same size class should reuse a returned slot");
    pool.deallocate(second, 128, alignof(std::max_align_t));

    void* large = pool.allocate(coropact::coro::CoroFramePoolResource::kMaxPooledBytes + 1,
                                alignof(std::max_align_t));
    Check(upstream.allocations() == chunk_allocations + 1,
          "large allocations should bypass size classes");
    pool.deallocate(large, coropact::coro::CoroFramePoolResource::kMaxPooledBytes + 1,
                    alignof(std::max_align_t));

    void* over_aligned = pool.allocate(sizeof(OverAlignedBlock), alignof(OverAlignedBlock));
    Check(upstream.allocations() == chunk_allocations + 2,
          "over-aligned allocations should bypass size classes");
    pool.deallocate(over_aligned, sizeof(OverAlignedBlock), alignof(OverAlignedBlock));
  }
  Check(upstream.allocations() == upstream.deallocations(),
        "size-class chunks and fallback allocations should be released");
}

bool TestRemoteFreeReusesAfterOwnerDrain() {
  RecordingResource upstream;
  coropact::coro::CoroFramePoolResource pool{upstream};
  void* first = pool.allocate(128, alignof(std::max_align_t));
  const std::size_t chunk_allocations = upstream.allocations();

  std::thread{[&] { pool.deallocate(first, 128, alignof(std::max_align_t)); }}.join();
  pool.DrainRemote();

  void* second = pool.allocate(128, alignof(std::max_align_t));
  if (!Check(second == first, "owner drain should reuse a remotely freed slot")) {
    return false;
  }
  if (!Check(upstream.allocations() == chunk_allocations, "remote free should not grow the pool")) {
    return false;
  }
  pool.deallocate(second, 128, alignof(std::max_align_t));
  return true;
}

bool TestDrainCurrentReusesRemote() {
  RecordingResource upstream;
  coropact::coro::CoroFramePoolResource pool{upstream};
  void* first = pool.allocate(128, alignof(std::max_align_t));
  const std::size_t chunk_allocations = upstream.allocations();

  std::thread{[&] { pool.deallocate(first, 128, alignof(std::max_align_t)); }}.join();
  coropact::coro::CoroFramePoolResource::DrainCurrent();

  void* second = pool.allocate(128, alignof(std::max_align_t));
  if (!Check(second == first, "DrainCurrent should reuse a remotely freed slot")) {
    return false;
  }
  if (!Check(upstream.allocations() == chunk_allocations,
             "DrainCurrent should not grow the pool")) {
    return false;
  }
  pool.deallocate(second, 128, alignof(std::max_align_t));
  return true;
}

bool TestRemoteFreeReusesWhenLocalEmpty() {
  RecordingResource upstream;
  coropact::coro::CoroFramePoolResource pool{upstream};
  const std::size_t slot_count = coropact::coro::CoroFramePoolResource::SlotsPerChunk(128);

  std::vector<void*> live(slot_count);
  for (void*& slot : live) {
    slot = pool.allocate(128, alignof(std::max_align_t));
  }
  const std::size_t chunk_allocations = upstream.allocations();

  std::thread{[&] { pool.deallocate(live[0], 128, alignof(std::max_align_t)); }}.join();

  void* reused = pool.allocate(128, alignof(std::max_align_t));
  if (!Check(reused == live[0], "allocate should drain remote when the local list is empty")) {
    return false;
  }
  if (!Check(upstream.allocations() == chunk_allocations,
             "drain-on-empty should not allocate another chunk")) {
    return false;
  }

  pool.deallocate(reused, 128, alignof(std::max_align_t));
  for (std::size_t i = 1; i < live.size(); ++i) {
    pool.deallocate(live[i], 128, alignof(std::max_align_t));
  }
  return true;
}

bool TestRemoteFreeFromManyThreadsReusesAfterDrain() {
  RecordingResource upstream;
  coropact::coro::CoroFramePoolResource pool{upstream};
  constexpr std::size_t kSlots = 32;
  std::vector<void*> slots(kSlots);
  for (void*& slot : slots) {
    slot = pool.allocate(128, alignof(std::max_align_t));
  }
  const std::size_t chunk_allocations = upstream.allocations();
  const std::set<void*> original{slots.begin(), slots.end()};

  std::vector<std::thread> threads;
  threads.reserve(kSlots);
  for (void* slot : slots) {
    threads.emplace_back([&pool, slot] { pool.deallocate(slot, 128, alignof(std::max_align_t)); });
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
  pool.DrainRemote();

  std::set<void*> reused;
  for (std::size_t i = 0; i < kSlots; ++i) {
    reused.insert(pool.allocate(128, alignof(std::max_align_t)));
  }
  if (!Check(reused == original, "draining remote should recover every concurrently freed slot")) {
    return false;
  }
  if (!Check(upstream.allocations() == chunk_allocations,
             "concurrent remote free should not grow the pool")) {
    return false;
  }
  for (void* slot : reused) {
    pool.deallocate(slot, 128, alignof(std::max_align_t));
  }
  return true;
}

bool TestDestructorDrainsRemote() {
  RecordingResource upstream;
  {
    coropact::coro::CoroFramePoolResource pool{upstream};
    void* first = pool.allocate(128, alignof(std::max_align_t));
    std::thread{[&] { pool.deallocate(first, 128, alignof(std::max_align_t)); }}.join();
  }
  return Check(upstream.allocations() == upstream.deallocations(),
               "pool destruction should drain remote frees before releasing chunks");
}

bool TestPooledSlotsShareAlignedSlab() {
  RecordingResource upstream;
  coropact::coro::CoroFramePoolResource pool{upstream};
  void* first = pool.allocate(128, alignof(std::max_align_t));
  void* second = pool.allocate(128, alignof(std::max_align_t));
  const auto chunk = coropact::coro::CoroFramePoolResource::kChunkBytes;
  const auto header = coropact::coro::CoroFramePoolResource::kSlabHeaderBytes;
  const auto a = reinterpret_cast<std::uintptr_t>(first);
  const auto b = reinterpret_cast<std::uintptr_t>(second);
  if (!Check((a & (chunk - 1)) >= header, "pooled slot should sit after the slab header")) {
    return false;
  }
  if (!Check((a & ~(chunk - 1)) == (b & ~(chunk - 1)),
             "same-class slots should share a chunk-aligned slab")) {
    return false;
  }
  if (!Check(((a & (chunk - 1)) - header) % 128 == 0,
             "a pooled slot should be a size-class stride from the slab header")) {
    return false;
  }
  pool.deallocate(first, 128, alignof(std::max_align_t));
  pool.deallocate(second, 128, alignof(std::max_align_t));
  return true;
}

bool TestPooledCoroutineFrameHasNoPrefix() {
  RecordingResource upstream;
  coropact::coro::CoroFramePoolResource pool{upstream};
  coropact::coro::FrameAllocatorScope scope{pool};
  void* first = coropact::coro::detail::AllocateFrame(64, alignof(std::max_align_t));
  const std::size_t chunks = upstream.allocations();
  if (!Check(chunks == 1, "a pooled coroutine frame should come from one slab")) {
    return false;
  }

  const auto chunk = coropact::coro::CoroFramePoolResource::kChunkBytes;
  const auto header = coropact::coro::CoroFramePoolResource::kSlabHeaderBytes;
  const auto offset = reinterpret_cast<std::uintptr_t>(first) % chunk;
  if (!Check(offset >= header && (offset - header) % 64 == 0,
             "a pooled coroutine frame should be returned at a slot start")) {
    return false;
  }

  coropact::coro::detail::DeallocateFrame(first);
  void* second = coropact::coro::detail::AllocateFrame(64, alignof(std::max_align_t));
  if (!Check(second == first, "slab lookup should reuse a pooled frame without a prefix header")) {
    return false;
  }
  if (!Check(upstream.allocations() == chunks, "slot reuse should not allocate another slab")) {
    return false;
  }
  coropact::coro::detail::DeallocateFrame(second);
  return true;
}

bool TestPooledCoroutineFrameRemoteFree() {
  RecordingResource upstream;
  coropact::coro::CoroFramePoolResource pool{upstream};
  coropact::coro::FrameAllocatorScope scope{pool};
  void* first = coropact::coro::detail::AllocateFrame(64, alignof(std::max_align_t));
  const std::size_t chunks = upstream.allocations();
  std::thread{[&] { coropact::coro::detail::DeallocateFrame(first); }}.join();
  pool.DrainRemote();
  void* second = coropact::coro::detail::AllocateFrame(64, alignof(std::max_align_t));
  if (!Check(second == first, "cross-thread destroy should return the slot to its slab owner")) {
    return false;
  }
  if (!Check(upstream.allocations() == chunks, "remote frame free should not grow the pool")) {
    return false;
  }
  coropact::coro::detail::DeallocateFrame(second);
  return true;
}

bool TestTightSizeClassesStayOnOneSlab() {
  RecordingResource upstream;
  coropact::coro::CoroFramePoolResource pool{upstream};
  const std::size_t overflow_128 = coropact::coro::CoroFramePoolResource::SlotsPerChunk(128) + 1;

  std::vector<void*> tiny(overflow_128);
  for (void*& slot : tiny) {
    slot = pool.allocate(64, alignof(std::max_align_t));
  }
  if (!Check(upstream.allocations() == 1, "64-byte frames should use a tighter class than 128")) {
    return false;
  }
  for (void* slot : tiny) {
    pool.deallocate(slot, 64, alignof(std::max_align_t));
  }

  std::vector<void*> medium(overflow_128);
  for (void*& slot : medium) {
    slot = pool.allocate(96, alignof(std::max_align_t));
  }
  if (!Check(upstream.allocations() == 2,
             "96-byte frames should not spill into the 128-byte class")) {
    return false;
  }
  for (void* slot : medium) {
    pool.deallocate(slot, 96, alignof(std::max_align_t));
  }

  const std::size_t overflow_256 = coropact::coro::CoroFramePoolResource::SlotsPerChunk(256) + 1;
  std::vector<void*> wide(overflow_256);
  for (void*& slot : wide) {
    slot = pool.allocate(129, alignof(std::max_align_t));
  }
  if (!Check(upstream.allocations() == 3,
             "129-byte frames should use 192-byte slots instead of 256")) {
    return false;
  }
  for (void* slot : wide) {
    pool.deallocate(slot, 129, alignof(std::max_align_t));
  }
  return true;
}

}  // namespace

int main() {
  if (!TestPackedMetadataRejectsInvalidValues()) return 1;
  TestPackedFrameMetadata();
  if (!TestNestedFrameAllocatorScopesRestoreSelection()) return 1;
  TestSizeClassReuseAndFallback();
  if (!TestRemoteFreeReusesAfterOwnerDrain()) return 1;
  if (!TestDrainCurrentReusesRemote()) return 1;
  if (!TestRemoteFreeReusesWhenLocalEmpty()) return 1;
  if (!TestRemoteFreeFromManyThreadsReusesAfterDrain()) return 1;
  if (!TestDestructorDrainsRemote()) return 1;
  if (!TestPooledSlotsShareAlignedSlab()) return 1;
  if (!TestPooledCoroutineFrameHasNoPrefix()) return 1;
  if (!TestPooledCoroutineFrameRemoteFree()) return 1;
  if (!TestTightSizeClassesStayOnOneSlab()) return 1;

  RecordingResource resource;

  // The default path remains independent of the recording resource.
  if (!Check(coropact::coro::SyncWait(Immediate()) == 42,
             "default frame allocation should still work")) {
    return 1;
  }
  if (!Check(resource.allocations() == 0, "default frames should not use custom resource")) {
    return 1;
  }

  // The scope covers argument evaluation, so both the leaf Task and the
  // eager SyncWait root are allocated from the selected resource.
  {
    coropact::coro::FrameAllocatorScope frame_scope{resource};
    if (!Check(coropact::coro::SyncWait(Immediate()) == 42,
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
    coropact::coro::CoroFramePoolResource pool;
    coropact::coro::FrameAllocatorScope frame_scope{pool};
    if (!Check(coropact::coro::SyncWait(Nested()) == 43,
               "size-class frame pool should preserve nested results")) {
      return 1;
    }
  }

  // Scheduler::Run re-enters the selected resource while a coroutine resumes,
  // so a child frame created inside Nested also uses the pool after the outer
  // creation scope has ended.
  {
    DrainScheduler scheduler{&resource};
    coropact::coro::JoinHandle<int> handle{nullptr};
    {
      coropact::coro::FrameAllocatorScope frame_scope{resource};
      handle = coropact::coro::Spawn(scheduler, Nested());
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
