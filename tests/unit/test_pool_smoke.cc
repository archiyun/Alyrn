// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <new>
#include <utility>
#include <vector>

#include "coropact/memory/pool.h"

namespace {

int g_failures = 0;
int g_cleanup_calls = 0;

#define EXPECT(cond, msg)                                          \
  do {                                                             \
    if (!(cond)) {                                                 \
      std::cerr << "[FAIL] " << __func__ << ": " << (msg) << '\n'; \
      ++g_failures;                                                \
      return;                                                      \
    }                                                              \
  } while (0)

using coropact::memory::Pool;

bool IsAligned(const void* p, std::size_t align) {
  return (reinterpret_cast<std::uintptr_t>(p) % align) == 0;
}

bool ExpectChildAbort(void (*entry)(), const char* message) {
  const pid_t child = ::fork();
  if (child < 0) {
    std::cerr << "[FAIL] fork failed for " << message << '\n';
    return false;
  }
  if (child == 0) {
    (void)::freopen("/dev/null", "w", stderr);
    entry();
    ::_exit(0);
  }

  int status = 0;
  while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
  }
  return WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT;
}

void AllocateWithZeroAlignment() {
  auto pool = Pool::Create();
  (void)pool->AllocateAligned(8, 0);
}

void AllocateWithNonPowerOfTwoAlignment() {
  auto pool = Pool::Create();
  (void)pool->AllocateAligned(8, 3);
}

void TestSingleSmallAlloc() {
  auto pool = Pool::Create();
  void* a = pool->Allocate(64);
  EXPECT(a != nullptr,             "small alloc returns non-null");
  EXPECT(IsAligned(a, alignof(std::max_align_t)), "default-aligned");
  EXPECT(pool->ChunkCount() == 1,  "still 1 chunk");
  EXPECT(pool->ByteUsed() >= 64,   "ByteUsed >= request");
}

void TestManyAllocsCrossChunk() {
  auto pool = Pool::Create(/*chunk_size=*/256);

  std::vector<void*> ptrs;
  ptrs.reserve(64);
  for (int i = 0; i < 32; ++i) {
    void* p = pool->Allocate(64);
    EXPECT(p != nullptr, "many small allocs succeed");
    ptrs.push_back(p);
  }

  EXPECT(pool->ChunkCount() > 1, "must have spilled into new chunk(s)");

  for (std::size_t i = 0; i < ptrs.size(); ++i) {
    for (std::size_t j = i + 1; j < ptrs.size(); ++j) {
      auto a = reinterpret_cast<std::uintptr_t>(ptrs[i]);
      auto b = reinterpret_cast<std::uintptr_t>(ptrs[j]);
      EXPECT(a + 64 <= b || b + 64 <= a, "no overlap between allocs");
    }
  }
}

void TestAlignment() {
  auto pool = Pool::Create();
  for (std::size_t align : {std::size_t{8}, std::size_t{16},
                            std::size_t{32}, std::size_t{64}}) {
    void* p1 = pool->AllocateAligned(7, align);
    void* p2 = pool->AllocateAligned(13, align);
    EXPECT(IsAligned(p1, align), "AllocateAligned returns aligned p1");
    EXPECT(IsAligned(p2, align), "AllocateAligned returns aligned p2");
  }
}

void TestLargeOverAlignedAlloc() {
  auto pool = Pool::Create();
  void* allocation = pool->AllocateAligned(64, 64);
  EXPECT(IsAligned(allocation, 64), "over-aligned allocation must be correctly aligned");
  EXPECT(pool->LargeCount() == 1, "over-aligned allocation must bypass the arena");
  pool->Free(allocation);
  EXPECT(pool->LargeCount() == 0, "Free must release over-aligned large allocation");
}

void TestInvalidAlignmentTerminates() {
  EXPECT(ExpectChildAbort(&AllocateWithZeroAlignment,
                          "zero alignment must terminate in every build"),
         "zero alignment did not terminate");
  EXPECT(ExpectChildAbort(&AllocateWithNonPowerOfTwoAlignment,
                          "non-power-of-two alignment must terminate in every build"),
         "non-power-of-two alignment did not terminate");
}

void TestCleanupSizeOverflow() {
  auto pool = Pool::Create();
  bool threw = false;
  try {
    (void)pool->RegisterCleanup(nullptr, std::numeric_limits<std::size_t>::max());
  } catch (const std::bad_array_new_length&) {
    threw = true;
  }
  EXPECT(threw, "cleanup size overflow must throw before allocation");
}

void TestLargeAllocBypass() {
  auto pool = Pool::Create(/*chunk_size=*/256);
  void* big = pool->Allocate(8192);
  EXPECT(big != nullptr,            "large alloc returns non-null");
  EXPECT(pool->LargeCount() == 1,   "LargeCount incremented");
  std::memset(big, 0xAB, 8192);
}

void TestFreeAndReuseLargeSlot() {
  auto pool = Pool::Create(/*chunk_size=*/256);
  void* a = pool->Allocate(8192);
  void* b = pool->Allocate(8192);
  EXPECT(pool->LargeCount() == 2, "two large blocks");

  pool->Free(a);
  EXPECT(pool->LargeCount() == 1, "free dropped one large slot");

  void* c = pool->Allocate(8192);
  EXPECT(pool->LargeCount() == 2, "new large alloc reuses slot");
  (void)b; (void)c;
}

void TestCalloc() {
  auto pool = Pool::Create();
  auto* p = static_cast<unsigned char*>(pool->Callocate(128));
  EXPECT(p != nullptr, "callocate non-null");
  for (int i = 0; i < 128; ++i) {
    EXPECT(p[i] == 0, "callocate zero-fills");
  }
}

void CleanupHandler(void* /*data*/) { ++g_cleanup_calls; }

void TestCleanupLifoOnDestroy() {
  g_cleanup_calls = 0;
  {
    auto pool = Pool::Create();
    pool->RegisterCleanup(&CleanupHandler, 0);
    pool->RegisterCleanup(&CleanupHandler, 0);
    pool->RegisterCleanup(&CleanupHandler, 0);
  }
  EXPECT(g_cleanup_calls == 3, "all 3 cleanups ran on ~Pool");
}

void TestCleanupNotRunOnReset() {
  // nginx 语义: Reset 不跑 cleanup, 但必须清掉 cleanup 链头,
  // 否则 ~Pool 会在野指针上 double-run.
  g_cleanup_calls = 0;
  {
    auto pool = Pool::Create();
    pool->RegisterCleanup(&CleanupHandler, 0);
    pool->Reset();
    EXPECT(g_cleanup_calls == 0, "Reset does not run cleanup");
  }
  EXPECT(g_cleanup_calls == 0, "~Pool after Reset does not re-run stale cleanup");
}

void TestCleanupWithDataPayload() {
  struct Payload { int x; int y; };
  static int g_sum = 0;
  auto handler = +[](void* p) {
    auto* d = static_cast<Payload*>(p);
    g_sum = d->x + d->y;
  };

  g_sum = 0;
  {
    auto pool = Pool::Create();
    auto* p = static_cast<Payload*>(
        pool->RegisterCleanup(handler, sizeof(Payload)));
    EXPECT(p != nullptr, "cleanup payload non-null");
    EXPECT(IsAligned(p, alignof(Payload)), "payload aligned");
    p->x = 17;
    p->y = 25;
  }
  EXPECT(g_sum == 42, "cleanup handler received payload");
}

void TestResetReusesChunks() {
  auto pool = Pool::Create(/*chunk_size=*/256);
  for (int i = 0; i < 16; ++i) (void)pool->Allocate(64);
  std::size_t chunks_before = pool->ChunkCount();
  EXPECT(chunks_before > 1, "spilled into more than one chunk");

  pool->Reset();
  EXPECT(pool->ChunkCount() == chunks_before, "Reset keeps chunks");
  EXPECT(pool->ByteUsed() == 0,               "Reset zeros used bytes");
  EXPECT(pool->LargeCount() == 0,             "Reset drops large blocks");

  void* p = pool->Allocate(64);
  EXPECT(p != nullptr, "alloc after Reset works");
}

void TestSmallChunkWithLargeAlignment() {
  // 用 kMinChunkSize 起步: 触发后续 chunk 的紧凑布局, 暴露 aligned 越过
  // end 的边界. 同时让 AlignPtr 推进的距离接近 chunk 大小.
  auto pool = Pool::Create(/*chunk_size=*/Pool::kMinChunkSize);

  std::vector<void*> ptrs;
  for (int i = 0; i < 20; ++i) {
    void* p = pool->AllocateAligned(64, 64);
    EXPECT(p != nullptr, "tight chunk + 64B align still serves");
    EXPECT(IsAligned(p, 64), "returned pointer respects 64B align");
    ptrs.push_back(p);
  }

  for (std::size_t i = 0; i < ptrs.size(); ++i) {
    for (std::size_t j = i + 1; j < ptrs.size(); ++j) {
      EXPECT(ptrs[i] != ptrs[j], "tight chunk path returns distinct ptrs");
    }
  }
}

void TestMoveOwnership() {
  // Pool::Ptr 是 unique_ptr, 应能移动而不复制; 移动后析构只跑一次.
  g_cleanup_calls = 0;
  {
    auto a = Pool::Create();
    a->RegisterCleanup(&CleanupHandler, 0);
    auto b = std::move(a);
    EXPECT(a.get() == nullptr, "moved-from Ptr is empty");
    EXPECT(b.get() != nullptr, "moved-to Ptr owns the pool");
  }
  EXPECT(g_cleanup_calls == 1, "cleanup ran exactly once after move");
}

void TestStressNoCrash() {
  auto pool = Pool::Create(/*chunk_size=*/512);
  for (int i = 0; i < 200; ++i) {
    if (i % 13 == 0)      (void)pool->Allocate(4096);
    else if (i % 7 == 0)  pool->RegisterCleanup(&CleanupHandler, 32);
    else                  (void)pool->Allocate(64);
  }
  EXPECT(pool->ChunkCount() >= 2,  "stress test grew chunks");
  EXPECT(pool->LargeCount() >= 1,  "stress test grew large list");

  pool->Reset();
  EXPECT(pool->ByteUsed() == 0,    "stress Reset clean");
  EXPECT(pool->LargeCount() == 0,  "stress Reset large gone");
}

}  // namespace

int main() {
  TestSingleSmallAlloc();
  TestManyAllocsCrossChunk();
  TestAlignment();
  TestLargeOverAlignedAlloc();
  TestInvalidAlignmentTerminates();
  TestCleanupSizeOverflow();
  TestLargeAllocBypass();
  TestFreeAndReuseLargeSlot();
  TestCalloc();
  TestCleanupLifoOnDestroy();
  TestCleanupNotRunOnReset();
  TestCleanupWithDataPayload();
  TestResetReusesChunks();
  TestSmallChunkWithLargeAlignment();
  TestMoveOwnership();
  TestStressNoCrash();

  if (g_failures == 0) {
    std::cout << "[PASS] pool_smoke_test (all checks ok)\n";
    return 0;
  }
  std::cerr << "[FAIL] pool_smoke_test (" << g_failures << " failures)\n";
  return 1;
}
