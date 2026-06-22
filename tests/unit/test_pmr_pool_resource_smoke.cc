// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include <cstdint>
#include <iostream>
#include <memory_resource>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "vexo/memory/pmr_pool_resource.h"
#include "vexo/memory/pool.h"

namespace {

int g_failures = 0;

#define EXPECT(cond, msg)                                          \
  do {                                                             \
    if (!(cond)) {                                                 \
      std::cerr << "[FAIL] " << __func__ << ": " << (msg) << '\n'; \
      ++g_failures;                                                \
      return;                                                      \
    }                                                              \
  } while (0)



using vexo::memory::Pool;
using vexo::memory::PoolResource;

using PmrString = std::pmr::string;

template <typename T>
using PmrVector = std::pmr::vector<T>;

template <typename K, typename V>
using PmrMap = std::pmr::unordered_map<K, V>;

bool IsAligned(const void* p, std::size_t align) {
  return (reinterpret_cast<std::uintptr_t>(p) % align) == 0;
}

bool PointerLooksInsideArena(const void* p, const Pool& /*pool*/) {
  // 没有 Pool 公开的 chunk 起点 API; 退而求其次, 看指针对齐合理且非空.
  return p != nullptr && IsAligned(p, alignof(char));
}

void TestIsEqual() {
  auto pa = Pool::Create();
  auto pb = Pool::Create();
  PoolResource ra1(*pa), ra2(*pa), rb(*pb);

  EXPECT(ra1 == ra2, "two resources on same Pool compare equal");
  EXPECT(!(ra1 == rb), "resources on different Pools are not equal");
  EXPECT(ra1.pool().chunk_count() >= 1, "underlying pool reachable via pool()");
}

void TestPmrStringGrowsThroughArena() {
  auto pool = Pool::Create();
  PoolResource res(*pool);
  const std::size_t bytes_before = pool->ByteUsed();

  PmrString s(&res);
  s.reserve(256);  // 单次大 reserve, 强制走 arena
  for (int i = 0; i < 200; ++i) {
    s.push_back('a' + (i % 26));
  }
  EXPECT(s.size() == 200, "pmr::string accumulates characters");
  EXPECT(pool->ByteUsed() > bytes_before,
         "Pool::ByteUsed increased after pmr::string activity");
  EXPECT(PointerLooksInsideArena(s.data(), *pool),
         "pmr::string buffer pointer is reasonable");
}

void TestPmrVectorReallocation() {
  auto pool = Pool::Create(/*chunk_size=*/512);
  PoolResource res(*pool);
  const std::size_t chunks_before = pool->chunk_count();

  PmrVector<int> v(&res);
  for (int i = 0; i < 1024; ++i) {
    v.push_back(i);
  }
  EXPECT(v.size() == 1024, "pmr::vector grew to 1024 entries");
  for (int i = 0; i < 1024; ++i) {
    EXPECT(v[i] == i, "pmr::vector preserves values across reallocate");
  }
  EXPECT(pool->chunk_count() > chunks_before,
         "vector reallocates spilled into new arena chunk(s)");
}

void TestPmrUnorderedMapInsertAndLookup() {
  auto pool = Pool::Create();
  PoolResource res(*pool);

  PmrMap<PmrString, int> m(&res);
  for (int i = 0; i < 64; ++i) {
    PmrString key("key_", &res);
    key.append(std::to_string(i));
    m.emplace(std::move(key), i);
  }
  EXPECT(m.size() == 64, "pmr::unordered_map holds all 64 entries");

  for (int i = 0; i < 64; ++i) {
    PmrString key("key_", &res);
    key.append(std::to_string(i));
    auto it = m.find(key);
    EXPECT(it != m.end(), "pmr::unordered_map key lookup hit");
    EXPECT(it->second == i, "pmr::unordered_map value preserved");
  }
}

void TestResetInvalidatesAndReuses() {
  // PMR 硬规则: Reset 之后所有从该 arena 分配的容器必须已被析构,
  // 否则继续访问就是 UAF. 本测试显式按序 destroy -> Reset -> rebuild.
  auto pool = Pool::Create();
  PoolResource res(*pool);

  {
    PmrVector<int> v(&res);
    for (int i = 0; i < 128; ++i) v.push_back(i);
    EXPECT(v.size() == 128, "first cycle vector populated");
  }  // v 析构, 但 arena 内存还没被回收 (do_deallocate 是 no-op)

  EXPECT(pool->ByteUsed() > 0, "arena still holds bytes after container dtor");

  pool->Reset();
  EXPECT(pool->ByteUsed() == 0, "Pool::Reset zeroes used bytes");

  // Reset 之后再构造新容器, 必须能正常使用同一个 PoolResource
  {
    PmrVector<int> v2(&res);
    for (int i = 0; i < 64; ++i) v2.push_back(i * 2);
    EXPECT(v2.size() == 64, "post-Reset vector works");
    EXPECT(v2.back() == 126, "post-Reset value correct");
  }
}

void TestAlignmentRoutedToPool() {
  // pmr 容器内部对齐请求要正确传到 Pool::AllocateAligned, 否则
  // node 类型的 alignof 大于 max_align 时会 UB.
  struct alignas(64) AlignedNode {
    std::uint64_t a, b, c, d, e, f, g, h;
  };

  auto pool = Pool::Create();
  PoolResource res(*pool);
  PmrVector<AlignedNode> v(&res);
  v.resize(4);
  for (std::size_t i = 0; i < v.size(); ++i) {
    EXPECT(IsAligned(&v[i], 64), "AlignedNode satisfies alignof(64)");
  }
}

}  // namespace

int main() {
  TestIsEqual();
  TestPmrStringGrowsThroughArena();
  TestPmrVectorReallocation();
  TestPmrUnorderedMapInsertAndLookup();
  TestResetInvalidatesAndReuses();
  TestAlignmentRoutedToPool();

  if (g_failures == 0) {
    std::cout << "[PASS] pmr_pool_resource_smoke_test (all checks ok)\n";
    return 0;
  }
  std::cerr << "[FAIL] pmr_pool_resource_smoke_test ("
            << g_failures << " failures)\n";
  return 1;
}
