// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include "coropact/cache/lru_cache.h"
#include "coropact/cache/sharded_lru_cache.h"

using coropact::cache::LRUCache;
using coropact::cache::ShardedLRUCache;

[[noreturn]] void Fail(const char* expression, int line) {
  std::fprintf(stderr, "FAIL line %d: %s\n", line, expression);
  std::abort();
}

#define CHECK(expression) ((expression) ? static_cast<void>(0) : Fail(#expression, __LINE__))

using Cache = LRUCache<int, std::string>;
using ShardedCache = ShardedLRUCache<int, std::string>;

static_assert(!std::is_copy_constructible_v<Cache>);
static_assert(!std::is_move_constructible_v<Cache>);
static_assert(!std::is_copy_constructible_v<ShardedCache>);
static_assert(!std::is_move_constructible_v<ShardedCache>);

void LruOrderTest() {
  Cache cache(2);
  CHECK(cache.Empty());
  CHECK(cache.Capacity() == 2);

  cache.Put(1, "one");
  cache.Put(2, "two");
  CHECK(cache.Size() == 2);
  CHECK(cache.Get(1) == "one");

  // Get(1) promoted key 1, so inserting key 3 evicts key 2.
  cache.Put(3, "three");
  CHECK(!cache.Contains(2));
  CHECK(cache.Contains(1));
  CHECK(cache.Contains(3));

  // Contains does not promote. Key 1 is still older than key 3.
  cache.Put(4, "four");
  CHECK(!cache.Contains(1));
  CHECK(cache.Get(3) == "three");
  CHECK(cache.Get(4) == "four");

  // Updating a key promotes it and replaces its value.
  cache.Put(3, "updated");
  CHECK(cache.Get(3) == "updated");
  CHECK(cache.Erase(3));
  CHECK(!cache.Erase(3));
  CHECK(cache.Size() == 1);

  cache.Clear();
  CHECK(cache.Empty());
  cache.Put(5, "reused");
  CHECK(cache.Get(5) == "reused");
}

void ExpirationTest() {
  Cache cache(4);
  cache.Put(1, "expired", coropact::time::Timestamp(1));
  cache.Put(2, "live");
  CHECK(!cache.Get(1).has_value());
  CHECK(cache.Size() == 1);
  CHECK(cache.Get(2) == "live");

  cache.Put(3, "expired", coropact::time::Timestamp(1));
  CHECK(cache.PruneExpired() == 1);
  CHECK(cache.Size() == 1);
}

void CapacityZeroTest() {
  Cache cache(0);
  cache.Put(1, "ignored");
  CHECK(cache.Empty());
  CHECK(!cache.Get(1).has_value());
}

void ShardedTest() {
  ShardedCache cache(5, 3);
  CHECK(cache.Capacity() == 5);
  CHECK(cache.ShardCount() == 3);

  for (int key = 0; key < 5; ++key) {
    cache.Put(key, std::to_string(key));
  }
  CHECK(cache.Size() == 5);
  for (int key = 0; key < 5; ++key) {
    CHECK(cache.Get(key) == std::to_string(key));
  }

  CHECK(cache.Erase(2));
  CHECK(!cache.Contains(2));
  CHECK(cache.Size() == 4);

  cache.Clear();
  CHECK(cache.Empty());

  bool threw = false;
  try {
    ShardedCache invalid(1, 0);
  } catch (const std::invalid_argument&) {
    threw = true;
  }
  CHECK(threw);
}

void ShardedConcurrentTest() {
  ShardedCache cache(256, 8);
  std::atomic<bool> ok{true};
  std::vector<std::thread> workers;

  for (int worker = 0; worker < 8; ++worker) {
    workers.emplace_back([&, worker] {
      for (int i = 0; i < 5000; ++i) {
        const int key = (worker * 17 + i) % 64;
        cache.Put(key, std::to_string(key));

        std::string value;
        if (!cache.Get(key, value) || value != std::to_string(key)) {
          ok.store(false, std::memory_order_relaxed);
        }
      }
    });
  }

  for (auto& worker : workers) worker.join();
  CHECK(ok.load(std::memory_order_relaxed));
  CHECK(cache.Size() == 64);
}

int main() {
  LruOrderTest();
  ExpirationTest();
  CapacityZeroTest();
  ShardedTest();
  ShardedConcurrentTest();
  std::printf("[PASS] lru_cache_test\n");
  return 0;
}
