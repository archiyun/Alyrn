// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <algorithm>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

#include "coropact/cache/lru_cache.h"
#include "coropact/utils/macros.h"

namespace coropact::cache {

// ShardedLRUCache owns one mutex and one LRUCache per shard. Operations on
// different keys usually lock different shards; Size, Clear and
// PruneExpired visit shards one at a time and are therefore not a global
// atomic snapshot under concurrent mutation.
template <typename Key, typename Value, typename Hash = std::hash<Key>,
          typename KeyEqual = std::equal_to<Key>>
class ShardedLRUCache {
public:
  COROPACT_DELETE_COPY_MOVE(ShardedLRUCache);

  ShardedLRUCache(std::size_t capacity, std::size_t shard_count, const Hash& hash = Hash{},
                  const KeyEqual& key_equal = KeyEqual{})
      : capacity_(capacity),
        shard_count_(NormalizeShardCount(capacity, shard_count)),
        hash_(hash),
        key_equal_(key_equal) {
    const std::size_t per_shard = capacity / shard_count_;
    const std::size_t remainder = capacity % shard_count_;
    shards_.reserve(shard_count_);
    for (std::size_t i = 0; i < shard_count_; ++i) {
      const std::size_t shard_capacity =
          per_shard + (i < remainder ? std::size_t{1} : std::size_t{0});
      shards_.push_back(std::make_unique<Shard>(shard_capacity, hash_, key_equal_));
    }
  }

  ~ShardedLRUCache() = default;

  std::optional<Value> Get(const Key& key) {
    Shard& shard = GetShard(key);
    std::lock_guard lock(shard.mutex);
    return shard.cache.Get(key);
  }

  bool Get(const Key& key, Value& value) {
    Shard& shard = GetShard(key);
    std::lock_guard lock(shard.mutex);
    return shard.cache.Get(key, value);
  }

  bool Contains(const Key& key) const {
    const Shard& shard = GetShard(key);
    std::lock_guard lock(shard.mutex);
    return shard.cache.Contains(key);
  }

  void Put(const Key& key, Value value) {
    Shard& shard = GetShard(key);
    std::lock_guard lock(shard.mutex);
    shard.cache.Put(key, std::move(value));
  }

  void Put(const Key& key, Value value, coropact::time::Timestamp expire_at) {
    Shard& shard = GetShard(key);
    std::lock_guard lock(shard.mutex);
    shard.cache.Put(key, std::move(value), expire_at);
  }

  void Put(const Key& key, Value value, double ttl_seconds) {
    Shard& shard = GetShard(key);
    std::lock_guard lock(shard.mutex);
    shard.cache.Put(key, std::move(value), ttl_seconds);
  }

  bool Erase(const Key& key) {
    Shard& shard = GetShard(key);
    std::lock_guard lock(shard.mutex);
    return shard.cache.Erase(key);
  }

  [[nodiscard]] std::size_t Size() const {
    std::size_t result = 0;
    for (const auto& shard_ptr : shards_) {
      const Shard& shard = *shard_ptr;
      std::lock_guard lock(shard.mutex);
      result += shard.cache.Size();
    }
    return result;
  }

  [[nodiscard]] bool Empty() const { return Size() == 0; }

  [[nodiscard]] std::size_t Capacity() const noexcept { return capacity_; }
  [[nodiscard]] std::size_t ShardCount() const noexcept { return shard_count_; }

  void Clear() noexcept {
    for (const auto& shard_ptr : shards_) {
      Shard& shard = *shard_ptr;
      std::lock_guard lock(shard.mutex);
      shard.cache.Clear();
    }
  }

  std::size_t PruneExpired() {
    std::size_t removed = 0;
    for (const auto& shard_ptr : shards_) {
      Shard& shard = *shard_ptr;
      std::lock_guard lock(shard.mutex);
      removed += shard.cache.PruneExpired();
    }
    return removed;
  }

private:
  using LocalCache = LRUCache<Key, Value, Hash, KeyEqual>;

  struct Shard {
    Shard(std::size_t capacity, const Hash& hash, const KeyEqual& key_equal)
        : cache(capacity, hash, key_equal) {}

    mutable std::mutex mutex;
    LocalCache cache;
  };

  static std::size_t NormalizeShardCount(std::size_t capacity, std::size_t shard_count) {
    if (shard_count == 0) {
      throw std::invalid_argument("shard_count must be greater than zero");
    }
    if (capacity == 0) return 1;
    return std::min(capacity, shard_count);
  }

  std::size_t ShardIndex(const Key& key) const {
    return static_cast<std::size_t>(hash_(key)) % shard_count_;
  }

  Shard& GetShard(const Key& key) { return *shards_[ShardIndex(key)]; }

  const Shard& GetShard(const Key& key) const { return *shards_[ShardIndex(key)]; }

  std::size_t capacity_{0};
  std::size_t shard_count_{0};
  Hash hash_{};
  KeyEqual key_equal_{};
  std::vector<std::unique_ptr<Shard>> shards_;
};

}  // namespace coropact::cache
