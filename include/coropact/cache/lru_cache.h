// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cassert>
#include <cstddef>
#include <functional>
#include <iterator>
#include <list>
#include <optional>
#include <unordered_map>
#include <utility>

#include "coropact/time/timestamp.h"
#include "coropact/utils/macros.h"

namespace coropact::cache {

// LRUCache is a single-owner LRU cache. It deliberately contains no mutex and
// is not safe for concurrent access. ShardedLRUCache provides a separately
// locked LRUCache per shard for concurrent callers.
//
// The list is ordered from least recently used at the front to most recently
// used at the back. A successful Get promotes the entry to the back. Contains
// does not promote an entry. An expired entry is reported as absent and is
// physically removed by Get or PruneExpired.
template <typename Key, typename Value, typename Hash = std::hash<Key>,
          typename KeyEqual = std::equal_to<Key>>
class LRUCache {
public:
  COROPACT_DELETE_COPY_MOVE(LRUCache);

  explicit LRUCache(std::size_t capacity, const Hash& hash = Hash{},
                    const KeyEqual& key_equal = KeyEqual{})
      : capacity_(capacity), index_(0, hash, key_equal) {}

  ~LRUCache() { Clear(); }

  [[nodiscard]] bool Empty() const noexcept { return index_.empty(); }
  [[nodiscard]] std::size_t Size() const noexcept { return index_.size(); }
  [[nodiscard]] std::size_t Capacity() const noexcept { return capacity_; }

  // Returns a copy and promotes a live entry to the MRU position.
  std::optional<Value> Get(const Key& key) {
    auto it = index_.find(key);
    if (it == index_.end()) return std::nullopt;

    ListIt entry = it->second;
    if (IsExpired(entry)) {
      EraseInternal(it);
      return std::nullopt;
    }

    Touch(entry);
    return entry->value;
  }

  // Output-parameter form for callers that want to avoid an optional object.
  bool Get(const Key& key, Value& value) {
    auto it = index_.find(key);
    if (it == index_.end()) return false;

    ListIt entry = it->second;
    if (IsExpired(entry)) {
      EraseInternal(it);
      return false;
    }

    Touch(entry);
    value = entry->value;
    return true;
  }

  // Contains does not update LRU order or mutate the cache.
  bool Contains(const Key& key) const {
    auto it = index_.find(key);
    if (it == index_.end()) return false;
    return !IsExpired(it->second);
  }

  // A zero-capacity cache accepts no entries.
  void Put(const Key& key, Value value) { PutInternal(key, std::move(value), std::nullopt); }

  void Put(const Key& key, Value value, coropact::time::Timestamp expire_at) {
    PutInternal(key, std::move(value), expire_at);
  }

  void Put(const Key& key, Value value, double ttl_seconds) {
    PutInternal(key, std::move(value),
                coropact::time::AddTime(coropact::time::Timestamp::Now(), ttl_seconds));
  }

  bool Erase(const Key& key) {
    auto it = index_.find(key);
    if (it == index_.end()) return false;
    EraseInternal(it);
    return true;
  }

  // Removes all entries and leaves the cache reusable.
  void Clear() noexcept {
    index_.clear();
    entries_.clear();
  }

  // Expiration is lazy. This method eagerly scans and removes expired entries.
  std::size_t PruneExpired() {
    std::size_t removed = 0;
    for (auto it = entries_.begin(); it != entries_.end();) {
      if (!IsExpired(it)) {
        ++it;
        continue;
      }

      auto next = std::next(it);
      auto map_it = index_.find(it->key);
      if (map_it != index_.end()) index_.erase(map_it);
      entries_.erase(it);
      it = next;
      ++removed;
    }
    return removed;
  }

private:
  struct Entry {
    Key key;
    Value value;
    std::optional<coropact::time::Timestamp> expire_at;
  };

  using List = std::list<Entry>;
  using ListIt = typename List::iterator;
  using Map = std::unordered_map<Key, ListIt, Hash, KeyEqual>;
  using MapIt = typename Map::iterator;

  [[nodiscard]] bool IsExpired(ListIt entry) const {
    return entry->expire_at.has_value() && coropact::time::Timestamp::Now() >= *entry->expire_at;
  }

  void Touch(ListIt entry) { entries_.splice(entries_.end(), entries_, entry); }

  void EraseInternal(MapIt it) {
    entries_.erase(it->second);
    index_.erase(it);
  }

  void EvictOne() {
    auto oldest = entries_.begin();
    auto map_it = index_.find(oldest->key);
    if (map_it != index_.end()) index_.erase(map_it);
    entries_.erase(oldest);
  }

  void PutInternal(const Key& key, Value value, std::optional<coropact::time::Timestamp> expire_at) {
    if (capacity_ == 0) return;

    auto existing = index_.find(key);
    if (existing != index_.end()) {
      existing->second->value = std::move(value);
      existing->second->expire_at = expire_at;
      Touch(existing->second);
      return;
    }

    PruneExpired();
    if (index_.size() >= capacity_) EvictOne();

    entries_.push_back(Entry{key, std::move(value), expire_at});
    auto entry = std::prev(entries_.end());
    try {
      const bool inserted = index_.emplace(entry->key, entry).second;
      assert(inserted);
      if (!inserted) entries_.pop_back();
    } catch (...) {
      entries_.pop_back();
      throw;
    }
  }

  std::size_t capacity_{0};
  List entries_;
  Map index_;
};

}  // namespace coropact::cache
