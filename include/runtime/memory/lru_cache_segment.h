#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/time/timestamp.h"

#include <list>
#include <mutex>
#include <optional>
#include <unordered_map>

namespace runtime::memory {

/**
 * LRUCacheSegment stores one shard of an LRU cache.
 *
 * The hash table maps keys to list iterators, and the linked list keeps
 * entries ordered from least recently used at the front to most recently
 * used at the back. Each segment owns its own mutex to reduce contention.
 */
template <typename Key, typename Value>
class LRUCacheSegment : public runtime::base::NonCopyable {
public:
    explicit LRUCacheSegment(std::size_t capacity) : capacity_(capacity) {}

    bool get(const Key& key, Value& value) {
        std::lock_guard<std::mutex> lk{mutex_};

        auto it = mp_.find(key);
        if(it == mp_.end()) return false;

        if(isExpired(it->second)) {
            eraseInternal(it);
            return false;
        }
        touch(it->second);
        value = it->second->value;
        return true;
    }

    std::optional<Value> get(const Key &key) {
        std::lock_guard<std::mutex> lk{mutex_};

        auto it = mp_.find(key);
        if(it == mp_.end()) return std::nullopt;

        if(isExpired(it->second)) {
            eraseInternal(it);
            return std::nullopt;
        }
        touch(it->second);
        return it->second->value;
    }

    // put supports persistent values, absolute expiration, and TTL.
    void put(const Key &key, const Value &value) {
        std::lock_guard<std::mutex> lk{mutex_};
        putInternal(key, value , std::nullopt);
    }

    void put(const Key& key, const Value& value, runtime::time::Timestamp expire_at) {
        std::lock_guard<std::mutex> lk{mutex_};
        putInternal(key, value, expire_at);
    }

    void put(const Key &key, const Value &value, double ttl_seconds) {
        std::lock_guard<std::mutex> lk{mutex_};
        const auto expire_at 
            = runtime::time::AddTime(runtime::time::Timestamp::Now(), ttl_seconds);
        putInternal(key, value, expire_at);
    }

    bool erase(const Key& key) {
        std::lock_guard<std::mutex> lk{mutex_};

        auto it = mp_.find(key);
        if(it == mp_.end()) return false;
        
        eraseInternal(it);
        return true;
    }

    std::size_t size() const {
        std::lock_guard<std::mutex> lk{mutex_};
        return mp_.size();
    }
    
    bool contains(const Key &key) const {
        std::lock_guard<std::mutex> lk{mutex_};

        auto it = mp_.find(key);
        if(it == mp_.end()) {
            return false;
        }

        if(isExpired(it->second)) {
            return false;
        }
        return true;
    }

    bool empty() const {
        std::lock_guard<std::mutex> lk{mutex_};
        return mp_.empty();
    }
    std::size_t capacity() const {
        return capacity_;
    }

    void clear() {
        std::lock_guard<std::mutex> lk{mutex_};
        mp_.clear();
        lru_list_.clear();
    }

    std::size_t pruneExpired() {
        std::lock_guard<std::mutex> lk{mutex_};
        return pruneExpiredUnlocked();
    }

private:
    struct Entry {
        Key key;
        Value value;
        std::optional<runtime::time::Timestamp> expire_at;
    };

    using List = std::list<Entry>;
    using ListIt = typename List::iterator;
    using Map = std::unordered_map<Key, ListIt>;
    using MapIt = typename Map::iterator;

    bool isExpired(ListIt it) const {
        if(!it->expire_at.has_value()) {
            return false;
        }
        return runtime::time::Timestamp::Now().MicrosecondsSinceEpoch()
            >= it->expire_at->MicrosecondsSinceEpoch();
    }

    void touch(ListIt it) {
        lru_list_.splice(lru_list_.end(), lru_list_, it);
    }

    void eraseInternal(MapIt it) {
        lru_list_.erase(it->second);
        mp_.erase(it);
    }

    void evictOne() {
        const auto& oldest = lru_list_.front();
        mp_.erase(oldest.key);
        lru_list_.pop_front();
    }

    void putInternal(const Key& key,
                     const Value& value,
                     std::optional<runtime::time::Timestamp> expire_at) {
        if (capacity_ == 0) {
            return;
        }

        auto it = mp_.find(key);
        if (it != mp_.end()) {
            it->second->value = value;
            it->second->expire_at = expire_at;
            touch(it->second);
            return;
        }

        pruneExpiredUnlocked();

        if (mp_.size() >= capacity_) {
            evictOne();
        }

        lru_list_.push_back(Entry{key, value, expire_at});
        auto list_it = std::prev(lru_list_.end());
        mp_[key] = list_it;
    }

    std::size_t pruneExpiredUnlocked() {
        std::size_t removed = 0;
        for (auto list_it = lru_list_.begin(); list_it != lru_list_.end();) {
            if (isExpired(list_it)) {
                mp_.erase(list_it->key);
                list_it = lru_list_.erase(list_it);
                ++removed;
            } else {
                ++list_it;
            }
        }
        return removed;
    }

    mutable std::mutex mutex_;
    List lru_list_;
    Map mp_;
    std::size_t capacity_;
};

}  // namespace runtime::memory
