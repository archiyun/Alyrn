
#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/memory/lru_cache_segment.h"

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <vector>

namespace runtime::memory {

template <typename Key, typename Value>
class SegmentLRUCache : public runtime::base::NonCopyable {
public:
    explicit SegmentLRUCache(std::size_t capacity, std::size_t segment_count)
        : segment_count_(normalizeSegmentCount(capacity, segment_count)) {
        const std::size_t per_count = capacity / segment_count_;
        const std::size_t remainder = capacity % segment_count_;

        for (std::size_t i = 0; i < segment_count_; ++i) {
            const std::size_t segment_capacity =
                per_count + (i < remainder ? 1U : 0U);
            segments_.emplace_back(
                std::make_unique<LRUCacheSegment<Key, Value>>(segment_capacity));
        }
    }

    bool get(const Key &key, Value &value) {
        return get_segment(key).get(key, value);
    } 

    std::optional<Value> get(const Key &key) {
        return get_segment(key).get(key);
    }

    void put(const Key &key, const Value &value) {
        get_segment(key).put(key, value);
    }

    void put(const Key& key,
             const Value& value,
             runtime::time::Timestamp expire_at) {
        get_segment(key).put(key, value, expire_at);
    }

    void put(const Key& key, const Value& value, double ttl_seconds) {
        get_segment(key).put(key, value, ttl_seconds);
    }

    bool erase(const Key &key) {
        return get_segment(key).erase(key);
    }

    bool contains(const Key &key) const {
        return get_segment(key).contains(key);
    }

    std::size_t size() const {
        std::size_t total_size = 0L;
        for(const auto &seg : segments_) {
            total_size += seg->size();
        }
        return total_size;
    }

    bool empty() const {
        return size() == 0;
    }

    std::size_t segmentCount() const {
        return segment_count_;
    }

    void clear() {
        for(auto &seg : segments_) {
            seg->clear();
        }
    }

    std::size_t pruneExpired() {
        std::size_t total_removed = 0;
        for (auto& seg : segments_) {
            total_removed += seg->pruneExpired();
        }
        return total_removed;
    }

private:
    LRUCacheSegment<Key, Value> &get_segment(const Key &key) {
        std::size_t idx = std::hash<Key>{}(key) % segment_count_;
        return *segments_[idx];
    }

    const LRUCacheSegment<Key, Value> &get_segment(const Key &key) const {
        std::size_t idx = std::hash<Key>{}(key) % segment_count_;
        return *segments_[idx];
    }

    static std::size_t normalizeSegmentCount(std::size_t capacity,
                                             std::size_t segment_count) {
        if (segment_count == 0) {
            throw std::invalid_argument("segment_count must be greater than 0");
        }

        if (capacity == 0) {
            return 1;
        }

        return segment_count > capacity ? capacity : segment_count;
    }

private:
    std::vector<std::unique_ptr<LRUCacheSegment<Key, Value>>> segments_;
    std::size_t segment_count_;
};

}  // namespace runtime::memory
