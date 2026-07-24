// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <list>
#include <memory>
#include <optional>
#include <utility>

#include "coropact/cache/intrusive_lru.h"
#include "coropact/ds/intrusive_list.h"
#include "coropact/gateway/upstream_peer.h"
#include "coropact/io/async_stream.h"
#include "coropact/utils/macros.h"

namespace coropact::gateway {

inline constexpr std::size_t kPoolLatencyHistogramBucketCount = 64;

struct PoolConfig {
  int max_idle_per_peer{8};
  // Optional Worker-local shared idle budget. When non-zero, it caps all peer
  // queues together. Set max_idle_per_peer to zero to disable the per-peer
  // cap while retaining only this shared budget.
  std::size_t max_idle_total{0};
  double keepalive_timeout_sec{60.0};
  bool collect_stats{false};
};

struct UpstreamStreamPoolStats {
  std::uint64_t acquire_count{0};
  std::uint64_t acquire_hit_count{0};
  std::uint64_t acquire_miss_count{0};
  std::uint64_t release_count{0};
  std::uint64_t release_drop_count{0};
  std::uint64_t connect_attempt_count{0};
  std::uint64_t connect_success_count{0};
  std::uint64_t connect_failure_count{0};
  std::uint64_t connect_time_ns_sum{0};
  std::uint64_t connect_time_ns_max{0};
  std::array<std::uint64_t, kPoolLatencyHistogramBucketCount> connect_time_histogram{};
  std::uint64_t upstream_write_count{0};
  std::uint64_t upstream_write_time_ns_sum{0};
  std::uint64_t upstream_write_time_ns_max{0};
  std::array<std::uint64_t, kPoolLatencyHistogramBucketCount> upstream_write_time_histogram{};
  std::uint64_t relay_count{0};
  std::uint64_t relay_time_ns_sum{0};
  std::uint64_t relay_time_ns_max{0};
  std::array<std::uint64_t, kPoolLatencyHistogramBucketCount> relay_time_histogram{};
};

// Backend-neutral upstream keep-alive pool. The pool owns already-connected
// AsyncStream objects and keys them by stable UpstreamPeer address.
template <coropact::io::AsyncStream Stream>
class UpstreamStreamPool {
private:
  struct GlobalIdleTag {};
  struct PeerIdleTag {};

  struct PeerQueue;

  struct IdleEntry
      : coropact::ds::ListNode<IdleEntry, GlobalIdleTag>,
        coropact::ds::ListNode<IdleEntry, PeerIdleTag> {
    IdleEntry(Stream value,
              const UpstreamPeer* upstream_peer,
              PeerQueue* queue,
              std::chrono::steady_clock::time_point idle_time)
        : stream(std::move(value)),
          peer(upstream_peer),
          owner_queue(queue),
          idle_since(idle_time) {}

    Stream stream;
    const UpstreamPeer* peer;
    PeerQueue* owner_queue;
    std::chrono::steady_clock::time_point idle_since;
  };

  using PeerIdleList = coropact::ds::IntrusiveList<IdleEntry, PeerIdleTag>;

  struct PeerQueue {
    explicit PeerQueue(const UpstreamPeer* upstream_peer) : peer(upstream_peer) {}

    const UpstreamPeer* peer;
    PeerIdleList idle;
  };

  using GlobalIdleLRU = coropact::cache::IntrusiveLRU<IdleEntry, GlobalIdleTag>;

public:
  using StreamHolder = std::optional<Stream>;

  explicit UpstreamStreamPool(PoolConfig cfg = {}) : config_(cfg) {}

  COROPACT_DELETE_COPY(UpstreamStreamPool);

  UpstreamStreamPool(UpstreamStreamPool&& other) noexcept
      : config_(other.config_),
        stats_(other.stats_),
        idle_total_(other.idle_total_),
        peer_queues_(std::move(other.peer_queues_)),
        global_idle_(std::move(other.global_idle_)) {
    other.idle_total_ = 0;
  }

  UpstreamStreamPool& operator=(UpstreamStreamPool&& other) noexcept {
    if (this != &other) {
      ClearEntries();
      config_ = other.config_;
      stats_ = other.stats_;
      idle_total_ = other.idle_total_;
      peer_queues_ = std::move(other.peer_queues_);
      global_idle_ = std::move(other.global_idle_);
      other.idle_total_ = 0;
    }
    return *this;
  }

  ~UpstreamStreamPool() { ClearEntries(); }

  StreamHolder Acquire(const UpstreamPeer* peer) {
    if (config_.collect_stats) {
      ++stats_.acquire_count;
    }
    auto it = FindPeer(peer);
    if (it == peer_queues_.end()) {
      if (config_.collect_stats) {
        ++stats_.acquire_miss_count;
      }
      return std::nullopt;
    }

    PeerQueue* queue = &*it;
    auto* entry = queue->idle.PopBack();
    if (entry == nullptr) {
      peer_queues_.erase(it);
      if (config_.collect_stats) {
        ++stats_.acquire_miss_count;
      }
      return std::nullopt;
    }

    const bool erased = global_idle_.Erase(entry);
    assert(erased);
    --idle_total_;
    StreamHolder stream{std::move(entry->stream)};
    delete entry;
    if (queue->idle.empty()) {
      peer_queues_.erase(it);
    }
    if (config_.collect_stats) {
      ++stats_.acquire_hit_count;
    }
    return stream;
  }

  void Release(const UpstreamPeer* peer, StreamHolder stream) {
    if (!stream || (config_.max_idle_per_peer <= 0 && config_.max_idle_total == 0)) {
      if (config_.collect_stats) {
        ++stats_.release_drop_count;
      }
      return;
    }

    auto it = FindPeer(peer);
    if (it != peer_queues_.end() && config_.max_idle_per_peer > 0 &&
        static_cast<int>(it->idle.size()) >= config_.max_idle_per_peer) {
      if (config_.collect_stats) {
        ++stats_.release_drop_count;
      }
      return;
    }

    if (config_.max_idle_total != 0 && idle_total_ >= config_.max_idle_total && !EvictOldest()) {
      if (config_.collect_stats) {
        ++stats_.release_drop_count;
      }
      return;
    }

    it = FindPeer(peer);
    if (it == peer_queues_.end()) {
      it = peer_queues_.emplace(peer_queues_.end(), peer);
    }

    auto owned = std::make_unique<IdleEntry>(std::move(*stream), peer, &*it,
                                             std::chrono::steady_clock::now());
    auto* entry = owned.get();
    if (!global_idle_.PushMRU(entry)) {
      if (config_.collect_stats) {
        ++stats_.release_drop_count;
      }
      return;
    }
    const bool linked_to_peer = it->idle.PushBack(entry);
    assert(linked_to_peer);
    if (!linked_to_peer) {
      global_idle_.Erase(entry);
      return;
    }
    owned.release();
    ++idle_total_;
    if (config_.collect_stats) {
      ++stats_.release_count;
    }
  }

  void EvictStale() {
    const auto now = std::chrono::steady_clock::now();
    const auto timeout = std::chrono::duration<double>(config_.keepalive_timeout_sec);
    while (auto* oldest = global_idle_.Oldest()) {
      if (now - oldest->idle_since < timeout) break;
      if (!EvictOldest()) break;
    }
  }

  [[nodiscard]] const UpstreamStreamPoolStats& stats() const noexcept { return stats_; }

  [[nodiscard]] bool stats_enabled() const noexcept { return config_.collect_stats; }

  void RecordConnect(std::uint64_t elapsed_ns, bool success) noexcept {
    if (!config_.collect_stats) return;
    ++stats_.connect_attempt_count;
    if (success) {
      ++stats_.connect_success_count;
    } else {
      ++stats_.connect_failure_count;
    }
    stats_.connect_time_ns_sum += elapsed_ns;
    stats_.connect_time_ns_max = std::max(stats_.connect_time_ns_max, elapsed_ns);
    RecordHistogram(&stats_.connect_time_histogram, elapsed_ns);
  }

  void RecordUpstreamWrite(std::uint64_t elapsed_ns) noexcept {
    if (!config_.collect_stats) return;
    ++stats_.upstream_write_count;
    stats_.upstream_write_time_ns_sum += elapsed_ns;
    stats_.upstream_write_time_ns_max = std::max(stats_.upstream_write_time_ns_max, elapsed_ns);
    RecordHistogram(&stats_.upstream_write_time_histogram, elapsed_ns);
  }

  void RecordRelay(std::uint64_t elapsed_ns) noexcept {
    if (!config_.collect_stats) return;
    ++stats_.relay_count;
    stats_.relay_time_ns_sum += elapsed_ns;
    stats_.relay_time_ns_max = std::max(stats_.relay_time_ns_max, elapsed_ns);
    RecordHistogram(&stats_.relay_time_histogram, elapsed_ns);
  }

  [[nodiscard]] std::size_t idle_count() const noexcept { return idle_total_; }

private:
  static void RecordHistogram(
      std::array<std::uint64_t, kPoolLatencyHistogramBucketCount>* histogram,
      std::uint64_t elapsed_ns) noexcept {
    std::size_t bucket = 0;
    while (elapsed_ns > 1 && bucket + 1 < kPoolLatencyHistogramBucketCount) {
      elapsed_ns >>= 1;
      ++bucket;
    }
    ++(*histogram)[bucket];
  }

  bool EvictOldest() {
    auto* oldest = global_idle_.PopLRU();
    if (oldest == nullptr) return false;

    PeerQueue* queue = oldest->owner_queue;
    const bool erased = queue->idle.Erase(oldest);
    assert(erased);
    --idle_total_;
    delete oldest;
    EraseEmptyPeerQueue(queue);
    return true;
  }

  auto FindPeer(const UpstreamPeer* peer) {
    return std::find_if(peer_queues_.begin(), peer_queues_.end(),
                        [peer](const PeerQueue& entry) { return entry.peer == peer; });
  }

  void EraseEmptyPeerQueue(PeerQueue* queue) {
    if (queue == nullptr || !queue->idle.empty()) return;
    auto it = std::find_if(peer_queues_.begin(), peer_queues_.end(),
                           [queue](const PeerQueue& entry) { return &entry == queue; });
    if (it != peer_queues_.end()) {
      peer_queues_.erase(it);
    }
  }

  void ClearEntries() noexcept {
    while (auto* entry = global_idle_.PopLRU()) {
      PeerQueue* queue = entry->owner_queue;
      const bool erased = queue->idle.Erase(entry);
      assert(erased);
      delete entry;
      --idle_total_;
    }
    peer_queues_.clear();
    idle_total_ = 0;
  }

  PoolConfig config_;
  UpstreamStreamPoolStats stats_;
  std::size_t idle_total_{0};
  // A pool is worker-local and upstream groups are normally small (the benchmark
  // uses four peers). Linear pointer lookup avoids a per-request hash. Each
  // entry has one hook for the global LRU and one hook for its peer's LIFO
  // queue, so cross-list removal does not scan a peer vector.
  std::list<PeerQueue> peer_queues_;
  GlobalIdleLRU global_idle_;
};

}  // namespace coropact::gateway
