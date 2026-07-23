// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <list>
#include <optional>
#include <vector>

#include "coropact/gateway/upstream_peer.h"
#include "coropact/io/async_stream.h"

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
public:
  using StreamHolder = std::optional<Stream>;

  explicit UpstreamStreamPool(PoolConfig cfg = {}) : config_(cfg) {}

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

    auto& idle = it->idle;
    while (!idle.empty()) {
      auto entry = idle.back();
      idle.pop_back();
      auto stream = std::move(entry->stream);
      idle_entries_.erase(entry);
      --idle_total_;
      if (config_.collect_stats) {
        ++stats_.acquire_hit_count;
      }
      return stream;
    }

    peer_queues_.erase(it);
    if (config_.collect_stats) {
      ++stats_.acquire_miss_count;
    }
    return std::nullopt;
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

    // EvictOldest may remove the last entry from this peer and invalidate it.
    it = FindPeer(peer);
    if (it == peer_queues_.end()) {
      peer_queues_.push_back(PeerQueue{.peer = peer});
      it = std::prev(peer_queues_.end());
      if (config_.max_idle_per_peer > 0) {
        it->idle.reserve(static_cast<std::size_t>(config_.max_idle_per_peer));
      }
    }

    auto& idle = it->idle;
    if (config_.max_idle_per_peer > 0 &&
        static_cast<int>(idle.size()) >= config_.max_idle_per_peer) {
      if (config_.collect_stats) {
        ++stats_.release_drop_count;
      }
      return;
    }
    idle_entries_.push_back(IdleEntry{.stream = std::move(*stream),
                                      .idle_since = std::chrono::steady_clock::now(),
                                      .peer = peer});
    idle.push_back(std::prev(idle_entries_.end()));
    ++idle_total_;
    if (config_.collect_stats) {
      ++stats_.release_count;
    }
  }

  void EvictStale() {
    const auto now = std::chrono::steady_clock::now();
    const auto timeout = std::chrono::duration<double>(config_.keepalive_timeout_sec);
    while (!idle_entries_.empty() && now - idle_entries_.front().idle_since >= timeout) {
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
    if (idle_entries_.empty()) return false;

    auto oldest = idle_entries_.begin();
    auto peer = FindPeer(oldest->peer);
    if (peer == peer_queues_.end()) return false;

    auto peer_entry = std::find(peer->idle.begin(), peer->idle.end(), oldest);
    if (peer_entry == peer->idle.end()) return false;
    peer->idle.erase(peer_entry);
    idle_entries_.erase(oldest);
    --idle_total_;
    if (peer->idle.empty()) {
      peer_queues_.erase(peer);
    }
    return true;
  }

  struct IdleEntry {
    Stream stream;
    std::chrono::steady_clock::time_point idle_since;
    const UpstreamPeer* peer;
  };

  using IdleList = std::list<IdleEntry>;
  using IdleIterator = IdleList::iterator;

  struct PeerQueue {
    const UpstreamPeer* peer;
    std::vector<IdleIterator> idle;
  };

  auto FindPeer(const UpstreamPeer* peer) {
    return std::find_if(peer_queues_.begin(), peer_queues_.end(),
                        [peer](const PeerQueue& entry) { return entry.peer == peer; });
  }

  PoolConfig config_;
  UpstreamStreamPoolStats stats_;
  std::size_t idle_total_{0};
  // A pool is worker-local and upstream groups are normally small (the benchmark
  // uses four peers). Linear pointer lookup avoids a per-request hash. Per-peer
  // vectors retain LIFO reuse while the list keeps a global release order for
  // O(1) insertion and oldest-first eviction.
  std::vector<PeerQueue> peer_queues_;
  IdleList idle_entries_;
};

}  // namespace coropact::gateway
