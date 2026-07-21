// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <algorithm>
#include <chrono>
#include <iterator>
#include <optional>
#include <vector>

#include "vexo/gateway/upstream_peer.h"
#include "vexo/io/async_stream.h"

namespace vexo::gateway {

struct PoolConfig {
  int max_idle_per_peer{8};
  double keepalive_timeout_sec{60.0};
};

// Backend-neutral upstream keep-alive pool. The pool owns already-connected
// AsyncStream objects and keys them by stable UpstreamPeer address.
template <vexo::io::AsyncStream Stream>
class UpstreamStreamPool {
public:
  using StreamHolder = std::optional<Stream>;

  explicit UpstreamStreamPool(PoolConfig cfg = {}) : config_(cfg) {}

  StreamHolder Acquire(const UpstreamPeer* peer) {
    auto it = FindPeer(peer);
    if (it == peer_queues_.end()) return std::nullopt;

    auto& idle = it->idle;
    while (!idle.empty()) {
      auto entry = std::move(idle.back());
      idle.pop_back();
      return std::move(entry.stream);
    }

    peer_queues_.erase(it);
    return std::nullopt;
  }

  void Release(const UpstreamPeer* peer, StreamHolder stream) {
    if (!stream || config_.max_idle_per_peer <= 0) return;

    auto it = FindPeer(peer);
    if (it == peer_queues_.end()) {
      peer_queues_.push_back(PeerQueue{.peer = peer});
      it = std::prev(peer_queues_.end());
      it->idle.reserve(static_cast<std::size_t>(config_.max_idle_per_peer));
    }

    auto& idle = it->idle;
    if (static_cast<int>(idle.size()) >= config_.max_idle_per_peer) {
      return;
    }
    idle.push_back({std::move(*stream), std::chrono::steady_clock::now()});
  }

  void EvictStale() {
    const auto now = std::chrono::steady_clock::now();
    const auto timeout = std::chrono::duration<double>(config_.keepalive_timeout_sec);
    for (auto it = peer_queues_.begin(); it != peer_queues_.end();) {
      auto& idle = it->idle;
      std::erase_if(idle, [&](IdleEntry& e) { return now - e.idle_since >= timeout; });
      it = idle.empty() ? peer_queues_.erase(it) : std::next(it);
    }
  }

private:
  struct IdleEntry {
    Stream stream;
    std::chrono::steady_clock::time_point idle_since;
  };

  struct PeerQueue {
    const UpstreamPeer* peer;
    std::vector<IdleEntry> idle;
  };

  auto FindPeer(const UpstreamPeer* peer) {
    return std::find_if(peer_queues_.begin(), peer_queues_.end(),
                        [peer](const PeerQueue& entry) { return entry.peer == peer; });
  }

  PoolConfig config_;
  // A pool is worker-local and upstream groups are normally small (the benchmark
  // uses four peers). Linear pointer lookup avoids a per-request hash and keeps
  // each peer's idle entries contiguous. LIFO reuse is intentional: idle streams
  // have no ordering contract, and pop_back avoids shifting a deque/vector.
  std::vector<PeerQueue> peer_queues_;
};

}  // namespace vexo::gateway
