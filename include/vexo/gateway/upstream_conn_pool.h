// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <algorithm>
#include <chrono>
#include <deque>
#include <memory>
#include <unordered_map>

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
  using StreamPtr = std::unique_ptr<Stream>;

  explicit UpstreamStreamPool(PoolConfig cfg = {}) : config_(cfg) {}

  StreamPtr Acquire(const UpstreamPeer* peer) {
    auto it = idle_.find(peer);
    if (it == idle_.end() || it->second.empty()) return nullptr;

    auto& q = it->second;
    while (!q.empty()) {
      auto entry = std::move(q.front());
      q.pop_front();
      if (entry.stream) return std::move(entry.stream);
    }
    idle_.erase(it);
    return nullptr;
  }

  void Release(const UpstreamPeer* peer, StreamPtr stream) {
    if (!stream) return;
    auto& q = idle_[peer];
    if (static_cast<int>(q.size()) >= config_.max_idle_per_peer) {
      return;
    }
    q.push_back({std::move(stream), std::chrono::steady_clock::now()});
  }

  void EvictStale() {
    const auto now = std::chrono::steady_clock::now();
    const auto timeout = std::chrono::duration<double>(config_.keepalive_timeout_sec);
    for (auto it = idle_.begin(); it != idle_.end();) {
      auto& q = it->second;
      std::erase_if(q, [&](IdleEntry& e) { return now - e.idle_since >= timeout; });
      it = q.empty() ? idle_.erase(it) : std::next(it);
    }
  }

private:
  struct IdleEntry {
    StreamPtr stream;
    std::chrono::steady_clock::time_point idle_since;
  };

  PoolConfig config_;
  std::unordered_map<const UpstreamPeer*, std::deque<IdleEntry>> idle_;
};

}  // namespace vexo::gateway
