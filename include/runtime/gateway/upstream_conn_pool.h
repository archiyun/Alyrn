// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <chrono>
#include <deque>
#include <memory>
#include <string>
#include <unordered_map>

#include "runtime/gateway/upstream_peer.h"
#include "runtime/net/tcp_client.h"
#include "runtime/net/tcp_connection.h"

namespace runtime::gateway {

struct PoolConfig {
  int max_idle_per_peer{8};
  double keepalive_timeout_sec{60.0};
};

// Per-event-loop upstream keep-alive pool, modeled after nginx's per-worker
// upstream keepalive cache.
//
// The pool owns whole TcpClient objects, not just TcpConnection shared_ptrs.
// TcpClient owns the connector/client-side connection lifecycle; destroying it
// tears down the underlying TcpConnection. Keeping only TcpConnectionPtr would
// leave the pooled connection unusable once the original TcpClient leaves scope.
//
// UpstreamConnPool is intended to be used from one EventLoop thread. The server
// keeps one pool per loop, so this class does not add its own synchronization.
class UpstreamConnPool {
public:
  using TcpConnectionPtr = runtime::net::TcpConnection::TcpConnectionPtr;
  using TcpClientPtr = std::unique_ptr<runtime::net::TcpClient>;

  explicit UpstreamConnPool(PoolConfig cfg = {}) : config_(cfg) {}

  // Returns a reusable connected TcpClient for peer_name.
  //
  // Stale or already-closed entries are discarded while scanning the idle
  // queue. nullptr means no live idle connection is currently available.
  TcpClientPtr Acquire(const std::string& peer_name) {
    auto it = idle_.find(peer_name);
    if (it == idle_.end() || it->second.empty()) return nullptr;

    auto& q = it->second;
    while (!q.empty()) {
      auto entry = std::move(q.front());
      q.pop_front();
      if (entry.client && entry.client->connection() &&
          entry.client->connection()->Connected()) {
        return std::move(entry.client);
      }
      // Dead entry: local TcpClient destruction cleans up the socket.
    }
    idle_.erase(it);
    return nullptr;
  }

  // Returns a connected upstream client to the idle queue.
  //
  // Closed clients are dropped. If the peer already has max_idle_per_peer idle
  // clients, the returned client is disconnected instead of being cached.
  void Release(const std::string& peer_name, TcpClientPtr client) {
    if (!client || !client->connection() || !client->connection()->Connected()) {
      return;
    }
    auto& q = idle_[peer_name];
    if (static_cast<int>(q.size()) >= config_.max_idle_per_peer) {
      client->Disconnect();
      return;
    }
    q.push_back({std::move(client), std::chrono::steady_clock::now()});
  }

  // Timer callback: evicts idle clients older than keepalive_timeout_sec.
  void EvictStale() {
    const auto now = std::chrono::steady_clock::now();
    const auto timeout = std::chrono::duration<double>(config_.keepalive_timeout_sec);
    for (auto it = idle_.begin(); it != idle_.end(); ) {
      auto& q = it->second;
      std::erase_if(q, [&](IdleEntry& e) {
        if (now - e.idle_since >= timeout) {
          if (e.client) e.client->Disconnect();
          return true;
        }
        return false;
      });
      it = q.empty() ? idle_.erase(it) : std::next(it);
    }
  }
private:
  struct IdleEntry {
    TcpClientPtr client;
    std::chrono::steady_clock::time_point idle_since;
  };
  PoolConfig config_;
  // peer_name -> idle clients for that peer.
  std::unordered_map<std::string, std::deque<IdleEntry>> idle_;
};

}  // namespace runtime::gateway
