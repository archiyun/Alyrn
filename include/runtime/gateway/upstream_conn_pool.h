#pragma once

#include "runtime/gateway/upstream_peer.h"
#include "runtime/net/tcp_connection.h"

#include <chrono>
#include <deque>
#include <string>
#include <unordered_map>

namespace runtime::gateway {

struct PoolConfig {
  int max_idle_per_peer{8};
  double keepalive_timeout_sec{60.0};
};

// 仿照 nginx per-worker keepalive的思路
class UpstreamConnPool {
public:
  using TcpConnectionPtr = runtime::net::TcpConnection::TcpConnectionPtr;

  explicit UpstreamConnPool(PoolConfig cfg = {}) : config_(cfg) {}

  TcpConnectionPtr Acquire(const std::string& peer_name) {
    auto it = idle_.find(peer_name);
    if (it == idle_.end() || it->second.empty()) return nullptr;

    auto& q = it->second;
    while (!q.empty()) {
      auto entry = std::move(q.front());
      q.pop_front();
      if (entry.conn && entry.conn->Connected()) {
        return entry.conn;
      }
    }
    idle_.erase(it);
    return nullptr;
  }

  void Release(const std::string& peer_name, TcpConnectionPtr conn) {
    if (!conn || !conn->Connected()) return;
    
    auto& q = idle_[peer_name];
    if (static_cast<int>(q.size()) >= config_.max_idle_per_peer) {
      conn->Shutdown();
      return;
    } 
    q.push_back({std::move(conn), std::chrono::steady_clock::now()});
  }

  void Evict(const std::string& peer_name, const TcpConnectionPtr& conn) {
    auto it = idle_.find(peer_name);
    if (it == idle_.end()) return;
    auto& q = it->second;
    std::erase_if(q, [&](const IdleEntry& e) { return e.conn == conn; });
    if (q.empty()) idle_.erase(it);
  }

  // 定时器回调: 清理超过 keepalive_timeout 的空闲连接
  void EvictStale() {
    const auto now = std::chrono::steady_clock::now();
    const auto timeout = std::chrono::duration<double>(config_.keepalive_timeout_sec);
    for (auto it = idle_.begin(); it != idle_.end(); ) {
      auto& q = it->second;
      std::erase_if(q, [&](const IdleEntry& e) {
        if (now - e.idle_since >= timeout) {
          e.conn->Shutdown();
          return true;
        }
        return false;
      });
      it = q.empty() ? idle_.erase(it) : std::next(it);
    }
  }
private:
  struct IdleEntry {
    TcpConnectionPtr conn;
    std::chrono::steady_clock::time_point idle_since;
  };
  PoolConfig config_;
  // upstream_name -> QUEUE<IdleEntry>
  std::unordered_map<std::string, std::deque<IdleEntry>> idle_;
};
} // namespace runtime::gateway

