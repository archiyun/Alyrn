#pragma once

#include "runtime/gateway/upstream_peer.h"
#include "runtime/net/tcp_client.h"
#include "runtime/net/tcp_connection.h"

#include <chrono>
#include <deque>
#include <memory>
#include <string>
#include <unordered_map>

namespace runtime::gateway {

struct PoolConfig {
  int max_idle_per_peer{8};
  double keepalive_timeout_sec{60.0};
};

// 仿照 nginx per-worker keepalive的思路。
// 池接管整个 TcpClient (而不仅是 TcpConnection)，因为 TcpClient 析构会
// 调 ConnectDestroyed() 销毁底层 socket —— 如果只存 TcpConnectionPtr，
// 上游 TcpClient 一离开作用域 conn 就废了。
class UpstreamConnPool {
public:
  using TcpConnectionPtr = runtime::net::TcpConnection::TcpConnectionPtr;
  using TcpClientPtr = std::unique_ptr<runtime::net::TcpClient>;

  explicit UpstreamConnPool(PoolConfig cfg = {}) : config_(cfg) {}

  // 返回一个仍然 Connected() 的 TcpClient (其 connection() 可立即使用)；
  // 若池中无可复用连接返回 nullptr。
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
      // 死连接：TcpClient 局部析构会清理底层 socket
    }
    idle_.erase(it);
    return nullptr;
  }

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

  // 定时器回调: 清理超过 keepalive_timeout 的空闲连接
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
  // upstream_name -> QUEUE<IdleEntry>
  std::unordered_map<std::string, std::deque<IdleEntry>> idle_;
};
} // namespace runtime::gateway

