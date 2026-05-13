#pragma once

#include "runtime/base/murmurhash3.h"
#include "runtime/gateway/upstream.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <limits>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <random>
#include <string>
#include <string_view>
#include <unordered_map>

namespace runtime::gateway {

struct RequestContext {
  std::string client_ip;
  std::string uri;
  std::string user_id;
  std::string session_id;
};
class LoadBalancer {
public:
  virtual ~LoadBalancer() = default;
  virtual std::shared_ptr<UpstreamPeer> Select(Upstream& upstream, const RequestContext ctx = {}) = 0;
};

// -- Round Robin --
class RoundRobinLB : public LoadBalancer {
public:
  std::shared_ptr<UpstreamPeer> Select(Upstream& upstream,  const RequestContext ctx = {}) override {
    auto peers = upstream.Available();
    if (peers.empty()) return nullptr;

    uint64_t idx = counter_.fetch_add(1, std::memory_order_relaxed);
    return peers[idx % peers.size()];
  }

private:
  std::atomic<uint64_t> counter_{0};
};

// -- Smooth Weighted Round Robin --
class WeightedRoundRobinLB : public LoadBalancer {
public:
  std::shared_ptr<UpstreamPeer> Select(Upstream& upstream, const RequestContext ctx = {}) override {
    auto peers = upstream.Available();
    if (peers.empty()) return nullptr;

    std::lock_guard lk(mutex_);

    int total = 0;
    for (const auto& peer : peers) {
      total += std::max(peer->Config().weight, 0);
    }

    if (total <= 0) return nullptr;

    std::shared_ptr<UpstreamPeer> best;
    int best_weight = std::numeric_limits<int>::min();

    for (const auto& peer : peers) {
      int weight = std::max(peer->Config().weight, 0);
      auto& cur = current_weights_[peer->Config().name];

      cur += weight;

      if (cur > best_weight) {
        best_weight = cur;
        best = peer;
      }
    }

    if (best) {
      current_weights_[best->Config().name] -= total;
    }

    return best;
  }

private:
  std::mutex mutex_;
  std::unordered_map<std::string, int> current_weights_;
};

// -- Least Connections --
class LeastConnectionLB : public LoadBalancer {
public:
  std::shared_ptr<UpstreamPeer> Select(Upstream& upstream,  const RequestContext ctx = {}) override {
    auto peers = upstream.Available();
    if (peers.empty()) return nullptr;

    std::shared_ptr<UpstreamPeer> best;
    int min_active = std::numeric_limits<int>::max();

    for (const auto& peer : peers) {
      int active = peer->State().active.load(std::memory_order_relaxed);
      if (active < min_active) {
        min_active = active;
        best = peer;
      }
    }

    return best;
  }
};

// -- Random --
class RandomLB : public LoadBalancer {
public:
  std::shared_ptr<UpstreamPeer> Select(Upstream& upstream,  const RequestContext ctx = {}) override {
    auto peers = upstream.Available();
    if (peers.empty()) return nullptr;

    thread_local std::mt19937 gen{std::random_device{}()};
    std::uniform_int_distribution<size_t> dist(0, peers.size() - 1);

    return peers[dist(gen)];
  }
};

// -- Weighted Random --
class WeightedRandomLB : public LoadBalancer {
public:
  std::shared_ptr<UpstreamPeer> Select(Upstream& upstream,  const RequestContext ctx = {}) override {
    auto peers = upstream.Available();
    if (peers.empty()) return nullptr;

    int total_weight = 0;
    for (const auto& peer : peers) {
      total_weight += std::max(peer->Config().weight, 0);
    }

    if (total_weight <= 0) return nullptr;

    thread_local std::mt19937 gen{std::random_device{}()};
    std::uniform_int_distribution<int> dist(1, total_weight);

    int r = dist(gen);

    for (const auto& peer : peers) {
      r -= std::max(peer->Config().weight, 0);
      if (r <= 0) {
        return peer;
      }
    }

    return peers.back();
  }
};

// -- IP-Hash --
class IPHashLB : public LoadBalancer {
public:
  std::shared_ptr<UpstreamPeer> Select(Upstream& upstream,  const RequestContext ctx = {}) override {
    auto peers = upstream.Available();
    if (peers.empty()) return nullptr;

    const auto& key = ctx.client_ip.empty() ? "0.0.0.0" : ctx.client_ip;
    uint32_t hash = runtime::base::MurmurHash3(key);
    return peers[hash % peers.size()];
  }
};

// -- Consistent-Hash --
// 一致性哈希负载均衡器， 采用虚拟节点 + 权重支持
// hash 函数: MurmurHash3
// hash_on 决定用 RequestContext 的哪个字段做路由。
class ConsistentHashLB : public LoadBalancer {
public:
  // 虚拟节点数量 值域(边界性检查)
  // 可以在此调节虚拟节点的值域参数。
  // 追求分布均匀，重建哈希环开销的权衡。 同时要考虑虚拟节点增加对分布提升效果
  // 一般来说如果 Key 规模足够大， 哈希函数本身就保证大样本分布均匀性。 
  static constexpr int kVNODESMAX = 200;
  static constexpr int kVNODESMIN = 100;

  // hash_on: "client_ip" | "uri" | "user_id" | "session_id"
  explicit ConsistentHashLB(int vnodes_per_unit = 150,
                            std::string hash_on = "client_ip") 
    : vnodes_per_unit_(vnodes_per_unit),
      hash_on_(std::move(hash_on)) {
    if (vnodes_per_unit < kVNODESMIN) vnodes_per_unit_ = kVNODESMIN;
    if (vnodes_per_unit > kVNODESMAX) vnodes_per_unit_ = kVNODESMAX;
  }
  std::shared_ptr<UpstreamPeer> Select(Upstream& upstream,  
                                       const RequestContext ctx = {}) override {
    auto peers = upstream.Available();
    if (peers.empty()) return nullptr;

    auto fp = ComputeFingerprint(peers);

    // 快路径： 指纹相同 -> 直接查环
    {
      std::shared_lock lk{mutex_};
      if (ring_ && ring_->fingerprint == fp) {
        return Lookup(*ring_, base::MurmurHash3(HashKey(ctx)));
      }
    }
    // 慢路径: 写锁内重新获取 peers 并二次确认指纹，避免竞态
    {
      std::lock_guard lk{mutex_};
      auto fresh_peers = upstream.Available();
      auto fresh_fp = ComputeFingerprint(fresh_peers);
      if (!ring_ || ring_->fingerprint != fresh_fp) {
        ring_ = BuildRing(fresh_peers);
      }
      return Lookup(*ring_, base::MurmurHash3(HashKey(ctx)));
    }
    
  }
private:
  // 哈希环的内部表示
  // 查找频繁， 增加和删除 很少 => 读多写少的场景
  // 从算法复杂度看， 红黑树 对于频繁插入删除的场景频繁。
  // 从查询多的场景， 内存连续 + 有序数组(二分查找) + CPU缓存 => 完爆红黑树， 均摊来看有序数组更好。
  struct HashRing {
    // {hash32, peer_ptr} 按 hash 升序排列
    std::vector<std::pair<uint32_t, std::shared_ptr<UpstreamPeer>>> nodes; //sorted by hash
    std::string fingerprint; // "peer1:w1;peer2:w2;..."
  };

  // 计算指纹 
  static std::string ComputeFingerprint(const std::vector<std::shared_ptr<UpstreamPeer>>& peers) {
    std::string fp;
    fp.reserve(peers.size() * 32); // 
    for (const auto& peer : peers) {
      fp += peer->Config().name;
      fp += ':';
      fp += std::to_string(peer->Config().weight);
      fp += ';';
    }
    return fp;
  }

  // 时间复杂度: O(N + V * N + NlogN) => O(max(V * N, NlogN))
  std::shared_ptr<const HashRing> BuildRing(
      const std::vector<std::shared_ptr<UpstreamPeer>>& peers) const {
    auto ring = std::make_shared<HashRing>();

    // 估计一下虚拟节点总数， 提前预留空间
    int total_weight = 0;
    for (const auto& peer : peers) {
      total_weight += std::max(peer->Config().weight, 1);
    }
    ring->nodes.reserve(total_weight * vnodes_per_unit_);

    // 物理节点 -> 生成虚拟节点
    for (const auto& peer : peers) {
      int weight = std::max(peer->Config().weight, 1);
      int vnode_cnt = weight * vnodes_per_unit_;
      for (int i = 0; i < vnode_cnt; i++) {
        // 虚拟节点标识: peer_name#0, peer_name#1, ...
        std::string vnode_key = peer->Config().name;
        vnode_key += "#";
        vnode_key += std::to_string(i);
        uint32_t hash = base::MurmurHash3(vnode_key);
        ring->nodes.emplace_back(hash, peer);
      }
    }
    // 按哈希值排序 O(logn)
    std::sort(ring->nodes.begin(), ring->nodes.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
    ring->fingerprint = ComputeFingerprint(peers);

    // 哈希碰撞的处理方案
    // 方案一(采用)： 排序后去重
    auto it = std::unique(ring->nodes.begin(), ring->nodes.end(), [](const auto& a, const auto& b) { return a.first == b.first; });
    ring->nodes.erase(it, ring->nodes.end());
    return ring;
  }

  // -- 从 RequestContext 中取哈希键 --
  const std::string& HashKey(const RequestContext& ctx) const {
    if (hash_on_ == "uri" && !ctx.uri.empty())        return ctx.uri;
    if (hash_on_ == "user_id" && !ctx.user_id.empty())    return ctx.user_id;
    if (hash_on_ == "session_id" && !ctx.session_id.empty()) return ctx.session_id;
    // 默认 fallback: client_ip
    if (!ctx.client_ip.empty()) return ctx.client_ip;

    // 极端情况, 所有字段都为空串 "默认构造情况" , 返回静态子串。
    static const std::string empty;
    return empty;
  }

  static std::shared_ptr<UpstreamPeer> Lookup(const HashRing& ring, uint32_t hash) {
    auto it = std::lower_bound(ring.nodes.begin(), ring.nodes.end(), hash, 
        [](const auto& node, uint32_t h) { return node.first < h; });
    if (it == ring.nodes.end()) {
      it = ring.nodes.begin();
    }
    return it->second;
  }
private:
  int vnodes_per_unit_;
  std::string hash_on_;
  mutable std::shared_mutex mutex_;
  std::shared_ptr<const HashRing> ring_;
};
namespace detail {

inline std::unique_ptr<LoadBalancer> MakeRoundRobinLB() {
  return std::make_unique<RoundRobinLB>();
}

inline std::unique_ptr<LoadBalancer> MakeLeastConnectionLB() {
  return std::make_unique<LeastConnectionLB>();
}

inline std::unique_ptr<LoadBalancer> MakeRandomLB() {
  return std::make_unique<RandomLB>();
}

inline std::unique_ptr<LoadBalancer> MakeWeightedRandomLB() {
  return std::make_unique<WeightedRandomLB>();
}

inline std::unique_ptr<LoadBalancer> MakeWeightedRoundRobinLB() {
  return std::make_unique<WeightedRoundRobinLB>();
}

inline std::unique_ptr<LoadBalancer> MakeIPHashLB() {
  return std::make_unique<IPHashLB>();
}

inline std::unique_ptr<LoadBalancer> MakeConsistentHashLB() {
  return std::make_unique<ConsistentHashLB>();
}
} // namespace detail

inline std::unique_ptr<LoadBalancer> CreateLoadBalancer(std::string_view algo) {
  using Creator = std::unique_ptr<LoadBalancer> (*)();

  static constexpr std::array<std::pair<std::string_view, Creator>, 7> table = {{
      {std::string_view{"round_robin"}, detail::MakeRoundRobinLB},
      {std::string_view{"weighted_round_robin"}, detail::MakeWeightedRoundRobinLB},
      {std::string_view{"least_connection"}, detail::MakeLeastConnectionLB},
      {std::string_view{"random"}, detail::MakeRandomLB},
      {std::string_view{"weighted_random"}, detail::MakeWeightedRandomLB},
      {std::string_view{"ip_hash"}, detail::MakeIPHashLB},
      {std::string_view{"consistent_hash"}, detail::MakeConsistentHashLB},
  }};

  for (const auto& [name, creator] : table) {
    if (algo == name) {
      return creator();
    }
  }

  return nullptr;
}

} // namespace runtime::gateway