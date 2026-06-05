// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Load-balancer strategies. Each strategy is a stateless (or near-stateless)
// policy object held by a Route; Select() is invoked on every proxy request
// to pick one UpstreamPeer out of the Upstream's peer list.
//
// Design notes:
//  * No allocation in Select() — every strategy filters Upstream::peers()
//    inline via UpstreamPeer::Available() to avoid materializing a snapshot.
//  * Per-peer atomic state (effective_weight, active, fails, ...) is read
//    relaxed; load balancing tolerates slightly stale values.
//  * CreateLoadBalancer(name) is the string-to-factory entry point used by
//    GatewayServer::AddProxyRoute.
#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <limits>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <string_view>
#include <unordered_map>

#include "runtime/ds/murmurhash3.h"
#include "runtime/gateway/upstream.h"

namespace runtime::gateway {

// Per-request signals forwarded to LBs that route on request attributes
// (IP hash, consistent hash on a specific field). Populated by GatewayServer
// before calling Select(); empty strings mean "not provided".
struct RequestContext {
  std::string client_ip;
  std::string uri;
  std::string user_id;
  std::string session_id;
};

// Abstract base for all load-balancing strategies.
// Implementations must be thread-safe: Select() may be invoked concurrently
// from any IO thread that processes a proxy request.
class LoadBalancer {
public:
  virtual ~LoadBalancer() = default;
  // Picks one available peer from `upstream`, or nullptr if no peer is eligible.
  // `ctx` is consulted only by hash-based strategies; other strategies ignore it.
  virtual std::shared_ptr<UpstreamPeer> Select(Upstream& upstream,
                                               const RequestContext ctx = {}) = 0;
};

// Round-robin: lock-free selection driven by a single atomic counter.
// Two passes over peers_ are needed because the available count must be known
// before the modular pick; a peer transitioning between passes is acceptable
// — UpstreamRequest will retry on a connection failure.
class RoundRobinLB : public LoadBalancer {
public:
  std::shared_ptr<UpstreamPeer> Select(Upstream& upstream, const RequestContext ctx = {}) override {
    const auto& peers = upstream.peers();
    std::size_t avail = 0;
    for (const auto& p : peers) {
      if (p->Available(0)) {
        ++avail;
      }
    }
    if (avail == 0) return nullptr;
    std::size_t pick = counter_.fetch_add(1, std::memory_order_relaxed) % avail;
    for (const auto& p : peers) {
      if (!p->Available(0)) continue;
      if (pick-- == 0) return p;
    }
    return nullptr;  // unreachable
  }

private:
  std::atomic<uint64_t> counter_{0};
};

// Nginx-style smooth weighted round robin.
// Drives selection off each peer's dynamic effective_weight (in [0, config.weight]):
// failures decrement effective_weight, successful health probes increment it back,
// and a peer at effective_weight=0 is silently skipped. current_weights_ holds the
// running selection counters across calls, so the whole step runs under mutex_.
class SmoothWeightedRoundRobinLB : public LoadBalancer {
public:
  std::shared_ptr<UpstreamPeer> Select(Upstream& upstream, const RequestContext ctx = {}) override {
    const auto& peers = upstream.peers();
    std::lock_guard lk{mutex_};

    int total = 0;
    for (const auto& peer : peers) {
      if (!peer->Available(0)) continue;
      total += std::max(peer->effective_weight(), 0);
    }
    if (total <= 0) return nullptr;

    std::shared_ptr<UpstreamPeer> best;
    int best_weight = std::numeric_limits<int>::min();

    for (const auto& peer : peers) {
      if (!peer->Available(0)) continue;
      int weight = std::max(peer->effective_weight(), 0);
      // peer pointers are stable after startup; using them as the key avoids
      // the cost of hashing/comparing peer name strings on every call.
      auto& cur = current_weights_[peer.get()];
      cur += weight;
      if (cur > best_weight) {
        best_weight = cur;
        best = peer;
      }
    }

    if (best) current_weights_[best.get()] -= total;
    return best;
  }

private:
  std::mutex mutex_;
  std::unordered_map<const UpstreamPeer*, int> current_weights_;
};

// Least connections: pick the available peer with the lowest in-flight count.
// Reads `state.active` relaxed; a tie is broken in iteration order.
class LeastConnectionLB : public LoadBalancer {
public:
  std::shared_ptr<UpstreamPeer> Select(Upstream& upstream, const RequestContext ctx = {}) override {
    const auto& peers = upstream.peers();
    std::shared_ptr<UpstreamPeer> best;
    int min_active = std::numeric_limits<int>::max();

    for (const auto& peer : peers) {
      if (!peer->Available(0)) continue;
      int active = peer->active_request();
      if (active < min_active) {
        min_active = active;
        best = peer;
      }
    }
    return best;
  }
};

// Weighted least connections: pick the available peer with the lowest
// active/weight ratio, so a peer configured with twice the weight tolerates
// twice the in-flight requests before it is considered equally loaded.
// Compared in fixed-point (cross-multiply) to stay integer and avoid division;
// ties break in iteration order, matching LeastConnectionLB.
class WeightedLeastConnectionLB : public LoadBalancer {
public:
  std::shared_ptr<UpstreamPeer> Select(Upstream& upstream, const RequestContext ctx = {}) override {
    const auto& peers = upstream.peers();
    std::shared_ptr<UpstreamPeer> best;
    int best_weight = 1;  // weight of `best`, clamped to >= 1 (see std::max below).

    for (const auto& peer : peers) {
      if (!peer->Available(0)) continue;
      // weight==0 would zero a cross-product and corrupt the comparison; clamp
      // to 1 like P2CLB / BuildRing so a misconfigured peer still participates.
      int weight = std::max(peer->weight(), 1);
      if (!best) {
        best = peer;
        best_weight = weight;
        continue;
      }
      // peer is less loaded  <=>  peer.active/weight < best.active/best_weight
      //                      <=>  peer.active * best_weight < best.active * weight
      // Widen one operand to int64_t first so the product can't overflow int.
      const int64_t peer_load = static_cast<int64_t>(peer->active_request()) * best_weight;
      const int64_t best_load = static_cast<int64_t>(best->active_request()) * weight;
      if (peer_load < best_load) {
        best = peer;
        best_weight = weight;
      }
    }
    return best;
  }
};

// Uniform random over available peers. thread_local RNG keeps Select() lock-free.
class RandomLB : public LoadBalancer {
public:
  std::shared_ptr<UpstreamPeer> Select(Upstream& upstream, const RequestContext ctx = {}) override {
    const auto& peers = upstream.peers();
    std::size_t avail = 0;
    for (const auto& p : peers)
      if (p->Available(0)) ++avail;
    if (avail == 0) return nullptr;

    thread_local std::mt19937 gen{std::random_device{}()};
    std::uniform_int_distribution<std::size_t> dist(0, avail - 1);
    std::size_t pick = dist(gen);
    for (const auto& p : peers) {
      if (!p->Available(0)) continue;
      if (pick-- == 0) return p;
    }
    return nullptr;  // unreachable
  }
};

// Weighted random over available peers. Uses static config.weight (not effective_weight)
// so it stays a fixed distribution; gradual demotion is reserved for WRR.
class WeightedRandomLB : public LoadBalancer {
public:
  std::shared_ptr<UpstreamPeer> Select(Upstream& upstream, const RequestContext ctx = {}) override {
    const auto& peers = upstream.peers();
    int total_weight = 0;
    for (const auto& peer : peers) {
      if (!peer->Available(0)) continue;
      total_weight += std::max(peer->weight(), 0);
    }
    if (total_weight <= 0) return nullptr;

    thread_local std::mt19937 gen{std::random_device{}()};
    std::uniform_int_distribution<int> dist(1, total_weight);
    int r = dist(gen);

    std::shared_ptr<UpstreamPeer> last_available;
    for (const auto& peer : peers) {
      if (!peer->Available(0)) continue;
      last_available = peer;
      r -= std::max(peer->weight(), 0);
      if (r <= 0) return peer;
    }
    return last_available;  // Guard against arithmetic edge cases (should be unreachable).
  }
};

// IP-hash: deterministic mapping from client IP to peer index.
// Falls back to "0.0.0.0" when no client IP is provided so the result is
// stable rather than dependent on uninitialized state.
class IPHashLB : public LoadBalancer {
public:
  std::shared_ptr<UpstreamPeer> Select(Upstream& upstream, const RequestContext ctx = {}) override {
    const auto& peers = upstream.peers();
    std::size_t avail = 0;
    for (const auto& p : peers)
      if (p->Available(0)) ++avail;
    if (avail == 0) return nullptr;

    const auto& key = ctx.client_ip.empty() ? "0.0.0.0" : ctx.client_ip;
    uint32_t hash = runtime::ds::MurmurHash3(key);
    std::size_t pick = hash % avail;
    for (const auto& p : peers) {
      if (!p->Available(0)) continue;
      if (pick-- == 0) return p;
    }
    return nullptr;  // unreachable
  }
};

// Consistent hashing with virtual nodes (ketama-style) for stable peer affinity.
//
// Hash function: MurmurHash3 over a per-vnode key "peer_name#i".
// `hash_on`   : which RequestContext field becomes the routing key (parsed once
//                from a string into a HashOn enum at construction time).
//
// Concurrency: the ring is cached behind a shared_mutex. The fast path takes
// a shared_lock and matches a cheap fingerprint of the current peer set; only
// a peer-set change drops to the slow path that holds an exclusive lock and
// rebuilds the ring.
class ConsistentHashLB : public LoadBalancer {
public:
  // Virtual nodes per unit of weight. Trades ring memory and rebuild cost
  // against distribution uniformity; values outside [kVNODESMIN, kVNODESMAX]
  // are clamped at construction.
  static constexpr int kVNODESMAX = 200;
  static constexpr int kVNODESMIN = 100;

  enum class HashOn : uint8_t {
    kClientIp,
    kUri,
    kUserId,
    kSessionId,
  };

  // hash_on accepts: "client_ip" | "uri" | "user_id" | "session_id".
  // Unknown values fall back to "client_ip".
  explicit ConsistentHashLB(int vnodes_per_unit = 150, std::string_view hash_on = "client_ip")
      : vnodes_per_unit_(std::clamp(vnodes_per_unit, kVNODESMIN, kVNODESMAX)),
        hash_on_(ParseHashOn(hash_on)) {}
  std::shared_ptr<UpstreamPeer> Select(Upstream& upstream, const RequestContext ctx = {}) override {
    const auto& peers = upstream.peers();
    auto fp = ComputeFingerprint(peers);
    if (fp.empty()) return nullptr;  // No peer is currently available.

    // Fast path: fingerprint matches the cached ring -> RCU read-end, lock-free.
    auto ring = ring_.load(std::memory_order_acquire);
    if (ring && ring->fingerprint == fp) {
      return Lookup(*ring, runtime::ds::MurmurHash3(HashKey(ctx)));
    }
    // Slow path (RCU update side): serialize rebuilders so a peer-set change
    // rebuilds once, not once per concurrent caller. Re-load under the lock to
    // absorb the race where another writer already published a matching ring.
    std::lock_guard lk{rebuild_mutex_};
    ring = ring_.load(std::memory_order_acquire);
    if (!ring || ring->fingerprint != fp) {
      ring = BuildRing(peers);
      ring_.store(ring, std::memory_order_release);
    }
    return Lookup(*ring, runtime::ds::MurmurHash3(HashKey(ctx)));
  }

private:
  // Sorted-array ring keyed by 32-bit hash. Lookup is hot (binary search) and
  // mutation is rare (only on peer-set change), so a contiguous array beats a
  // red-black tree on both cache locality and constant factors.
  struct HashRing {
    // {hash32, peer_ptr} sorted ascending by hash.
    std::vector<std::pair<uint32_t, std::shared_ptr<UpstreamPeer>>> nodes;
    // Compact identity of the peer set ("name1:w1;name2:w2;..."). Comparing
    // this on every Select() decides whether the ring needs to be rebuilt.
    std::string fingerprint;
  };

  // Compute the fingerprint by filtering inline — avoids materializing a
  // snapshot vector of available peers on each Select().
  static std::string ComputeFingerprint(const std::vector<std::shared_ptr<UpstreamPeer>>& peers) {
    std::string fp;
    fp.reserve(peers.size() * 32);
    for (const auto& peer : peers) {
      if (!peer->Available(0)) continue;
      fp += peer->config().name;
      fp += ':';
      fp += std::to_string(peer->config().weight);
      fp += ';';
    }
    return fp;
  }

  // Build a fresh ring from the current set of available peers.
  // Complexity: O(N + V*N + N log N) where V == vnodes_per_unit_.
  // Generates `weight * V` virtual nodes per peer, sorts them by hash, and
  // dedups collisions (rare with MurmurHash3 at typical scales).
  std::shared_ptr<const HashRing> BuildRing(
      const std::vector<std::shared_ptr<UpstreamPeer>>& peers) const {
    auto ring = std::make_shared<HashRing>();

    int total_weight = 0;
    for (const auto& peer : peers) {
      if (!peer->Available(0)) continue;
      total_weight += std::max(peer->config().weight, 1);
    }
    ring->nodes.reserve(total_weight * vnodes_per_unit_);

    for (const auto& peer : peers) {
      if (!peer->Available(0)) continue;
      int weight = std::max(peer->config().weight, 1);
      int vnode_cnt = weight * vnodes_per_unit_;
      for (int i = 0; i < vnode_cnt; i++) {
        // Virtual-node identity: "peer_name#0", "peer_name#1", ...
        std::string vnode_key = peer->config().name;
        vnode_key += "#";
        vnode_key += std::to_string(i);
        uint32_t hash = runtime::ds::MurmurHash3(vnode_key);
        ring->nodes.emplace_back(hash, peer);
      }
    }
    std::sort(ring->nodes.begin(), ring->nodes.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
    ring->fingerprint = ComputeFingerprint(peers);

    // Collision handling: simply drop duplicates after sorting.
    // Alternatives considered: rehash with a new seed, or use a multimap.
    // Dedup after sort is the simplest and is fine because MurmurHash3
    // collisions at typical ring sizes are rare and a lost vnode just
    // marginally skews the distribution.
    auto it = std::unique(ring->nodes.begin(), ring->nodes.end(),
                          [](const auto& a, const auto& b) { return a.first == b.first; });
    ring->nodes.erase(it, ring->nodes.end());
    return ring;
  }

  // Parse the hash_on string once at construction so HashKey() can dispatch
  // via a cheap switch instead of repeated string comparisons.
  static HashOn ParseHashOn(std::string_view s) {
    if (s == "uri") return HashOn::kUri;
    if (s == "user_id") return HashOn::kUserId;
    if (s == "session_id") return HashOn::kSessionId;
    return HashOn::kClientIp;
  }

  // Returns the routing key for the configured `hash_on` field.
  // Falls back to client_ip when the configured field is empty so a request
  // missing optional context still has a deterministic mapping.
  const std::string& HashKey(const RequestContext& ctx) const {
    static const std::string empty;
    switch (hash_on_) {
      case HashOn::kUri:
        if (!ctx.uri.empty()) return ctx.uri;
        break;
      case HashOn::kUserId:
        if (!ctx.user_id.empty()) return ctx.user_id;
        break;
      case HashOn::kSessionId:
        if (!ctx.session_id.empty()) return ctx.session_id;
        break;
      case HashOn::kClientIp:
        break;
    }
    return ctx.client_ip.empty() ? empty : ctx.client_ip;
  }

  // Binary-search the ring for the first vnode with hash >= key, wrapping to
  // the head when the key is larger than every hash on the ring.
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
  HashOn hash_on_;
  std::atomic<std::shared_ptr<const HashRing>> ring_;
  std::mutex rebuild_mutex_;
};

// Power-of-two-choices: sample two random available peers and pick the one with
// the lower load. Approximates LeastConnection's quality at O(1) inspection
// cost regardless of fleet size, and avoids the herd effect where every IO
// thread converges on the same "least loaded" peer between probes.
//
// Implementation notes:
//  * Two passes over peers_ (count, then index-walk) — no per-call allocation,
//    matching RoundRobinLB / RandomLB.
//  * Distinct pair via dist(0, n-2) + shift, not (idx2+1)%n: the latter
//    over-samples the peer at (idx1+1)%n whenever the two RNG draws collide.
//  * Load metric is active/weight (fixed-point), so a peer configured with
//    twice the weight is treated as half as loaded at the same active count.
class P2CLB : public LoadBalancer {
public:
  std::shared_ptr<UpstreamPeer> Select(Upstream& upstream, const RequestContext ctx = {}) override {
    const auto& peers = upstream.peers();
    std::size_t avail = 0;
    for (const auto& p : peers) {
      if (p->Available(0)) ++avail;
    }
    if (avail == 0) return nullptr;

    auto pick_at = [&](std::size_t k) -> std::shared_ptr<UpstreamPeer> {
      for (const auto& p : peers) {
        if (!p->Available(0)) continue;
        if (k-- == 0) return p;
      }
      return nullptr;  // unreachable
    };

    if (avail == 1) return pick_at(0);

    thread_local std::mt19937 gen{std::random_device{}()};
    std::uniform_int_distribution<std::size_t> dist(0, avail - 1);
    std::size_t idx1 = dist(gen);
    // Sample idx2 from [0, avail-2] then shift past idx1 — produces a uniform
    // distinct pair without the (idx1+1)%avail bias.
    std::uniform_int_distribution<std::size_t> dist2(0, avail - 2);
    std::size_t idx2 = dist2(gen);
    if (idx2 >= idx1) ++idx2;

    auto peer1 = pick_at(idx1);
    auto peer2 = pick_at(idx2);
    if (!peer1) return peer2;
    if (!peer2) return peer1;

    // Compare active/weight in fixed-point (scale by max weight to stay integer).
    // weight==0 should not appear for an available peer, but guard with max(.,1).
    int w1 = std::max(peer1->weight(), 1);
    int w2 = std::max(peer2->weight(), 1);
    // load1 < load2  <=>  active1 * w2 < active2 * w1
    int64_t load1 = static_cast<int64_t>(peer1->active_request()) * w2;
    int64_t load2 = static_cast<int64_t>(peer2->active_request()) * w1;
    return load1 <= load2 ? peer1 : peer2;
  }
};

namespace detail {

inline std::unique_ptr<LoadBalancer> MakeRoundRobinLB() { return std::make_unique<RoundRobinLB>(); }

inline std::unique_ptr<LoadBalancer> MakeLeastConnectionLB() {
  return std::make_unique<LeastConnectionLB>();
}

inline std::unique_ptr<LoadBalancer> MakeWeightedLeastConnectionLB() {
  return std::make_unique<WeightedLeastConnectionLB>();
}

inline std::unique_ptr<LoadBalancer> MakeRandomLB() { return std::make_unique<RandomLB>(); }

inline std::unique_ptr<LoadBalancer> MakeWeightedRandomLB() {
  return std::make_unique<WeightedRandomLB>();
}

inline std::unique_ptr<LoadBalancer> MakeSmoothWeightedRoundRobinLB() {
  return std::make_unique<SmoothWeightedRoundRobinLB>();
}

inline std::unique_ptr<LoadBalancer> MakeIPHashLB() { return std::make_unique<IPHashLB>(); }

inline std::unique_ptr<LoadBalancer> MakeConsistentHashLB() {
  return std::make_unique<ConsistentHashLB>();
}

inline std::unique_ptr<LoadBalancer> MakeP2CLB() { return std::make_unique<P2CLB>(); }

}  // namespace detail

// String-to-factory entry point used by GatewayServer when registering a route.
// Returns nullptr for an unknown algorithm name so the caller can reject the
// route configuration explicitly instead of silently defaulting.
inline std::unique_ptr<LoadBalancer> CreateLoadBalancer(std::string_view algo) {
  using Creator = std::unique_ptr<LoadBalancer> (*)();

  static constexpr std::array<std::pair<std::string_view, Creator>, 9> table = {{
      {std::string_view{"round_robin"}, detail::MakeRoundRobinLB},
      {std::string_view{"smooth_weighted_round_robin"}, detail::MakeSmoothWeightedRoundRobinLB},
      {std::string_view{"least_connection"}, detail::MakeLeastConnectionLB},
      {std::string_view{"weighted_least_connection"}, detail::MakeWeightedLeastConnectionLB},
      {std::string_view{"random"}, detail::MakeRandomLB},
      {std::string_view{"weighted_random"}, detail::MakeWeightedRandomLB},
      {std::string_view{"ip_hash"}, detail::MakeIPHashLB},
      {std::string_view{"consistent_hash"}, detail::MakeConsistentHashLB},
      {std::string_view{"p2c"}, detail::MakeP2CLB},
  }};

  for (const auto& [name, creator] : table) {
    if (algo == name) {
      return creator();
    }
  }

  return nullptr;
}

}  // namespace runtime::gateway
