#pragma once

#include "runtime/gateway/upstream.h"

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

namespace runtime::gateway {

class LoadBalancer {
public:
  virtual ~LoadBalancer() = default;
  virtual std::shared_ptr<UpstreamPeer> Select(Upstream& upstream) = 0;
};

// -- Round Robin --
class RoundRobinLB : public LoadBalancer {
public:
  std::shared_ptr<UpstreamPeer> Select(Upstream& upstream) override {
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
  std::shared_ptr<UpstreamPeer> Select(Upstream& upstream) override {
    auto peers = upstream.Available();
    if (peers.empty()) return nullptr;

    std::lock_guard<std::mutex> lk(mutex_);

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
  std::shared_ptr<UpstreamPeer> Select(Upstream& upstream) override {
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
  std::shared_ptr<UpstreamPeer> Select(Upstream& upstream) override {
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
  std::shared_ptr<UpstreamPeer> Select(Upstream& upstream) override {
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

} // namespace detail

inline std::unique_ptr<LoadBalancer> CreateLoadBalancer(std::string_view algo) {
  using Creator = std::unique_ptr<LoadBalancer> (*)();

  static constexpr std::array<std::pair<std::string_view, Creator>, 5> table = {{
      {std::string_view{"round_robin"}, detail::MakeRoundRobinLB},
      {std::string_view{"least_connection"}, detail::MakeLeastConnectionLB},
      {std::string_view{"random"}, detail::MakeRandomLB},
      {std::string_view{"weighted_random"}, detail::MakeWeightedRandomLB},
      {std::string_view{"weighted_round_robin"}, detail::MakeWeightedRoundRobinLB},
  }};

  for (const auto& [name, creator] : table) {
    if (algo == name) {
      return creator();
    }
  }

  return nullptr;
}

} // namespace runtime::gateway