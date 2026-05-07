#pragma once

#include "runtime/gateway/upstream_group.h"
#include <atomic>
#include <string>
#include <memory>
#include <mutex>
#include <string_view>
#include <unordered_map>

namespace runtime::gateway {

class LoadBalancer {
public:
  virtual ~LoadBalancer() = default;
  virtual std::shared_ptr<Backend> Select(UpStreamGroup& group) = 0;
};

// -- Round Robin --
class RoundRobinLB : public LoadBalancer {
public:
  std::shared_ptr<Backend> Select(UpStreamGroup& group) override {
    auto backends = group.HealthyBackends();
    if (backends.empty()) return nullptr;
    uint64_t idx = counter_.fetch_add(1, std::memory_order_relaxed);
    return backends[idx % backends.size()];
  }
private:
  std::atomic<uint64_t> counter_{0U};
};

// -- Weighted Round Robin (Smooth) in Nginx --
class WeightedRoundRobinLB : public LoadBalancer {
public:
  std::shared_ptr<Backend> Select(UpStreamGroup& group) override {
    auto backends = group.HealthyBackends();
    if (backends.empty()) return nullptr;

    std::lock_guard lk{mutex_};
    int total = 0;
    for (const auto& backend : backends) total += backend->config_.weight;

    std::shared_ptr<Backend> best;
    int best_weight = INT_MIN;
    for (const auto& backend : backends) {
      auto& cur = current_weights_[backend->config_.id];
      cur += backend->config_.weight;
      if (cur > best_weight) { best_weight = cur; best = backend; }
    }

    if (best) current_weights_[best->config_.id] -= total;
    return best;
  };
private:
  std::mutex mutex_;
  std::unordered_map<std::string, int> current_weights_;
};

// -- Least Connections ---
class LeastConnectionLB : public LoadBalancer {
public:
  std::shared_ptr<Backend> Select(UpStreamGroup& group) override {
    auto backends = group.HealthyBackends();
    if (backends.empty()) return nullptr;

    std::shared_ptr<Backend> best;
    int min_req = INT_MAX;

    for (auto& backend : backends) {
        int cur = backend->state_.active_requests.load(std::memory_order_relaxed);
        if (cur < min_req) { min_req = cur; best = backend; }
    }
    return best;
  }
};

inline std::unique_ptr<LoadBalancer> MakeLoadBalancer(std::string_view algo) {
    if (algo == "weighted_round_robin") return std::make_unique<WeightedRoundRobinLB>();
    if (algo == "least_connections") return std::make_unique<LeastConnectionLB>();
    return std::make_unique<RoundRobinLB>(); // default Round-Robin
}
}  // namespace runtime::gateway