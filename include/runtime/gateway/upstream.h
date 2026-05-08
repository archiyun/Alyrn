#pragma once

#include "runtime/gateway/health_check_config.h"
#include "runtime/gateway/upstream_peer.h"

#include <memory>
#include <string>
#include <vector>

namespace runtime::gateway {

enum class LoadBalancePolicy {
  RoundRobin,
  WeightedRoundRobin,
  LeastConnection,
  Random,
  WeightedRandom,
};

struct UpstreamConfig {
  std::string name;
  LoadBalancePolicy policy{LoadBalancePolicy::RoundRobin};
  HealthCheckConfig health_check{};
};

class Upstream {
public:
  explicit Upstream(UpstreamConfig config) : config_(std::move(config)) {}

  void AddPeer(std::shared_ptr<UpstreamPeer> peer) {
    peers_.push_back(std::move(peer));
  }

  const std::string& Name() const { return config_.name; }
  LoadBalancePolicy Policy() const { return config_.policy; }

  std::vector<std::shared_ptr<UpstreamPeer>> Peers() const { return peers_; }

  std::vector<std::shared_ptr<UpstreamPeer>> Available(uint64_t now_ms = 0) const {
    std::vector<std::shared_ptr<UpstreamPeer>> res;
    for (const auto& peer : peers_) {
      if (peer->Available(now_ms)) res.push_back(peer);
    }
    return res;
  }

private:
  UpstreamConfig config_;
  std::vector<std::shared_ptr<UpstreamPeer>> peers_;
};

}  // namespace runtime::gateway
