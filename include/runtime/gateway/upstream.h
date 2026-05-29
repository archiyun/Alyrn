// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Upstream: a named group of backend peers sharing one load-balance policy
// and (optionally) one circuit breaker. Built once at startup and treated as
// read-only thereafter; per-peer mutable state lives inside each UpstreamPeer.
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "runtime/gateway/circuit_breaker.h"
#include "runtime/gateway/health_check_config.h"
#include "runtime/gateway/upstream_peer.h"

namespace runtime::gateway {

// Selection algorithm used by LoadBalancer::Select to pick a peer.
enum class LoadBalancePolicy {
  RoundRobin,
  WeightedRoundRobin,
  LeastConnection,
  Random,
  WeightedRandom,
  IPHash,
  ConsistentHash,
  P2C,
};

// Static configuration block for an Upstream. Immutable after construction.
struct UpstreamConfig {
  std::string name;
  LoadBalancePolicy policy{LoadBalancePolicy::RoundRobin};
  HealthCheckConfig health_check{};
  CircuitBreakerConfig circuit_breaker{};
  bool circuit_breaker_enabled{false};
};

// A named group of upstream peers sharing one LB policy and an optional
// circuit breaker. Lifecycle:
//   1. construct
//   2. AddPeer() repeatedly during startup
//   3. handed to UpstreamRegistry; from this point treated as read-only
//      (peers_ and config_ never mutate again).
// Concurrency: peers_ is read-only at runtime, so Peers() is lock-free.
// Per-peer dynamic state is owned by each UpstreamPeer via atomics.
class Upstream {
public:
  explicit Upstream(UpstreamConfig config) : config_(std::move(config)) {}

  // Append a peer. Call only during startup, before any IO thread observes
  // this Upstream — there is no synchronization on peers_.
  void AddPeer(std::shared_ptr<UpstreamPeer> peer) {
    peers_.push_back(std::move(peer));
  }

  const std::string& Name() const { return config_.name; }
  LoadBalancePolicy Policy() const { return config_.policy; }
  const UpstreamConfig& Config() const { return config_; }
  // Returns the full peer list; load balancers filter via UpstreamPeer::Available()
  // inline on each Select to avoid per-call allocation of a snapshot vector.
  const std::vector<std::shared_ptr<UpstreamPeer>>& Peers() const { return peers_; }

  // Circuit breaker is attached lazily by GatewayServer::AddProxyRoute when
  // circuit_breaker_enabled is set on a Route. Stored as shared_ptr so
  // GatewayServer and proxy code can share the same instance across requests.
  void SetCircuitBreaker(std::shared_ptr<CircuitBreaker> cb) { cb_ = std::move(cb); }
  std::shared_ptr<CircuitBreaker> GetCircuitBreaker() const { return cb_; }

private:
  UpstreamConfig config_;
  std::vector<std::shared_ptr<UpstreamPeer>> peers_;
  std::shared_ptr<CircuitBreaker> cb_;
};

}  // namespace runtime::gateway
