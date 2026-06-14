// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Upstream: a named group of backend peers with (optionally) one circuit
// breaker. Built once at startup and treated as read-only thereafter; per-peer
// mutable state lives inside each UpstreamPeer. The load-balancing strategy is
// not stored here — it is owned per-route (Route::lb, built from the algo string
// passed to GatewayServer::AddProxyRoute).
#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "runtime/gateway/circuit_breaker.h"
#include "runtime/gateway/health_check_config.h"
#include "runtime/gateway/upstream_peer.h"

namespace runtime::gateway {

// Static configuration block for an Upstream. Immutable after construction.
struct UpstreamConfig {
  std::string name;
  HealthCheckConfig health_check{};
  CircuitBreakerConfig circuit_breaker{};
  bool circuit_breaker_enabled{false};

  // Bulkhead and deadline defaults. A value of 0 keeps the legacy unbounded
  // behavior; the default is deliberately bounded.
  std::size_t max_concurrent_requests{1024};
  std::chrono::milliseconds request_timeout{5000};
};

// A named group of upstream peers with an optional circuit breaker. Lifecycle:
//   1. construct
//   2. AddPeer() repeatedly during startup
//   3. handed to UpstreamRegistry; from this point treated as read-only
//      (peers_ and config_ never mutate again).
// Concurrency: peers_ is read-only at runtime, so peers() is lock-free.
// Per-peer dynamic state is owned by each UpstreamPeer via atomics.
class Upstream {
public:
  explicit Upstream(UpstreamConfig config) : config_(std::move(config)) {}

  // Append a peer. Call only during startup, before any IO thread observes
  // this Upstream — there is no synchronization on peers_.
  void AddPeer(std::shared_ptr<UpstreamPeer> peer) {
    peers_.push_back(std::move(peer));
  }

  const std::string& name() const { return config_.name; }
  const UpstreamConfig& config() const { return config_; }
  // Returns the full peer list; load balancers filter via UpstreamPeer::AvailableAt()
  // inline on each Select to avoid per-call allocation of a snapshot vector.
  const std::vector<std::shared_ptr<UpstreamPeer>>& peers() const { return peers_; }

  // Circuit breaker is attached lazily by GatewayServer::AddProxyRoute when
  // circuit_breaker_enabled is set on a Route. Stored as shared_ptr so
  // GatewayServer and proxy code can share the same instance across requests.
  void set_circuit_breaker(std::shared_ptr<CircuitBreaker> cb) { cb_ = std::move(cb); }
  std::shared_ptr<CircuitBreaker> circuit_breaker() const { return cb_; }

  bool TryAcquireRequestSlot() noexcept {
    const std::size_t limit = config_.max_concurrent_requests;
    if (limit == 0) {
      active_requests_.fetch_add(1, std::memory_order_acq_rel);
      return true;
    }

    std::size_t current =
        active_requests_.load(std::memory_order_relaxed);
    while (current < limit) {
      if (active_requests_.compare_exchange_weak(
              current, current + 1,
              std::memory_order_acq_rel, std::memory_order_relaxed)) {
        return true;
      }
    }
    return false;
  }

  void ReleaseRequestSlot() noexcept {
    active_requests_.fetch_sub(1, std::memory_order_acq_rel);
  }

  std::size_t active_requests() const noexcept {
    return active_requests_.load(std::memory_order_relaxed);
  }

private:
  UpstreamConfig config_;
  std::vector<std::shared_ptr<UpstreamPeer>> peers_;
  std::shared_ptr<CircuitBreaker> cb_;
  std::atomic<std::size_t> active_requests_{0};
};

}  // namespace runtime::gateway
