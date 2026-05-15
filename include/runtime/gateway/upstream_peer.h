// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
//
// UpstreamPeer: a single backend endpoint of an Upstream group.
//
// Holds (1) static identity/config (host:port, weight, fail policy) and
// (2) atomic runtime state (active count, fail count, dynamic effective_weight).
// Concurrency model: any IO thread may read/write the runtime state via the
// atomic fields; the config block is frozen after construction.
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>

namespace runtime::gateway {

// Static configuration for an upstream peer. Built at startup; read-only after.
struct UpstreamPeerConfig {
  std::string name;  // Unique identifier, format "ip:port", e.g. "127.0.0.1:9001"
  std::string host;
  uint16_t port{0};

  int weight{1};  // Configured target weight (upper bound for effective_weight).
  int max_fails{3};
  std::chrono::milliseconds fail_timeout{10000};
};

// Runtime state for an upstream peer. All fields are atomic; safe for concurrent access
// across IO threads. Relaxed ordering is intentional — load balancing tolerates slightly
// stale counts.
struct UpstreamPeerState {
  std::atomic<bool> down{false};        // Set by active health checker only.
  std::atomic<int> active{0};           // In-flight request count; used by LeastConn.
  std::atomic<uint64_t> requests{0};
  std::atomic<uint64_t> fails{0};
  std::atomic<uint64_t> checked_ms{0};  // Timestamp of last failure; used for fail_timeout window.
  std::atomic<uint64_t> accessed_ms{0};
  // Dynamic weight for SWRR. Starts at config.weight; decremented on failure (min 0),
  // incremented on success (max config.weight). Drives gradual demotion of flaky peers
  // without crossing the binary down/up boundary owned by HealthChecker.
  std::atomic<int> effective_weight{0};
};

// Represents a single upstream peer: static config + runtime state.
//
// Passive health check logic is split across Available(), OnFailure(), and OnSuccess().
// Active health check (HealthChecker) writes state_.down directly and is orthogonal to passive.
class UpstreamPeer {
public:
  explicit UpstreamPeer(UpstreamPeerConfig config) : config_(std::move(config)) {
    // Start fully eligible; OnFailure/OnSuccess move it within [0, config_.weight].
    state_.effective_weight.store(config_.weight, std::memory_order_relaxed);
  }

  const UpstreamPeerConfig& Config() const { return config_; }
  UpstreamPeerState& State() { return state_; }

  // Returns false if: (1) marked down by health checker, or (2) fail count has reached
  // max_fails and the fail_timeout cooldown has not yet elapsed.
  bool Available(uint64_t now_ms) const {
    if (state_.down.load(std::memory_order_relaxed)) return false;
    const int fails = static_cast<int>(state_.fails.load(std::memory_order_relaxed));
    if (fails < config_.max_fails) return true;
    const uint64_t checked = state_.checked_ms.load(std::memory_order_relaxed);
    return (now_ms - checked) >= static_cast<uint64_t>(config_.fail_timeout.count());
  }

  int Weight() const { return config_.weight; }
  int EffectiveWeight() const {
    return state_.effective_weight.load(std::memory_order_relaxed);
  }

  void OnRequestStart() {
    state_.active.fetch_add(1, std::memory_order_relaxed);
    state_.requests.fetch_add(1, std::memory_order_relaxed);
  }

  void OnRequestDone() {
    state_.active.fetch_sub(1, std::memory_order_relaxed);
  }

  // Records a failure: bumps fails/checked_ms (drives Available's fail_timeout window)
  // and decrements effective_weight toward 0 (drives WRR gradual demotion).
  // CAS loop keeps the value in [0, config_.weight] under concurrent updates.
  void OnFailure(uint64_t now_ms) {
    state_.fails.fetch_add(1, std::memory_order_relaxed);
    state_.checked_ms.store(now_ms, std::memory_order_relaxed);
    int cur = state_.effective_weight.load(std::memory_order_relaxed);
    while (cur > 0 &&
           !state_.effective_weight.compare_exchange_weak(
               cur, cur - 1, std::memory_order_relaxed)) {
      // CAS reloads cur on failure; loop until success or floor (0) is hit.
    }
  }

  // Records a success: clears passive failure counters AND bumps effective_weight back
  // toward config.weight. HealthChecker is the only path that can lift a peer out of
  // effective_weight=0 once WRR has fully steered traffic away.
  void OnSuccess() {
    state_.fails.store(0, std::memory_order_relaxed);
    state_.down.store(false, std::memory_order_relaxed);
    int cur = state_.effective_weight.load(std::memory_order_relaxed);
    while (cur < config_.weight &&
           !state_.effective_weight.compare_exchange_weak(
               cur, cur + 1, std::memory_order_relaxed)) {
      // CAS reloads cur on failure; loop until success or ceiling (weight) is hit.
    }
  }

private:
  UpstreamPeerConfig config_;
  UpstreamPeerState state_;
};

}  // namespace runtime::gateway
