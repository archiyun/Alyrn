#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>

namespace runtime::gateway {

struct UpstreamPeerConfig {
  std::string name;      // ip:port => eg. "127.0.0.1:9001"
  std::string host;
  uint16_t port{0};

  int weight{1};
  int effective_weight{1};
  int max_fails{3};
  std::chrono::milliseconds fail_timeout{10000};
};

struct UpstreamPeerState {
  std::atomic<bool> down{false};
  std::atomic<int> active{0};
  std::atomic<uint64_t> requests{0};
  std::atomic<uint64_t> fails{0};
  std::atomic<uint64_t> checked_ms{0};
  std::atomic<uint64_t> accessed_ms{0};
};

class UpstreamPeer {
public:
  explicit UpstreamPeer(UpstreamPeerConfig config) : config_(std::move(config)) {}

  const UpstreamPeerConfig& Config() const { return config_; }
  UpstreamPeerState& State() { return state_; }

  bool Available(uint64_t now_ms) const {
    if (state_.down.load(std::memory_order_relaxed)) return false;
    const int fails = static_cast<int>(state_.fails.load(std::memory_order_relaxed));
    if (fails < config_.max_fails) return true;
    const uint64_t checked = state_.checked_ms.load(std::memory_order_relaxed);
    return (now_ms - checked) >= static_cast<uint64_t>(config_.fail_timeout.count());
  }

  int Weight() const {
    return config_.weight;
  }

  void OnRequestStart() {
    state_.active.fetch_add(1, std::memory_order_relaxed);
    state_.requests.fetch_add(1, std::memory_order_relaxed);
  }

  void OnRequestDone() {
    state_.active.fetch_sub(1, std::memory_order_relaxed);
  }

  void OnFailure(uint64_t now_ms) {
    state_.fails.fetch_add(1, std::memory_order_relaxed);
    state_.checked_ms.store(now_ms, std::memory_order_relaxed);
  }

  void OnSuccess() {
    state_.fails.store(0, std::memory_order_relaxed);
    state_.down.store(false, std::memory_order_relaxed);
  }

private:
  UpstreamPeerConfig config_;
  UpstreamPeerState state_;
};


}  // namespace runtime::gateway
