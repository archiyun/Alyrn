// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

namespace runtime::gateway {

// Rate limiter settings. Rates are expressed as tokens per second,
// and burst values are the maximum number of tokens a bucket may accumulate.
struct RateLimiterConfig {
  bool global_enabled{false};
  double global_rate{1000.0};
  double global_burst{2000.0};

  // Per-client-IP limiter. Each observed IP address gets an independent bucket.
  bool per_ip_enabled{false};
  double per_ip_rate{10.0};
  double per_ip_burst{20.0};
};

// Thread-safe token bucket.
//
// Tokens are refilled lazily when TryConsume() is called, so no background
// timer is required. Each accepted request consumes one token.
// If fewer than one token is available, the request is rejected.
// Idle time lets tokens build up to burst_, which allows short traffic
// spikes without raising the steady request rate.
//
// Token counts are stored as fixed-point integers. kScale represents one whole
// token and preserves fractional refill progress between calls.
class TokenBucket {
public:
  explicit TokenBucket(double rate, double burst)
    : rate_(rate), burst_(burst),
      scaled_tokens_(static_cast<uint64_t>(burst * kScale)) {}

  bool TryConsume() noexcept {
    std::lock_guard lk{mutex_};

    uint64_t now = NowMs();
    uint64_t current = scaled_tokens_.load(std::memory_order_relaxed);
    uint64_t last = last_refill_ms_.load(std::memory_order_relaxed);

    uint64_t elasped = now > last ? now - last : 0;
    uint64_t refill = static_cast<uint64_t>(elasped * rate_ * kScale / 1000.0);
    uint64_t burst_scaled = static_cast<uint64_t>(burst_ * kScale);

    uint64_t new_tokens = current + refill;
    if (new_tokens > burst_scaled) new_tokens = burst_scaled;

    last_refill_ms_.store(now, std::memory_order_relaxed);

    if (new_tokens < kScale) {
      scaled_tokens_.store(new_tokens, std::memory_order_relaxed);
      return false;
    }
    scaled_tokens_.store(new_tokens - kScale, std::memory_order_relaxed);
    return true;
  }

  double rate() const noexcept { return rate_; }
  double burst() const noexcept { return burst_; }

private:
  static constexpr uint64_t kScale = 1'000'000;  // Fixed-point units per token.
  static uint64_t NowMs() noexcept {
    return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch())
        .count()
    );
  }

  mutable std::mutex mutex_;
  // Current token balance, scaled by kScale.
  std::atomic<uint64_t> last_refill_ms_{NowMs()};
  std::atomic<uint64_t> scaled_tokens_;
  double rate_;   // Refill rate in tokens per second.
  double burst_;  // Maximum bucket capacity in tokens.
};


// Gateway-wide rate limiter.
//
// The global bucket is checked once for every request when enabled.
// Per-IP buckets are created lazily and are keyed by the peer address observed
// by the gateway. Map access is serialized, and each TokenBucket serializes
// its own refill/consume sequence.
//
// Both locks protect short critical sections and are intended to avoid blocking
// IO threads for meaningful periods of time.
class RateLimiter {
public:
  explicit RateLimiter(RateLimiterConfig config) noexcept
    : config_(config),
      global_bucket_(config_.global_rate, config_.global_burst) {}

  bool AllowGlobal() {
    if (!config_.global_enabled) return true;
    return global_bucket_.TryConsume();
  }

  bool AllowPerIP(std::string_view client_ip) {
    if (!config_.per_ip_enabled) return true;

    std::string key(client_ip);
    std::lock_guard lk{ip_mutex_};

    auto [it, _] = ip_buckets_.try_emplace(
        key, std::make_shared<TokenBucket>(config_.per_ip_rate, config_.per_ip_burst));
    return it->second->TryConsume();
  }
private:
  RateLimiterConfig config_;
  TokenBucket global_bucket_;

  std::mutex ip_mutex_;
  std::unordered_map<std::string, std::shared_ptr<TokenBucket>> ip_buckets_;
};

}  // namespace runtime::gateway
