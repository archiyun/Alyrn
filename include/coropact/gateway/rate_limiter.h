// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "coropact/time/clock.h"
#include "coropact/utils/macros.h"

namespace coropact::gateway {

namespace {

constexpr double kGlobalRate = 1000.0;
constexpr double kGlobalBurst = 2000.0;

constexpr double kPerIPRate = 10.0;
constexpr double kPerIPBurst = 20.0;

constexpr std::size_t kPerIPMaxBuckets = 65536;

}  // namespace

// Rate limiter settings. Rates are expressed as tokens per second,
// and burst values are the maximum number of tokens a bucket may accumulate.
struct RateLimiterConfig {
  bool global_enabled{false};
  double global_rate{kGlobalRate};
  double global_burst{kGlobalBurst};

  // Per-client-IP limiter. Each observed IP address gets an independent bucket.
  bool per_ip_enabled{false};
  double per_ip_rate{kPerIPRate};
  double per_ip_burst{kPerIPBurst};
  // Cap on live per-IP buckets. Once the map reaches this size, idle buckets
  // (already refilled to full — loss-less to drop) are evicted. 0 disables the
  // cap (unbounded growth, the original behavior).
  std::size_t per_ip_max_buckets{kPerIPMaxBuckets};
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
  COROPACT_DELETE_COPY_MOVE(TokenBucket);

  explicit TokenBucket(double rate, double burst)
      : rate_(rate), burst_(burst), scaled_tokens_(static_cast<uint64_t>(burst * kScale)) {}

  [[nodiscard]]
  bool TryConsume() noexcept {
    std::lock_guard lock{mutex_};

    std::uint64_t now = time::SteadyNowMs();
    std::uint64_t current = scaled_tokens_.load(std::memory_order_relaxed);
    std::uint64_t last = last_refill_ms_.load(std::memory_order_relaxed);

    std::uint64_t elapsed = now > last ? now - last : 0;
    auto refill = static_cast<uint64_t>(elapsed * rate_ * kScale / 1000.0);
    auto burst_scaled = static_cast<uint64_t>(burst_ * kScale);

    uint64_t new_tokens = std::min(current + refill, burst_scaled);

    last_refill_ms_.store(now, std::memory_order_relaxed);

    if (new_tokens < kScale) {
      scaled_tokens_.store(new_tokens, std::memory_order_relaxed);
      return false;
    }
    scaled_tokens_.store(new_tokens - kScale, std::memory_order_relaxed);
    return true;
  }

  [[nodiscard]]
  double rate() const noexcept {
    return rate_;
  }

  [[nodiscard]]
  double burst() const noexcept {
    return burst_;
  }

  // True when enough time has elapsed since the last refill that the bucket is
  // guaranteed back at burst_. A full bucket is indistinguishable from a
  // freshly-created one, so the owner may evict it without changing any future
  // TryConsume() outcome — this is what makes idle per-IP bucket eviction safe.
  [[nodiscard]]
  bool RefilledToFull(uint64_t now) const noexcept {
    if (rate_ <= 0.0) {
      return false;  // never refills; evicting would reset it
    }

    uint64_t last = last_refill_ms_.load(std::memory_order_relaxed);
    if (now <= last) {
      return false;
    }

    const double full_ms = (burst_ / rate_) * 1000.0;
    return static_cast<double>(now - last) >= full_ms;
  }

private:
  static constexpr uint64_t kScale = 1'000'000;  // Fixed-point units per token.

  mutable std::mutex mutex_;
  // Current token balance, scaled by kScale.
  std::atomic<uint64_t> last_refill_ms_{time::SteadyNowMs()};
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
      : config_(config), global_bucket_(config_.global_rate, config_.global_burst) {}

  [[nodiscard]]
  bool AllowGlobal() noexcept {
    if (!config_.global_enabled) {
      return true;
    }

    return global_bucket_.TryConsume();
  }

  [[nodiscard]]
  bool AllowPerIP(std::string_view client_ip) noexcept {
    if (!config_.per_ip_enabled) {
      return true;
    }

    std::string key(client_ip);
    std::lock_guard lock{ip_mutex_};

    auto existing = ip_buckets_.find(key);
    if (existing != ip_buckets_.end()) {
      return existing->second->TryConsume();
    }

    if (config_.per_ip_max_buckets != 0) {
      if (ip_buckets_.size() >= config_.per_ip_max_buckets) {
        EvictFullBucketsLocked();
      }
      // Hard admission bound: never evict an active bucket (which would reset
      // its limit), and never grow beyond the configured memory cap. Unknown
      // identities are rate-limited until an idle bucket becomes evictable.
      if (ip_buckets_.size() >= config_.per_ip_max_buckets) {
        return false;
      }
    }

    auto token_bucket =
        ip_buckets_
            .try_emplace(std::move(key),
                         std::make_shared<TokenBucket>(config_.per_ip_rate, config_.per_ip_burst))
            .first;
    return token_bucket->second->TryConsume();
  }

  // Number of live per-IP buckets. Exposed for diagnostics and tests.
  std::size_t per_ip_bucket_count() const noexcept {
    std::lock_guard lock{ip_mutex_};
    return ip_buckets_.size();
  }

private:
  // Drop buckets that have refilled to full. Loss-less: a re-created bucket also
  // starts full, so eviction changes no future outcome. Throttled to at most one
  // O(n) sweep per kEvictIntervalMs, so a workload that pins the map at the cap
  // with genuinely active buckets can't sweep on every call. Caller holds
  // ip_mutex_.
  void EvictFullBucketsLocked() {
    const uint64_t now = time::SteadyNowMs();
    if (now - last_evict_ms_ < kEvictIntervalMs) {
      return;
    }

    last_evict_ms_ = now;
    for (auto iterator = ip_buckets_.begin(); iterator != ip_buckets_.end();) {
      if (iterator->second->RefilledToFull(now)) {
        iterator = ip_buckets_.erase(iterator);
      } else {
        ++iterator;
      }
    }
  }

  static constexpr uint64_t kEvictIntervalMs = 1000;

  RateLimiterConfig config_;
  TokenBucket global_bucket_;

  mutable std::mutex ip_mutex_;
  std::unordered_map<std::string, std::shared_ptr<TokenBucket>> ip_buckets_;
  uint64_t last_evict_ms_{0};  // guarded by ip_mutex_
};

}  // namespace coropact::gateway
