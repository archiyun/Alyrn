#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

namespace runtime::gateway {

// 单位: 秒
struct RateLimiterConfig {
  bool global_enabled{false};
  double global_rate{1000.0};
  double global_burst{2000.0};

  // per-ip 
  bool per_ip_enabled{false};
  double per_ip_rate{10.0};
  double per_ip_burst{20.0};
};

// 令牌桶, 全 atomic , thread-safe
// 令牌桶工作方式:
// - 以 rate 的速率往桶里加令牌
// - 桶的容量上限是 burst
// - 每个请求消耗是 1 个令牌
// - 桶空了 -> reject
// - 空闲令牌累积 -> 可处理突发
// uint64_t 
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

  double Rate() const noexcept { return rate_; }
  double Burst() const noexcept { return burst_; }

private:
  static constexpr uint64_t kScale = 1'000'000;  // 精度换算系数
  static uint64_t NowMs() noexcept {
    return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch())
        .count()
    );
  }

  mutable std::mutex mutex_;
  std::atomic<uint64_t> scaled_tokens_; // 当前令牌数 , 实际令牌数 等价于 -> scaled_tokens / kScale
  std::atomic<uint64_t> last_refill_ms_{NowMs()};
  double rate_; // 每 s 填充速率
  double burst_;  // 桶容量上限
};


// 全局限流器， 挂在 GatewayServer 上。
//
// 全局桶: 每个请求过一次， 用 mutex 保护 (临界区 ~20 CPU 周期)。
// Per-IP 桶: 懒创建， 用 mutex 保护 map access
// 
// 两层锁都极短， 不会阻塞 IO 线程
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

    // try_emplace constructs TokenBucket in-place (no move/copy needed),
    // which is required because std::mutex and std::atomic are non-movable.
    auto [it, _] = ip_buckets_.try_emplace(
        key, config_.per_ip_rate, config_.per_ip_burst);
    return it->second.TryConsume();
  }
private:
  RateLimiterConfig config_;
  TokenBucket global_bucket_;

  std::mutex ip_mutex_;
  std::unordered_map<std::string, TokenBucket> ip_buckets_;
};
}  // namespace runtime::gateway