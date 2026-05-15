#pragma once

#include <chrono>
#include <atomic>
#include <cstdint>

namespace runtime::gateway {

// 编写一个熔断器， 待测试。
enum class CircuitBreakerState : uint8_t {
  kClosed,
  kOpen,
  kHalfOpen,
};

struct CircuitBreakerConfig {
  int failure_threshold{5};
  int success_threshold{2};
  std::chrono::milliseconds open_timeout{10000};
  int half_open_max_requests{1};
};

// Per-upstream 熔断器，全 atomic + CAS，零互斥锁。
//
// 三层防护各司其职：
//   Layer 1 — CircuitBreaker    per-upstream  "整个服务挂了就别试了"
//   Layer 2 — UpstreamPeer 健康  per-peer      "这个节点坏了换一个"
//   Layer 3 — UpstreamRequest 重试 per-request "偶发抖动换节点重试"
//
// 状态机：
//   CLOSED ──[连续失败 >= failure_threshold]──> OPEN
//   OPEN   ──[open_timeout 到期]──────────────> HALF_OPEN
//   HALF_OPEN ──[连续成功 >= success_threshold]──> CLOSED
//   HALF_OPEN ──[任意失败]────────────────────> OPEN
//
// CLOSED 下连续失败达到阈值才 OPEN，连续成功达到 success_threshold 才重置计数器，
// 避免间歇性故障场景下单次成功永久抑制熔断。
//
// AllowRequest() 是热路径（每次代理请求都调）：
//   CLOSED    → 一次 acquire-load
//   OPEN      → 一次 acquire-load + 一次 relaxed-load 比较超时
//   HALF_OPEN → 一次 fetch_add，限量放行 
class CircuitBreaker {
public:
  explicit CircuitBreaker(CircuitBreakerConfig cfg) noexcept
    : cfg_(cfg) {}
  CircuitBreakerState State() const noexcept {
    return static_cast<CircuitBreakerState>(
      state_.load(std::memory_order_acquire)
    );
  }

  uint64_t FailureCount() const noexcept {
    return failure_count_.load(std::memory_order_relaxed);
  }

  uint64_t TransitionCount() const noexcept {
    return transition_count_.load(std::memory_order_relaxed);
  }
  // 热路径， 无锁。 返回 true 表示放行
  bool AllowRequest() noexcept {
    int s = state_.load(std::memory_order_acquire);

    if (s == kClosedInt) {
      return true;
    }
    
    if (s == kOpenInt) {
      uint64_t now = NowMs();
      uint64_t entered = open_entered_ms_.load(std::memory_order_relaxed);
      if (now - entered < static_cast<uint64_t>(cfg_.open_timeout.count())) {
        return false; // 超时未到， 快速拒绝
      }
      // 超时到期， CAS OPEN -> HALF_OPEN
      if (state_.compare_exchange_strong(s, kHalfOpenInt,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        half_open_requests_.store(0, std::memory_order_relaxed);
        success_count_.store(0, std::memory_order_relaxed);
        transition_count_.fetch_add(1, std::memory_order_relaxed);
        s = kHalfOpenInt;
      }
      // CAS 失败: s 已被 compare_exchange_strong 更新为实际状态， fall through
    }

    if (s == kHalfOpenInt) {
      uint64_t n = half_open_requests_.fetch_add(1, std::memory_order_relaxed);
      return n < static_cast<uint64_t>(cfg_.half_open_max_requests);
    }
    return false;
  }

  // 请求成功时调用
  // CLOSED: 连续成功达标才重置失败计数 (避免间歇故障下单次成功永久抑制熔断)
  // HALF_OPEN: 连续成功达标 -> CLOSED
  void OnSuccess() noexcept {
    int s = state_.load(std::memory_order_acquire);

    if (s == kClosedInt) {
      uint64_t n = success_count_.fetch_add(1, std::memory_order_relaxed) + 1;
      if (n >= static_cast<uint64_t>(cfg_.success_threshold)) {
        failure_count_.store(0, std::memory_order_relaxed);
        success_count_.store(0, std::memory_order_relaxed);
      }
      return;
    }

    if (s == kHalfOpenInt) {
      uint64_t n = success_count_.fetch_add(1, std::memory_order_relaxed) + 1;
      if (n >= static_cast<uint64_t>(cfg_.success_threshold)) {
        int expected = kHalfOpenInt;
        if (state_.compare_exchange_strong(expected, kClosedInt,
                std::memory_order_acq_rel, std::memory_order_acquire)) {
          failure_count_.store(0, std::memory_order_relaxed);
          transition_count_.fetch_add(1, std::memory_order_relaxed);
        }
      }
    }
  }

  void OnFailure() noexcept {
    int s = state_.load(std::memory_order_acquire);

    if (s == kHalfOpenInt) {
      int expected = kHalfOpenInt;
      if (state_.compare_exchange_strong(expected, kOpenInt,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        open_entered_ms_.store(NowMs(), std::memory_order_release);
        success_count_.store(0, std::memory_order_relaxed);
        transition_count_.fetch_add(1, std::memory_order_relaxed);
      }
      return;
    }

    if (s == kClosedInt) {
      uint64_t n = failure_count_.fetch_add(1, std::memory_order_relaxed) + 1;
      if (n >= static_cast<uint64_t>(cfg_.failure_threshold)) {
        int expected = kClosedInt;
        if (state_.compare_exchange_strong(expected, kOpenInt,
                std::memory_order_acq_rel, std::memory_order_acquire)) {
          open_entered_ms_.store(NowMs(), std::memory_order_release);
          transition_count_.fetch_add(1, std::memory_order_relaxed);
        }
      }
    }
  }
private:
  static uint64_t NowMs() noexcept {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
  }

  static constexpr int kClosedInt   = static_cast<int>(CircuitBreakerState::kClosed);
  static constexpr int kOpenInt     = static_cast<int>(CircuitBreakerState::kOpen);
  static constexpr int kHalfOpenInt = static_cast<int>(CircuitBreakerState::kHalfOpen);

  CircuitBreakerConfig cfg_;

  std::atomic<int>      state_{kClosedInt};
  std::atomic<uint64_t> failure_count_{0};
  std::atomic<uint64_t> success_count_{0};
  std::atomic<uint64_t> half_open_requests_{0};
  std::atomic<uint64_t> open_entered_ms_{0};
  std::atomic<uint64_t> transition_count_{0};
};

}  // namespace runtime::gateway
