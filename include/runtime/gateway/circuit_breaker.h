// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <chrono>
#include <atomic>
#include <cstdint>

namespace runtime::gateway {

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

// Per-upstream circuit breaker. Lock-free — all state transitions use
// std::atomic + CAS. Safe for concurrent access from multiple EventLoop
// threads with no mutexes on the hot path.
//
// Sits at Layer 1 of the three-tier fault protection stack:
//   Layer 1  CircuitBreaker     per-upstream   fast-fail when the whole backend is down
//   Layer 2  UpstreamPeer       per-peer        skip individual unhealthy nodes
//   Layer 3  UpstreamRequest    per-request     retry on transient connection errors
//
// State machine:
//   CLOSED   → [consecutive failures >= failure_threshold]  → OPEN
//   OPEN     → [open_timeout elapsed]                       → HALF_OPEN
//   HALF_OPEN → [consecutive successes >= success_threshold] → CLOSED
//   HALF_OPEN → [any failure]                               → OPEN
//
// In CLOSED state, a single success does NOT reset the failure counter.
// The counter resets only after consecutive successes reach success_threshold,
// which prevents intermittent faults from permanently suppressing the trip.
//
// AllowRequest() is on the critical path (called once per proxied request):
//   CLOSED    — one acquire-load
//   OPEN      — one acquire-load + one relaxed-load for timeout comparison
//   HALF_OPEN — one fetch_add to claim a probe slot
class CircuitBreaker {
public:
  explicit CircuitBreaker(CircuitBreakerConfig cfg) noexcept
    : cfg_(cfg) {}

  // Returns the current state. Intended for metrics export only;
  // do not use the result to make routing decisions — call AllowRequest().
  CircuitBreakerState State() const noexcept {
    return static_cast<CircuitBreakerState>(
      state_.load(std::memory_order_acquire)
    );
  }

  // Cumulative consecutive failure count. Exposed for metrics.
  uint64_t FailureCount() const noexcept {
    return failure_count_.load(std::memory_order_relaxed);
  }

  // Total number of state transitions since construction. Exposed for metrics.
  uint64_t TransitionCount() const noexcept {
    return transition_count_.load(std::memory_order_relaxed);
  }

  // Hot path. Lock-free. Returns true if the request should be forwarded.
  //
  // Callers MUST invoke OnSuccess() or OnFailure() after the request
  // completes. Failing to do so in HALF_OPEN state will exhaust the probe
  // quota and stall recovery until the next open_timeout cycle.
  bool AllowRequest() noexcept {
    int s = state_.load(std::memory_order_acquire);

    if (s == kClosedInt) {
      return true;
    }

    if (s == kOpenInt) {
      uint64_t now     = NowMs();
      uint64_t entered = open_entered_ms_.load(std::memory_order_relaxed);
      if (now - entered < static_cast<uint64_t>(cfg_.open_timeout.count())) {
        return false;  // timeout not yet elapsed — fast reject
      }
      // Timeout elapsed. Race all threads to claim the OPEN→HALF_OPEN transition.
      // compare_exchange_strong updates s to the observed value on failure,
      // so the fall-through below handles the "lost the race" case correctly.
      if (state_.compare_exchange_strong(s, kHalfOpenInt,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        half_open_requests_.store(0, std::memory_order_relaxed);
        success_count_.store(0, std::memory_order_relaxed);
        transition_count_.fetch_add(1, std::memory_order_relaxed);
        s = kHalfOpenInt;
      }
    }

    if (s == kHalfOpenInt) {
      // Admit only the first half_open_max_requests probes.
      // fetch_add returns the pre-increment value; requests with index >= limit
      // are rejected without touching the upstream.
      uint64_t slot = half_open_requests_.fetch_add(1, std::memory_order_relaxed);
      return slot < static_cast<uint64_t>(cfg_.half_open_max_requests);
    }

    return false;
  }

  // Must be called after a successful upstream response (2xx, 3xx, 4xx).
  // 5xx and connection errors must call OnFailure() instead.
  //
  // CLOSED:    accumulates consecutive successes; resets failure_count_ only
  //            after success_threshold is reached.
  // HALF_OPEN: sufficient consecutive successes transition to CLOSED.
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

  // Must be called on upstream failure: 5xx response, connection error, or timeout.
  //
  // HALF_OPEN: any failure immediately re-opens the breaker.
  // CLOSED:    increments the consecutive failure counter; trips to OPEN
  //            once failure_threshold is reached.
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
  std::atomic<uint64_t> failure_count_{0};    // consecutive failures (CLOSED)
  std::atomic<uint64_t> success_count_{0};    // consecutive successes (CLOSED / HALF_OPEN)
  std::atomic<uint64_t> half_open_requests_{0}; // probe slots claimed in HALF_OPEN
  std::atomic<uint64_t> open_entered_ms_{0};  // steady_clock ms when OPEN was entered
  std::atomic<uint64_t> transition_count_{0}; // total state transitions — metrics only
};

}  // namespace runtime::gateway