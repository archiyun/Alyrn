// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <limits>

namespace vexo::gateway {

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

  // Returns the current state for diagnostics; do not use it to make routing
  // decisions — call AllowRequest().
  CircuitBreakerState state() const noexcept {
    const int state = state_.load(std::memory_order_acquire);
    if (state == kTransitioningToOpenInt ||
        state == kTransitioningToHalfOpenInt) {
      return CircuitBreakerState::kOpen;
    }
    if (state == kTransitioningToClosedInt) {
      return CircuitBreakerState::kHalfOpen;
    }
    return static_cast<CircuitBreakerState>(state);
  }

  // Cumulative consecutive failure count for diagnostics and tests.
  uint64_t failure_count() const noexcept {
    return ClosedFailures(
        closed_counts_.load(std::memory_order_relaxed));
  }

  // Total number of state transitions since construction.
  uint64_t transition_count() const noexcept {
    return transition_count_.load(std::memory_order_relaxed);
  }

  // Hot path. Lock-free. Returns true if the request should be forwarded.
  //
  // Callers MUST invoke OnSuccess() or OnFailure() after the request completes.
  // Defense-in-depth: if a HALF_OPEN probe's outcome is lost, AllowRequest()
  // falls back to OPEN once open_timeout elapses with the quota exhausted and
  // unresolved (see the HALF_OPEN branch below), so a single dropped outcome
  // cannot strand the breaker in HALF_OPEN rejecting everything forever.
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
      // Initialize the new probe cycle before publishing HALF_OPEN. Publishing
      // HALF_OPEN first would let another thread claim a slot before the
      // counters are reset, then allow the winner to reset and claim it again.
      if (state_.compare_exchange_strong(s, kTransitioningToHalfOpenInt,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        half_open_requests_.store(0, std::memory_order_relaxed);
        half_open_success_count_.store(0, std::memory_order_relaxed);
        half_open_probe_ms_.store(0, std::memory_order_relaxed);
        transition_count_.fetch_add(1, std::memory_order_relaxed);
        state_.store(kHalfOpenInt, std::memory_order_release);
        s = kHalfOpenInt;
      }
    }

    if (s == kHalfOpenInt) {
      // Admit only the first half_open_max_requests probes. fetch_add returns
      // the pre-increment value; requests with index >= limit are rejected
      // without touching the upstream.
      uint64_t now  = NowMs();
      uint64_t slot = half_open_requests_.fetch_add(1, std::memory_order_relaxed);
      if (slot < static_cast<uint64_t>(cfg_.half_open_max_requests)) {
        half_open_probe_ms_.store(now, std::memory_order_release);
        return true;
      }
      // Quota exhausted. If no admitted probe has resolved within open_timeout,
      // its outcome was likely lost (caller never called OnSuccess/OnFailure).
      // Fall back to OPEN so the OPEN->HALF_OPEN timer re-arms and a fresh probe
      // is admitted next cycle, instead of rejecting forever.
      uint64_t probe_at = half_open_probe_ms_.load(std::memory_order_acquire);
      if (cfg_.open_timeout.count() > 0 &&
          now - probe_at >=
              static_cast<uint64_t>(cfg_.open_timeout.count())) {
        int expected = kHalfOpenInt;
        if (state_.compare_exchange_strong(expected, kTransitioningToOpenInt,
              std::memory_order_acq_rel, std::memory_order_acquire)) {
          open_entered_ms_.store(now, std::memory_order_release);
          half_open_success_count_.store(0, std::memory_order_relaxed);
          half_open_requests_.store(0, std::memory_order_relaxed);
          half_open_probe_ms_.store(0, std::memory_order_relaxed);
          transition_count_.fetch_add(1, std::memory_order_relaxed);
          state_.store(kOpenInt, std::memory_order_release);
        }
      }
      return false;
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
      uint64_t current = closed_counts_.load(std::memory_order_relaxed);
      while (state_.load(std::memory_order_acquire) == kClosedInt) {
        uint32_t failures = ClosedFailures(current);
        uint32_t successes = SaturatingIncrement(ClosedSuccesses(current));
        if (successes >= SuccessThreshold()) {
          failures = 0;
          successes = 0;
        }
        if (closed_counts_.compare_exchange_weak(
                current, PackClosedCounts(failures, successes),
                std::memory_order_relaxed, std::memory_order_relaxed)) {
          return;
        }
      }
      return;
    }

    if (s == kHalfOpenInt) {
      uint64_t n =
          half_open_success_count_.fetch_add(1, std::memory_order_relaxed) + 1;
      if (n >= SuccessThreshold()) {
        int expected = kHalfOpenInt;
        if (state_.compare_exchange_strong(expected, kTransitioningToClosedInt,
                std::memory_order_acq_rel, std::memory_order_acquire)) {
          closed_counts_.store(0, std::memory_order_relaxed);
          half_open_success_count_.store(0, std::memory_order_relaxed);
          half_open_requests_.store(0, std::memory_order_relaxed);
          half_open_probe_ms_.store(0, std::memory_order_relaxed);
          transition_count_.fetch_add(1, std::memory_order_relaxed);
          state_.store(kClosedInt, std::memory_order_release);
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
      if (state_.compare_exchange_strong(expected, kTransitioningToOpenInt,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        open_entered_ms_.store(NowMs(), std::memory_order_release);
        half_open_success_count_.store(0, std::memory_order_relaxed);
        half_open_requests_.store(0, std::memory_order_relaxed);
        half_open_probe_ms_.store(0, std::memory_order_relaxed);
        transition_count_.fetch_add(1, std::memory_order_relaxed);
        state_.store(kOpenInt, std::memory_order_release);
      }
      return;
    }

    if (s == kClosedInt) {
      uint64_t current = closed_counts_.load(std::memory_order_relaxed);
      uint32_t failures = 0;
      while (state_.load(std::memory_order_acquire) == kClosedInt) {
        failures = SaturatingIncrement(ClosedFailures(current));
        if (closed_counts_.compare_exchange_weak(
                current, PackClosedCounts(failures, 0),
                std::memory_order_relaxed, std::memory_order_relaxed)) {
          break;
        }
      }
      if (failures >= FailureThreshold()) {
        int expected = kClosedInt;
        if (state_.compare_exchange_strong(expected, kTransitioningToOpenInt,
                std::memory_order_acq_rel, std::memory_order_acquire)) {
          open_entered_ms_.store(NowMs(), std::memory_order_release);
          half_open_success_count_.store(0, std::memory_order_relaxed);
          half_open_requests_.store(0, std::memory_order_relaxed);
          half_open_probe_ms_.store(0, std::memory_order_relaxed);
          transition_count_.fetch_add(1, std::memory_order_relaxed);
          state_.store(kOpenInt, std::memory_order_release);
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
  static constexpr int kTransitioningToOpenInt = 3;
  static constexpr int kTransitioningToHalfOpenInt = 4;
  static constexpr int kTransitioningToClosedInt = 5;

  static constexpr uint64_t PackClosedCounts(
      uint32_t failures, uint32_t successes) noexcept {
    return (static_cast<uint64_t>(failures) << 32) | successes;
  }

  static constexpr uint32_t ClosedFailures(uint64_t counts) noexcept {
    return static_cast<uint32_t>(counts >> 32);
  }

  static constexpr uint32_t ClosedSuccesses(uint64_t counts) noexcept {
    return static_cast<uint32_t>(counts);
  }

  static constexpr uint32_t SaturatingIncrement(uint32_t value) noexcept {
    return value == std::numeric_limits<uint32_t>::max() ? value : value + 1;
  }

  uint32_t FailureThreshold() const noexcept {
    return cfg_.failure_threshold > 0
        ? static_cast<uint32_t>(cfg_.failure_threshold)
        : 1;
  }

  uint32_t SuccessThreshold() const noexcept {
    return cfg_.success_threshold > 0
        ? static_cast<uint32_t>(cfg_.success_threshold)
        : 1;
  }

  CircuitBreakerConfig cfg_;

  std::atomic<int>      state_{kClosedInt};
  // CLOSED failure score and recovery-success streak are updated together so
  // concurrent outcomes cannot reset a failure with a stale success streak.
  std::atomic<uint64_t> closed_counts_{0};
  std::atomic<uint64_t> half_open_success_count_{0};
  std::atomic<uint64_t> half_open_requests_{0}; // probe slots claimed in HALF_OPEN
  std::atomic<uint64_t> half_open_probe_ms_{0}; // steady_clock ms of last admitted probe
  std::atomic<uint64_t> open_entered_ms_{0};  // steady_clock ms when OPEN was entered
  std::atomic<uint64_t> transition_count_{0};
};

}  // namespace vexo::gateway
