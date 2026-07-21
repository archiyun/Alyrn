// Adversarial regression tests for gateway resilience components.
//
// These tests encode defensive invariants. They are intentionally stricter
// than smoke tests and are expected to fail when an implementation can be
// driven into a false-health or resource-exhaustion state.

#include <iostream>
#include <string>

#include "vexo/gateway/circuit_breaker.h"
#include "vexo/gateway/rate_limiter.h"
#include "vexo/gateway/upstream.h"

namespace {

bool Expect(bool condition, const char* message) {
  if (!condition) {
    std::cerr << "[VULNERABLE] " << message << '\n';
    return false;
  }
  return true;
}

void Passed(const char* name) {
  std::cout << "[RESISTED] " << name << '\n';
}

bool TestCircuitBreakerRequiresConsecutiveSuccesses() {
  vexo::gateway::CircuitBreakerConfig cfg;
  cfg.failure_threshold = 100;
  cfg.success_threshold = 2;
  vexo::gateway::CircuitBreaker breaker(cfg);

  breaker.OnFailure();
  breaker.OnSuccess();
  breaker.OnFailure();  // Must break the success streak.
  breaker.OnSuccess();

  if (!Expect(
          breaker.failure_count() == 2,
          "circuit breaker resets failures after non-consecutive successes")) {
    return false;
  }

  Passed("TestCircuitBreakerRequiresConsecutiveSuccesses");
  return true;
}

bool TestPerIPBucketLimitIsHard() {
  vexo::gateway::RateLimiterConfig cfg;
  cfg.per_ip_enabled = true;
  cfg.per_ip_rate = 0.000001;
  cfg.per_ip_burst = 1.0;
  cfg.per_ip_max_buckets = 8;
  vexo::gateway::RateLimiter limiter(cfg);

  // Simulate a high-cardinality identity spray. Every bucket remains active,
  // so an eviction policy that only drops full buckets cannot enforce the cap.
  for (int i = 0; i < 256; ++i) {
    limiter.AllowPerIP("198.51.100." + std::to_string(i));
  }

  if (!Expect(
          limiter.per_ip_bucket_count() <= cfg.per_ip_max_buckets,
          "per-IP bucket spray exceeds the configured cap and grows memory")) {
    std::cerr << "  configured cap=" << cfg.per_ip_max_buckets
              << " live buckets=" << limiter.per_ip_bucket_count() << '\n';
    return false;
  }

  Passed("TestPerIPBucketLimitIsHard");
  return true;
}

bool TestUpstreamBulkheadIsHard() {
  vexo::gateway::UpstreamConfig cfg;
  cfg.name = "bulkhead";
  cfg.max_concurrent_requests = 2;
  vexo::gateway::Upstream upstream(cfg);

  if (!Expect(upstream.TryAcquireRequestSlot(), "bulkhead slot 1 must pass")) {
    return false;
  }
  if (!Expect(upstream.TryAcquireRequestSlot(), "bulkhead slot 2 must pass")) {
    return false;
  }
  if (!Expect(!upstream.TryAcquireRequestSlot(),
              "bulkhead must reject above max_concurrent_requests")) {
    return false;
  }
  if (!Expect(upstream.active_requests() == 2,
              "rejected request must not inflate active slot count")) {
    return false;
  }

  upstream.ReleaseRequestSlot();
  if (!Expect(upstream.TryAcquireRequestSlot(),
              "released bulkhead slot must be reusable")) {
    return false;
  }
  upstream.ReleaseRequestSlot();
  upstream.ReleaseRequestSlot();

  if (!Expect(upstream.active_requests() == 0,
              "all bulkhead slots must be released exactly once")) {
    return false;
  }

  Passed("TestUpstreamBulkheadIsHard");
  return true;
}

}  // namespace

int main() {
  int passed = 0;
  int total = 0;

#define RUN(test)                 \
  do {                            \
    ++total;                      \
    if (test()) ++passed;         \
  } while (false)

  RUN(TestCircuitBreakerRequiresConsecutiveSuccesses);
  RUN(TestPerIPBucketLimitIsHard);
  RUN(TestUpstreamBulkheadIsHard);

  std::cout << "===========================\n"
            << passed << "/" << total << " adversarial invariants held.\n";
  return passed == total ? 0 : 1;
}
