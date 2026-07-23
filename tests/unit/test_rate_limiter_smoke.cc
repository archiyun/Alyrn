// RateLimiter 限流器单元测试
//
// 编译
// cmake --build build-tests --target rate_limiter_smoke_test -j$(nproc)
// 运行
// ./build-tests/tests/rate_limiter_smoke_test

#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "coropact/gateway/rate_limiter.h"

namespace {

using namespace std::chrono_literals;

bool Expect(bool condition, const char* msg) {
  if (!condition) {
    std::cerr << "[FAIL] " << msg << '\n';
    return false;
  }
  return true;
}

void Passed(const char* name) {
  std::cout << "[PASS] " << name << '\n';
}

// ================================================================
// 全局限流 —— 禁用时
// ================================================================

bool TestGlobalDisabledAlwaysAllows() {
  coropact::gateway::RateLimiterConfig cfg;
  cfg.global_enabled = false;
  coropact::gateway::RateLimiter rl(cfg);

  for (int i = 0; i < 10000; ++i) {
    if (!Expect(rl.AllowGlobal(), "global disabled must always allow")) return false;
  }
  Passed("TestGlobalDisabledAlwaysAllows");
  return true;
}

// ================================================================
// 全局限流 —— 启用时, burst 内放行
// ================================================================

bool TestGlobalEnabledAllowsUpToBurst() {
  coropact::gateway::RateLimiterConfig cfg;
  cfg.global_enabled = true;
  cfg.global_rate = 0.001; // 极低速率，确保不会在测试期间补充
  cfg.global_burst = 3.0;
  coropact::gateway::RateLimiter rl(cfg);

  // burst=3，初始令牌满，前 3 个请求放行
  if (!Expect(rl.AllowGlobal(), "request 1/3 must pass")) return false;
  if (!Expect(rl.AllowGlobal(), "request 2/3 must pass")) return false;
  if (!Expect(rl.AllowGlobal(), "request 3/3 must pass")) return false;

  // 桶已空，第 4 个请求被拒
  if (!Expect(!rl.AllowGlobal(), "request 4 must be rejected (bucket empty)")) return false;

  Passed("TestGlobalEnabledAllowsUpToBurst");
  return true;
}

// ================================================================
// 全局限流 —— 令牌补充后恢复
// ================================================================

bool TestGlobalTokenRefillAfterTime() {
  coropact::gateway::RateLimiterConfig cfg;
  cfg.global_enabled = true;
  cfg.global_rate = 200.0; // 200 token/s = 1 token per 5ms
  cfg.global_burst = 1.0;
  coropact::gateway::RateLimiter rl(cfg);

  // 消耗掉唯一令牌
  if (!Expect(rl.AllowGlobal(), "initial token must pass")) return false;
  if (!Expect(!rl.AllowGlobal(), "must be rejected after exhaustion")) return false;

  // 等待补充
  std::this_thread::sleep_for(20ms);

  if (!Expect(rl.AllowGlobal(), "must pass after token refill")) return false;

  Passed("TestGlobalTokenRefillAfterTime");
  return true;
}

// ================================================================
// Per-IP 限流 —— 禁用时
// ================================================================

bool TestPerIPDisabledAlwaysAllows() {
  coropact::gateway::RateLimiterConfig cfg;
  cfg.per_ip_enabled = false;
  coropact::gateway::RateLimiter rl(cfg);

  for (int i = 0; i < 1000; ++i) {
    if (!Expect(rl.AllowPerIP("192.168.1.1"), "per-ip disabled must always allow")) return false;
  }
  Passed("TestPerIPDisabledAlwaysAllows");
  return true;
}

// ================================================================
// Per-IP 限流 —— 启用时, burst 内放行
// ================================================================

bool TestPerIPEnabledAllowsUpToBurst() {
  coropact::gateway::RateLimiterConfig cfg;
  cfg.per_ip_enabled = true;
  cfg.per_ip_rate = 0.001;
  cfg.per_ip_burst = 2.0;
  coropact::gateway::RateLimiter rl(cfg);

  if (!Expect(rl.AllowPerIP("10.0.0.1"), "ip request 1/2 must pass")) return false;
  if (!Expect(rl.AllowPerIP("10.0.0.1"), "ip request 2/2 must pass")) return false;
  if (!Expect(!rl.AllowPerIP("10.0.0.1"), "ip request 3 must be rejected")) return false;

  Passed("TestPerIPEnabledAllowsUpToBurst");
  return true;
}

// ================================================================
// Per-IP 限流 —— 不同 IP 各自独立
// ================================================================

bool TestDifferentIPsAreIndependent() {
  coropact::gateway::RateLimiterConfig cfg;
  cfg.per_ip_enabled = true;
  cfg.per_ip_rate = 0.001;
  cfg.per_ip_burst = 1.0;
  coropact::gateway::RateLimiter rl(cfg);

  // 两个 IP 各自消耗自己的 burst
  if (!Expect(rl.AllowPerIP("1.1.1.1"), "ip1 first must pass")) return false;
  if (!Expect(!rl.AllowPerIP("1.1.1.1"), "ip1 second must be rejected")) return false;

  // ip2 有独立的桶，不受 ip1 影响
  if (!Expect(rl.AllowPerIP("2.2.2.2"), "ip2 first must pass")) return false;
  if (!Expect(!rl.AllowPerIP("2.2.2.2"), "ip2 second must be rejected")) return false;

  Passed("TestDifferentIPsAreIndependent");
  return true;
}

// ================================================================
// Per-IP 令牌补充
// ================================================================

bool TestPerIPTokenRefillAfterTime() {
  coropact::gateway::RateLimiterConfig cfg;
  cfg.per_ip_enabled = true;
  cfg.per_ip_rate = 200.0; // 1 token per 5ms
  cfg.per_ip_burst = 1.0;
  coropact::gateway::RateLimiter rl(cfg);

  if (!Expect(rl.AllowPerIP("3.3.3.3"), "initial token must pass")) return false;
  if (!Expect(!rl.AllowPerIP("3.3.3.3"), "must be rejected after exhaustion")) return false;

  std::this_thread::sleep_for(20ms);

  if (!Expect(rl.AllowPerIP("3.3.3.3"), "must pass after refill")) return false;

  Passed("TestPerIPTokenRefillAfterTime");
  return true;
}

// ================================================================
// 两层限流都必须通过
// ================================================================

bool TestBothGlobalAndPerIPMustPass() {
  coropact::gateway::RateLimiterConfig cfg;
  cfg.global_enabled = true;
  cfg.global_rate = 0.001;
  cfg.global_burst = 10.0; // 全局桶宽松
  cfg.per_ip_enabled = true;
  cfg.per_ip_rate = 0.001;
  cfg.per_ip_burst = 1.0; // per-ip 桶严格
  coropact::gateway::RateLimiter rl(cfg);

  // 第 1 个：global OK, per-ip OK → 通过
  bool global_ok = rl.AllowGlobal();
  bool ip_ok = rl.AllowPerIP("4.4.4.4");
  if (!Expect(global_ok && ip_ok, "first request must pass both checks")) return false;

  // 第 2 个：per-ip 已空 → 被拒
  global_ok = rl.AllowGlobal();
  ip_ok = rl.AllowPerIP("4.4.4.4");
  if (!Expect(!ip_ok, "second request must fail per-ip check")) return false;

  Passed("TestBothGlobalAndPerIPMustPass");
  return true;
}

// ================================================================
// 并发全局限流：多线程争抢同一个全局桶
// ================================================================

bool TestConcurrentGlobalRateLimiter() {
  coropact::gateway::RateLimiterConfig cfg;
  cfg.global_enabled = true;
  cfg.global_rate = 0.001;
  cfg.global_burst = 50.0;
  coropact::gateway::RateLimiter rl(cfg);

  std::atomic<int> allowed{0};
  std::atomic<int> rejected{0};

  auto worker = [&] {
    for (int i = 0; i < 20; ++i) {
      if (rl.AllowGlobal()) allowed.fetch_add(1, std::memory_order_relaxed);
      else                  rejected.fetch_add(1, std::memory_order_relaxed);
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < 5; ++i) threads.emplace_back(worker);
  for (auto& t : threads) t.join();

  // burst=50, 100 total requests → exactly 50 allowed
  if (!Expect(allowed.load() == 50, "exactly burst=50 requests must pass")) return false;
  if (!Expect(rejected.load() == 50, "remaining 50 must be rejected")) return false;

  Passed("TestConcurrentGlobalRateLimiter");
  return true;
}

// ================================================================
// 并发 Per-IP 限流：多线程同一个 IP
// ================================================================

bool TestConcurrentPerIPSameIP() {
  coropact::gateway::RateLimiterConfig cfg;
  cfg.per_ip_enabled = true;
  cfg.per_ip_rate = 0.001;
  cfg.per_ip_burst = 10.0;
  coropact::gateway::RateLimiter rl(cfg);

  std::atomic<int> allowed{0};

  auto worker = [&] {
    for (int i = 0; i < 5; ++i) {
      if (rl.AllowPerIP("9.9.9.9")) allowed.fetch_add(1, std::memory_order_relaxed);
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < 4; ++i) threads.emplace_back(worker);
  for (auto& t : threads) t.join();

  // burst=10, 20 total requests → exactly 10 allowed
  if (!Expect(allowed.load() == 10, "concurrent per-ip: exactly 10 must pass")) return false;

  Passed("TestConcurrentPerIPSameIP");
  return true;
}

// ================================================================
// 并发 Per-IP 限流：多线程不同 IP（懒创建 bucket 的正确性）
// ================================================================

bool TestConcurrentPerIPDifferentIPs() {
  coropact::gateway::RateLimiterConfig cfg;
  cfg.per_ip_enabled = true;
  cfg.per_ip_rate = 0.001;
  cfg.per_ip_burst = 1.0;
  coropact::gateway::RateLimiter rl(cfg);

  std::atomic<int> allowed{0};
  constexpr int kNumIPs = 8;

  auto worker = [&](int id) {
    std::string ip = "10.0.0." + std::to_string(id);
    // 每个 IP burst=1，发送 3 次，只有 1 次应通过
    for (int i = 0; i < 3; ++i) {
      if (rl.AllowPerIP(ip)) allowed.fetch_add(1, std::memory_order_relaxed);
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < kNumIPs; ++i) threads.emplace_back(worker, i);
  for (auto& t : threads) t.join();

  // 每个 IP 各 1 次 → 共 kNumIPs 次
  if (!Expect(allowed.load() == kNumIPs,
              "each IP must pass exactly once (one per burst)")) return false;

  Passed("TestConcurrentPerIPDifferentIPs");
  return true;
}

// Idle per-IP buckets that have refilled to full are evicted once the map hits
// per_ip_max_buckets. Eviction is loss-less: a re-created bucket starts full
// too, so a previously-seen IP behaves identically afterwards.
bool TestPerIPEvictsIdleFullBuckets() {
  coropact::gateway::RateLimiterConfig cfg;
  cfg.per_ip_enabled = true;
  cfg.per_ip_rate = 1000.0;    // full_ms = burst/rate*1000 = 1ms (refills fast)
  cfg.per_ip_burst = 1.0;
  cfg.per_ip_max_buckets = 4;  // small cap so eviction is easy to trigger
  coropact::gateway::RateLimiter rl(cfg);

  for (int i = 0; i < 4; ++i) {
    rl.AllowPerIP("10.0.0." + std::to_string(i));
  }
  if (!Expect(rl.per_ip_bucket_count() == 4, "4 buckets created (at cap)")) return false;

  // Let every bucket refill to full so it becomes evictable.
  std::this_thread::sleep_for(20ms);

  // The 5th distinct IP hits the cap, triggers a sweep that drops the 4 idle
  // full buckets, then inserts itself.
  rl.AllowPerIP("10.0.0.99");
  if (!Expect(rl.per_ip_bucket_count() == 1,
              "idle full buckets evicted; only the new one remains")) return false;

  // Loss-less: an evicted IP works exactly as before (a fresh full bucket).
  if (!Expect(rl.AllowPerIP("10.0.0.0"),
              "re-created bucket for an evicted IP starts full")) return false;

  Passed("TestPerIPEvictsIdleFullBuckets");
  return true;
}

// Eviction must never drop a bucket that is still rate-limiting. Once the hard
// cap is full of active buckets, a new identity is rejected rather than growing
// memory or silently resetting an existing client's limit.
bool TestPerIPEvictionKeepsActiveBuckets() {
  coropact::gateway::RateLimiterConfig cfg;
  cfg.per_ip_enabled = true;
  cfg.per_ip_rate = 0.001;  // full_ms ~= 1e6 ms: effectively never refills
  cfg.per_ip_burst = 1.0;
  cfg.per_ip_max_buckets = 2;
  coropact::gateway::RateLimiter rl(cfg);

  // Drain both buckets (token -> 0); they are nowhere near refilled.
  if (!Expect(rl.AllowPerIP("a"), "a: first token passes")) return false;
  if (!Expect(rl.AllowPerIP("b"), "b: first token passes")) return false;
  if (!Expect(!rl.AllowPerIP("a"), "a: second is limited")) return false;

  // A 3rd IP cannot be admitted. No bucket is full, so none can be dropped.
  if (!Expect(!rl.AllowPerIP("c"),
              "new identity must be rejected when active buckets fill cap")) {
    return false;
  }
  if (!Expect(rl.per_ip_bucket_count() == 2,
              "per-ip bucket count must never exceed the hard cap")) return false;
  // The drained bucket is intact: still limited, not silently reset.
  if (!Expect(!rl.AllowPerIP("a"), "a stays limited after the over-cap insert")) return false;

  Passed("TestPerIPEvictionKeepsActiveBuckets");
  return true;
}

}  // namespace

int main() {
  int passed = 0;
  int total = 0;

#define RUN(test) do { ++total; if (test()) ++passed; } while (0)

  RUN(TestGlobalDisabledAlwaysAllows);
  RUN(TestGlobalEnabledAllowsUpToBurst);
  RUN(TestGlobalTokenRefillAfterTime);
  RUN(TestPerIPDisabledAlwaysAllows);
  RUN(TestPerIPEnabledAllowsUpToBurst);
  RUN(TestDifferentIPsAreIndependent);
  RUN(TestPerIPTokenRefillAfterTime);
  RUN(TestBothGlobalAndPerIPMustPass);
  RUN(TestConcurrentGlobalRateLimiter);
  RUN(TestConcurrentPerIPSameIP);
  RUN(TestConcurrentPerIPDifferentIPs);
  RUN(TestPerIPEvictsIdleFullBuckets);
  RUN(TestPerIPEvictionKeepsActiveBuckets);

  std::cout << "===========================\n";
  std::cout << passed << "/" << total << " tests passed.\n";

  return (passed == total) ? 0 : 1;
}
