// 熔断 + 限流 + 降级 集成测试
//
// 不依赖 GTest，不依赖真实 TCP 连接。
// 通过复现 GatewayServer::OnMessage 中的策略决策逻辑，
// 验证三个组件在协同工作时的正确行为。
//
// 编译
// cmake --build build-tests --target resilience_integration_smoke_test -j$(nproc)
// 运行
// ./build-tests/tests/resilience_integration_smoke_test

#include <chrono>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>

#include "vexo/gateway/circuit_breaker.h"
#include "vexo/gateway/fallback_config.h"
#include "vexo/gateway/rate_limiter.h"

namespace {

using namespace std::chrono_literals;

// ================================================================
// 测试基础设施
// ================================================================

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

// 复现 GatewayServer::OnMessage 中的策略决策顺序：
//   1. 全局限流
//   2. Per-IP 限流
//   3. 熔断检查
//   4. 转发 upstream
//
// 返回值枚举
enum class Outcome {
  kForwarded,    // 正常转发到 upstream
  kRateLimited,  // 限流拒绝 (429)
  kCircuitOpen,  // 熔断，返回 fallback
};

Outcome Dispatch(vexo::gateway::RateLimiter* rl,
                 vexo::gateway::CircuitBreaker* cb,
                 std::string_view client_ip) {
  if (rl) {
    if (!rl->AllowGlobal() || !rl->AllowPerIP(client_ip)) {
      return Outcome::kRateLimited;
    }
  }
  if (cb && !cb->AllowRequest()) {
    return Outcome::kCircuitOpen;
  }
  return Outcome::kForwarded;
}

// ================================================================
// Test 1: 正常流量 —— 全部放行
// ================================================================

bool TestNormalFlowPassesThrough() {
  vexo::gateway::RateLimiterConfig rl_cfg;
  rl_cfg.global_enabled = true;
  rl_cfg.global_rate = 10000.0;
  rl_cfg.global_burst = 100.0;
  rl_cfg.per_ip_enabled = true;
  rl_cfg.per_ip_rate = 1000.0;
  rl_cfg.per_ip_burst = 10.0;
  vexo::gateway::RateLimiter rl(rl_cfg);

  vexo::gateway::CircuitBreakerConfig cb_cfg;
  vexo::gateway::CircuitBreaker cb(cb_cfg);

  for (int i = 0; i < 5; ++i) {
    auto r = Dispatch(&rl, &cb, "10.0.0.1");
    if (!Expect(r == Outcome::kForwarded, "normal flow must be forwarded")) return false;
    cb.OnSuccess();
  }
  Passed("TestNormalFlowPassesThrough");
  return true;
}

// ================================================================
// Test 2: 全局限流触发 → 429 响应
// ================================================================

bool TestGlobalRateLimitTriggered() {
  vexo::gateway::RateLimiterConfig rl_cfg;
  rl_cfg.global_enabled = true;
  rl_cfg.global_rate = 0.001; // 几乎不补充
  rl_cfg.global_burst = 2.0;
  vexo::gateway::RateLimiter rl(rl_cfg);

  // 前 2 次通过
  if (!Expect(Dispatch(&rl, nullptr, "1.1.1.1") == Outcome::kForwarded,
              "global rl: first request must pass")) return false;
  if (!Expect(Dispatch(&rl, nullptr, "1.1.1.1") == Outcome::kForwarded,
              "global rl: second request must pass")) return false;

  // 桶空，后续全被限流
  for (int i = 0; i < 5; ++i) {
    if (!Expect(Dispatch(&rl, nullptr, "1.1.1.1") == Outcome::kRateLimited,
                "global rl: subsequent requests must be rate limited")) return false;
  }
  Passed("TestGlobalRateLimitTriggered");
  return true;
}

// ================================================================
// Test 3: Per-IP 限流触发 → 429 响应
// ================================================================

bool TestPerIPRateLimitTriggered() {
  vexo::gateway::RateLimiterConfig rl_cfg;
  rl_cfg.global_enabled = true;
  rl_cfg.global_rate = 10000.0;
  rl_cfg.global_burst = 1000.0;  // 全局宽松
  rl_cfg.per_ip_enabled = true;
  rl_cfg.per_ip_rate = 0.001;
  rl_cfg.per_ip_burst = 1.0;    // per-ip 严格
  vexo::gateway::RateLimiter rl(rl_cfg);

  // ip1 通过一次后被限
  if (!Expect(Dispatch(&rl, nullptr, "2.2.2.2") == Outcome::kForwarded,
              "per-ip: first request must pass")) return false;
  if (!Expect(Dispatch(&rl, nullptr, "2.2.2.2") == Outcome::kRateLimited,
              "per-ip: second request must be rate limited")) return false;

  // ip2 仍然可以通过（独立的桶）
  if (!Expect(Dispatch(&rl, nullptr, "3.3.3.3") == Outcome::kForwarded,
              "per-ip: different IP must still pass")) return false;

  Passed("TestPerIPRateLimitTriggered");
  return true;
}

// ================================================================
// Test 4: 熔断器 OPEN → 降级（RL 通过，CB 拒绝）
// ================================================================

bool TestCircuitBreakerOpenReturnsFallback() {
  vexo::gateway::RateLimiterConfig rl_cfg;
  rl_cfg.global_enabled = true;
  rl_cfg.global_rate = 10000.0;
  rl_cfg.global_burst = 1000.0;
  vexo::gateway::RateLimiter rl(rl_cfg);

  vexo::gateway::CircuitBreakerConfig cb_cfg;
  cb_cfg.failure_threshold = 3;
  cb_cfg.open_timeout = 60000ms; // 很长，确保测试期间不会变回 HALF_OPEN
  vexo::gateway::CircuitBreaker cb(cb_cfg);

  // 触发 3 次失败 → CB OPEN
  cb.OnFailure();
  cb.OnFailure();
  cb.OnFailure();
  if (!Expect(cb.state() == vexo::gateway::CircuitBreakerState::kOpen,
              "CB must be open after failures")) return false;

  // 后续所有请求被熔断，即使 RL 通过
  for (int i = 0; i < 5; ++i) {
    if (!Expect(Dispatch(&rl, &cb, "4.4.4.4") == Outcome::kCircuitOpen,
                "CB open: requests must return fallback")) return false;
  }
  Passed("TestCircuitBreakerOpenReturnsFallback");
  return true;
}

// ================================================================
// Test 5: 限流优先于熔断 —— RL 先拒，CB 不判
//
// 网关逻辑：先过 RL，再过 CB。RL 拒绝时直接返回 429，不走 CB。
// ================================================================

bool TestRateLimitFiresBeforeCircuitBreaker() {
  vexo::gateway::RateLimiterConfig rl_cfg;
  rl_cfg.global_enabled = true;
  rl_cfg.global_rate = 0.001;
  rl_cfg.global_burst = 1.0; // 只放 1 个
  vexo::gateway::RateLimiter rl(rl_cfg);

  vexo::gateway::CircuitBreakerConfig cb_cfg;
  cb_cfg.failure_threshold = 1;
  vexo::gateway::CircuitBreaker cb(cb_cfg);

  // 让 CB 先打开
  cb.OnFailure();
  if (!Expect(cb.state() == vexo::gateway::CircuitBreakerState::kOpen,
              "CB must be open")) return false;

  // 消耗唯一的 RL 令牌
  Dispatch(&rl, nullptr, "5.5.5.5");

  // 此时 RL 桶空、CB 也开
  // 期望：返回 kRateLimited（RL 先检查，先失败）
  auto r = Dispatch(&rl, &cb, "5.5.5.5");
  if (!Expect(r == Outcome::kRateLimited,
              "rate limit must fire before circuit breaker")) return false;

  Passed("TestRateLimitFiresBeforeCircuitBreaker");
  return true;
}

// ================================================================
// Test 6: 熔断恢复 —— OPEN → HALF_OPEN → CLOSED
// ================================================================

bool TestCircuitBreakerRecovery() {
  vexo::gateway::RateLimiterConfig rl_cfg;
  rl_cfg.global_enabled = true;
  rl_cfg.global_rate = 10000.0;
  rl_cfg.global_burst = 1000.0;
  vexo::gateway::RateLimiter rl(rl_cfg);

  vexo::gateway::CircuitBreakerConfig cb_cfg;
  cb_cfg.failure_threshold = 2;
  cb_cfg.success_threshold = 1;
  cb_cfg.open_timeout = 30ms; // 短超时
  cb_cfg.half_open_max_requests = 1;
  vexo::gateway::CircuitBreaker cb(cb_cfg);

  // 阶段 1：触发熔断
  cb.OnFailure();
  cb.OnFailure();
  if (!Expect(cb.state() == vexo::gateway::CircuitBreakerState::kOpen,
              "phase1: CB must be open")) return false;
  if (!Expect(Dispatch(&rl, &cb, "6.6.6.6") == Outcome::kCircuitOpen,
              "phase1: request must be rejected")) return false;

  // 阶段 2：等待超时，探测放行
  std::this_thread::sleep_for(50ms);
  if (!Expect(Dispatch(&rl, &cb, "6.6.6.6") == Outcome::kForwarded,
              "phase2: probe must be forwarded (HALF_OPEN)")) return false;
  if (!Expect(cb.state() == vexo::gateway::CircuitBreakerState::kHalfOpen,
              "phase2: CB must be HALF_OPEN")) return false;

  // 阶段 3：探测成功 → CLOSED
  cb.OnSuccess();
  if (!Expect(cb.state() == vexo::gateway::CircuitBreakerState::kClosed,
              "phase3: CB must recover to CLOSED")) return false;

  // 阶段 4：恢复后正常流量通过
  for (int i = 0; i < 3; ++i) {
    if (!Expect(Dispatch(&rl, &cb, "6.6.6.6") == Outcome::kForwarded,
                "phase4: normal traffic must pass after recovery")) return false;
    cb.OnSuccess();
  }
  Passed("TestCircuitBreakerRecovery");
  return true;
}

// ================================================================
// Test 7: 探测失败 → 重新打开
// ================================================================

bool TestHalfOpenProbeFails() {
  vexo::gateway::CircuitBreakerConfig cb_cfg;
  cb_cfg.failure_threshold = 1;
  cb_cfg.open_timeout = 10ms;
  cb_cfg.half_open_max_requests = 2;
  vexo::gateway::CircuitBreaker cb(cb_cfg);

  cb.OnFailure(); // → OPEN
  std::this_thread::sleep_for(20ms);

  // 探测放行
  cb.AllowRequest(); // OPEN → HALF_OPEN, 放行 1 个

  // 探测失败 → 重新 OPEN
  cb.OnFailure();
  if (!Expect(cb.state() == vexo::gateway::CircuitBreakerState::kOpen,
              "probe failure must re-open CB")) return false;

  Passed("TestHalfOpenProbeFails");
  return true;
}

// ================================================================
// Test 8: FallbackConfig 预渲染内容验证
// ================================================================

bool TestFallbackPreRenderedContent() {
  vexo::gateway::FallbackConfig fallback;
  fallback.enabled = true;
  fallback.status_code = vexo::http::StatusCode::ServiceUnavailable;
  fallback.body = R"({"error":"upstream unavailable"})";
  fallback.Init();

  if (!Expect(!fallback.pre_rendered.empty(),
              "fallback pre_rendered must not be empty")) return false;
  if (!Expect(fallback.pre_rendered.find("503") != std::string::npos,
              "fallback must contain status code 503")) return false;
  if (!Expect(fallback.pre_rendered.find("upstream unavailable") != std::string::npos,
              "fallback must contain custom body")) return false;
  if (!Expect(fallback.pre_rendered.find("Connection: close") != std::string::npos,
              "fallback must close connection")) return false;

  Passed("TestFallbackPreRenderedContent");
  return true;
}

// ================================================================
// Test 9: 完整降级链路
//   正常 → 故障累积 → CB 打开 → RL 限流 → CB 恢复 → 正常
// ================================================================

bool TestFullResilienceLifecycle() {
  vexo::gateway::RateLimiterConfig rl_cfg;
  rl_cfg.global_enabled = true;
  rl_cfg.global_rate = 10000.0;
  rl_cfg.global_burst = 100.0;
  vexo::gateway::RateLimiter rl(rl_cfg);

  vexo::gateway::CircuitBreakerConfig cb_cfg;
  cb_cfg.failure_threshold = 3;
  cb_cfg.success_threshold = 1;
  cb_cfg.open_timeout = 30ms;
  vexo::gateway::CircuitBreaker cb(cb_cfg);

  vexo::gateway::FallbackConfig fallback;
  fallback.enabled = true;
  fallback.Init();

  const std::string_view ip = "7.7.7.7";

  // 阶段 1：正常服务
  for (int i = 0; i < 5; ++i) {
    if (!Expect(Dispatch(&rl, &cb, ip) == Outcome::kForwarded,
                "lifecycle phase1: must forward")) return false;
    cb.OnSuccess();
  }

  // 阶段 2：下游故障 → CB OPEN
  cb.OnFailure();
  cb.OnFailure();
  cb.OnFailure();
  if (!Expect(cb.state() == vexo::gateway::CircuitBreakerState::kOpen,
              "lifecycle phase2: CB must open")) return false;

  // 阶段 3：熔断期间 → 降级响应（RL 通过，CB 拒绝）
  for (int i = 0; i < 3; ++i) {
    auto r = Dispatch(&rl, &cb, ip);
    if (!Expect(r == Outcome::kCircuitOpen,
                "lifecycle phase3: CB open must return fallback")) return false;
    // 降级时不调 upstream，CB 状态不变
  }

  // 阶段 4：超时后探测
  std::this_thread::sleep_for(50ms);
  auto r = Dispatch(&rl, &cb, ip);
  if (!Expect(r == Outcome::kForwarded,
              "lifecycle phase4: probe request must be forwarded")) return false;

  // 阶段 5：探测成功 → 恢复
  cb.OnSuccess();
  if (!Expect(cb.state() == vexo::gateway::CircuitBreakerState::kClosed,
              "lifecycle phase5: CB must recover to CLOSED")) return false;

  // 阶段 6：正常服务恢复
  for (int i = 0; i < 5; ++i) {
    if (!Expect(Dispatch(&rl, &cb, ip) == Outcome::kForwarded,
                "lifecycle phase6: normal traffic must resume")) return false;
    cb.OnSuccess();
  }

  Passed("TestFullResilienceLifecycle");
  return true;
}

// ================================================================
// Test 10: RL 令牌恢复后，即使 CB 曾开过，流量也能正常恢复
// ================================================================

bool TestRateLimitRecoveryIndependentOfCB() {
  vexo::gateway::RateLimiterConfig rl_cfg;
  rl_cfg.global_enabled = true;
  rl_cfg.global_rate = 200.0; // 1 token per 5ms
  rl_cfg.global_burst = 1.0;
  vexo::gateway::RateLimiter rl(rl_cfg);

  vexo::gateway::CircuitBreakerConfig cb_cfg;
  cb_cfg.failure_threshold = 10; // 很高，CB 不会打开
  vexo::gateway::CircuitBreaker cb(cb_cfg);

  // 消耗 RL 令牌
  if (!Expect(Dispatch(&rl, &cb, "8.8.8.8") == Outcome::kForwarded,
              "rl recovery: first must pass")) return false;
  if (!Expect(Dispatch(&rl, &cb, "8.8.8.8") == Outcome::kRateLimited,
              "rl recovery: must be rate limited")) return false;

  // 等待 RL 补充
  std::this_thread::sleep_for(20ms);

  // RL 恢复，CB 正常 → 请求通过
  if (!Expect(Dispatch(&rl, &cb, "8.8.8.8") == Outcome::kForwarded,
              "rl recovery: must pass after token refill")) return false;

  Passed("TestRateLimitRecoveryIndependentOfCB");
  return true;
}

}  // namespace

int main() {
  int passed = 0;
  int total = 0;

#define RUN(test) do { ++total; if (test()) ++passed; } while (0)

  RUN(TestNormalFlowPassesThrough);
  RUN(TestGlobalRateLimitTriggered);
  RUN(TestPerIPRateLimitTriggered);
  RUN(TestCircuitBreakerOpenReturnsFallback);
  RUN(TestRateLimitFiresBeforeCircuitBreaker);
  RUN(TestCircuitBreakerRecovery);
  RUN(TestHalfOpenProbeFails);
  RUN(TestFallbackPreRenderedContent);
  RUN(TestFullResilienceLifecycle);
  RUN(TestRateLimitRecoveryIndependentOfCB);

  std::cout << "===========================\n";
  std::cout << passed << "/" << total << " tests passed.\n";

  return (passed == total) ? 0 : 1;
}
