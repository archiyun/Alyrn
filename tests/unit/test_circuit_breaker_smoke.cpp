// CircuitBreaker 熔断器冒烟测试
//
// 编译
// cmake --build build-tests --target circuit_breaker_smoke_test -j$(nproc)
// 运行
// ./build-tests/tests/circuit_breaker_smoke_test
// 或 cd build-tests && ctest -R circuit_breaker --output-on-failure

#include "runtime/gateway/circuit_breaker.h"

#include <chrono>
#include <iostream>
#include <thread>

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
// 基础状态测试
// ================================================================

bool TestInitialStateIsClosed() {
  runtime::gateway::CircuitBreakerConfig cfg;
  runtime::gateway::CircuitBreaker cb(cfg);
  if (!Expect(cb.State() == runtime::gateway::CircuitBreakerState::kClosed,
              "initial state must be kClosed")) return false;
  Passed("TestInitialStateIsClosed");
  return true;
}

bool TestAllowRequestReturnsTrueInClosed() {
  runtime::gateway::CircuitBreakerConfig cfg;
  runtime::gateway::CircuitBreaker cb(cfg);
  if (!Expect(cb.AllowRequest(), "AllowRequest must return true in kClosed")) return false;
  // 多次调用应一致
  for (int i = 0; i < 100; i++) {
    if (!Expect(cb.AllowRequest(), "AllowRequest must stay true in kClosed")) return false;
  }
  Passed("TestAllowRequestReturnsTrueInClosed");
  return true;
}

// ================================================================
// CLOSED → OPEN 转换
// ================================================================

bool TestTransitionToOpenAfterFailures() {
  runtime::gateway::CircuitBreakerConfig cfg;
  cfg.failure_threshold = 3;
  runtime::gateway::CircuitBreaker cb(cfg);

  // 2 次失败：还不到阈值
  cb.OnFailure();
  cb.OnFailure();
  if (!Expect(cb.State() == runtime::gateway::CircuitBreakerState::kClosed,
              "must stay kClosed before threshold")) return false;

  // 第 3 次失败 → OPEN
  cb.OnFailure();
  if (!Expect(cb.State() == runtime::gateway::CircuitBreakerState::kOpen,
              "must transition to kOpen after failure threshold")) return false;
  Passed("TestTransitionToOpenAfterFailures");
  return true;
}

bool TestAllowRequestReturnsFalseInOpen() {
  runtime::gateway::CircuitBreakerConfig cfg;
  cfg.failure_threshold = 2;
  runtime::gateway::CircuitBreaker cb(cfg);

  cb.OnFailure();
  cb.OnFailure();  // → OPEN
  if (!Expect(!cb.AllowRequest(),
              "AllowRequest must return false in kOpen")) return false;
  Passed("TestAllowRequestReturnsFalseInOpen");
  return true;
}

// ================================================================
// OPEN → HALF_OPEN 转换（超时后懒触发）
// ================================================================

bool TestTransitionToHalfOpenAfterTimeout() {
  runtime::gateway::CircuitBreakerConfig cfg;
  cfg.failure_threshold = 1;
  cfg.open_timeout = 50ms;  // 极短超时
  runtime::gateway::CircuitBreaker cb(cfg);

  cb.OnFailure();  // → OPEN
  if (!Expect(cb.State() == runtime::gateway::CircuitBreakerState::kOpen,
              "must be kOpen")) return false;

  // 立即请求：还在超时内
  if (!Expect(!cb.AllowRequest(),
              "must reject within open_timeout")) return false;

  // 等待超时
  std::this_thread::sleep_for(60ms);

  // 下一个请求触发 OPEN → HALF_OPEN
  if (!Expect(cb.AllowRequest(),
              "must allow after open_timeout (lazy OPEN→HALF_OPEN)")) return false;
  if (!Expect(cb.State() == runtime::gateway::CircuitBreakerState::kHalfOpen,
              "must be kHalfOpen after timeout")) return false;
  Passed("TestTransitionToHalfOpenAfterTimeout");
  return true;
}

// ================================================================
// HALF_OPEN 限量放行
// ================================================================

bool TestHalfOpenLimitsRequests() {
  runtime::gateway::CircuitBreakerConfig cfg;
  cfg.failure_threshold = 1;
  cfg.half_open_max_requests = 2;
  cfg.open_timeout = 1ms;  // 立即触发
  runtime::gateway::CircuitBreaker cb(cfg);

  cb.OnFailure();  // → OPEN
  std::this_thread::sleep_for(5ms);

  // first request triggers OPEN→HALF_OPEN
  if (!Expect(cb.AllowRequest(), "first probe must be allowed")) return false;
  // second: allowed (max=2)
  if (!Expect(cb.AllowRequest(), "second probe must be allowed")) return false;
  // third: rejected
  if (!Expect(!cb.AllowRequest(), "third probe must be rejected")) return false;
  Passed("TestHalfOpenLimitsRequests");
  return true;
}

// ================================================================
// HALF_OPEN → CLOSED（连续成功达标）
// ================================================================

bool TestTransitionToClosedAfterSuccesses() {
  runtime::gateway::CircuitBreakerConfig cfg;
  cfg.failure_threshold = 1;
  cfg.success_threshold = 2;
  cfg.open_timeout = 1ms;
  runtime::gateway::CircuitBreaker cb(cfg);

  cb.OnFailure();  // → OPEN
  std::this_thread::sleep_for(5ms);
  cb.AllowRequest();  // OPEN → HALF_OPEN

  // 1 次成功 → 不达标
  cb.OnSuccess();
  if (!Expect(cb.State() == runtime::gateway::CircuitBreakerState::kHalfOpen,
              "must stay kHalfOpen after 1/2 successes")) return false;

  // 2 次成功 → CLOSED
  cb.OnSuccess();
  if (!Expect(cb.State() == runtime::gateway::CircuitBreakerState::kClosed,
              "must transition to kClosed after success threshold")) return false;
  Passed("TestTransitionToClosedAfterSuccesses");
  return true;
}

// ================================================================
// HALF_OPEN → OPEN（探测期间任意失败）
// ================================================================

bool TestTransitionBackToOpenOnFailureInHalfOpen() {
  runtime::gateway::CircuitBreakerConfig cfg;
  cfg.failure_threshold = 1;
  cfg.open_timeout = 1ms;
  runtime::gateway::CircuitBreaker cb(cfg);

  cb.OnFailure();  // → OPEN
  std::this_thread::sleep_for(5ms);
  cb.AllowRequest();  // OPEN → HALF_OPEN

  // 探测请求失败 → 立刻回到 OPEN
  cb.OnFailure();
  if (!Expect(cb.State() == runtime::gateway::CircuitBreakerState::kOpen,
              "must go back to kOpen on failure in kHalfOpen")) return false;
  Passed("TestTransitionBackToOpenOnFailureInHalfOpen");
  return true;
}

// ================================================================
// CLOSED: 单次成功不重置失败计数（连续阈值保护）
// ================================================================

bool TestClosedSingleSuccessDoesNotReset() {
  runtime::gateway::CircuitBreakerConfig cfg;
  cfg.failure_threshold = 5;
  cfg.success_threshold = 2;
  runtime::gateway::CircuitBreaker cb(cfg);

  // 4 次失败
  cb.OnFailure();
  cb.OnFailure();
  cb.OnFailure();
  cb.OnFailure();

  // 1 次成功 → 不达标（需要 success_threshold=2）
  cb.OnSuccess();
  // failure_count 应该保持而不是重置
  if (!Expect(cb.State() == runtime::gateway::CircuitBreakerState::kClosed,
              "must stay kClosed after single success")) return false;

  // 第 2 次成功 → 达标，重置 failure_count
  cb.OnSuccess();

  // 再 5 次失败 → 应该需要 5 次（因为计数器已重置）
  cb.OnFailure();
  cb.OnFailure();
  cb.OnFailure();
  cb.OnFailure();
  if (!Expect(cb.State() == runtime::gateway::CircuitBreakerState::kClosed,
              "must stay kClosed at 4/5 failures after reset")) return false;
  cb.OnFailure();  // 第 5 次 → OPEN
  if (!Expect(cb.State() == runtime::gateway::CircuitBreakerState::kOpen,
              "must go kOpen at 5/5 failures after reset")) return false;
  Passed("TestClosedSingleSuccessDoesNotReset");
  return true;
}

// ================================================================
// 状态转换计数
// ================================================================

bool TestTransitionCount() {
  runtime::gateway::CircuitBreakerConfig cfg;
  cfg.failure_threshold = 1;
  cfg.success_threshold = 1;
  cfg.open_timeout = 1ms;
  runtime::gateway::CircuitBreaker cb(cfg);

  if (!Expect(cb.TransitionCount() == 0,
              "transition_count must start at 0")) return false;

  cb.OnFailure();  // CLOSED → OPEN (1)
  if (!Expect(cb.TransitionCount() == 1,
              "must be 1 after CLOSED→OPEN")) return false;

  std::this_thread::sleep_for(5ms);
  cb.AllowRequest();  // OPEN → HALF_OPEN (2)
  if (!Expect(cb.TransitionCount() == 2,
              "must be 2 after OPEN→HALF_OPEN")) return false;

  cb.OnSuccess();  // HALF_OPEN → CLOSED (3)
  if (!Expect(cb.TransitionCount() == 3,
              "must be 3 after HALF_OPEN→CLOSED")) return false;
  Passed("TestTransitionCount");
  return true;
}

// ================================================================
// 完整周期
// ================================================================

bool TestFullCycle() {
  runtime::gateway::CircuitBreakerConfig cfg;
  cfg.failure_threshold = 2;
  cfg.success_threshold = 1;
  cfg.open_timeout = 50ms;
  runtime::gateway::CircuitBreaker cb(cfg);

  // 1. CLOSED → OPEN
  cb.OnFailure();
  cb.OnFailure();
  if (!Expect(cb.State() == runtime::gateway::CircuitBreakerState::kOpen,
              "step 1: must be kOpen")) return false;

  // 2. Rejected during OPEN
  if (!Expect(!cb.AllowRequest(), "step 2: must reject in kOpen")) return false;

  // 3. OPEN → HALF_OPEN (after timeout)
  std::this_thread::sleep_for(60ms);
  if (!Expect(cb.AllowRequest(), "step 3: must allow (lazy HALF_OPEN)")) return false;

  // 4. HALF_OPEN → CLOSED (success)
  cb.OnSuccess();
  if (!Expect(cb.State() == runtime::gateway::CircuitBreakerState::kClosed,
              "step 4: must be kClosed after success")) return false;

  // 5. CLOSED again, can allow
  if (!Expect(cb.AllowRequest(), "step 5: must allow in kClosed")) return false;
  Passed("TestFullCycle");
  return true;
}

// ================================================================
// Open 状态下超时 CAS 竞争（多线程）
// ================================================================

bool TestConcurrentAllowRequestInOpen() {
  runtime::gateway::CircuitBreakerConfig cfg;
  cfg.failure_threshold = 1;
  cfg.open_timeout = 1ms;
  cfg.half_open_max_requests = 1;
  runtime::gateway::CircuitBreaker cb(cfg);

  cb.OnFailure();  // → OPEN
  std::this_thread::sleep_for(5ms);

  // 多个线程同时调 AllowRequest，只有一个该拿到探测令牌
  std::atomic<int> allowed{0};
  std::thread t1([&] { if (cb.AllowRequest()) allowed.fetch_add(1); });
  std::thread t2([&] { if (cb.AllowRequest()) allowed.fetch_add(1); });
  std::thread t3([&] { if (cb.AllowRequest()) allowed.fetch_add(1); });
  t1.join(); t2.join(); t3.join();

  if (!Expect(allowed.load() == 1,
              "exactly 1 thread must get the HALF_OPEN probe token")) return false;
  Passed("TestConcurrentAllowRequestInOpen");
  return true;
}

// ================================================================
// FailureCount 暴露
// ================================================================

bool TestFailureCountExposed() {
  runtime::gateway::CircuitBreakerConfig cfg;
  cfg.failure_threshold = 10;
  runtime::gateway::CircuitBreaker cb(cfg);

  if (!Expect(cb.FailureCount() == 0, "failure_count must start at 0")) return false;
  cb.OnFailure();
  cb.OnFailure();
  if (!Expect(cb.FailureCount() == 2, "failure_count must be 2")) return false;
  Passed("TestFailureCountExposed");
  return true;
}

}  // namespace

int main() {
  int passed = 0;
  int total = 0;

#define RUN(test) do { ++total; if (test()) ++passed; } while (0)

  RUN(TestInitialStateIsClosed);
  RUN(TestAllowRequestReturnsTrueInClosed);
  RUN(TestTransitionToOpenAfterFailures);
  RUN(TestAllowRequestReturnsFalseInOpen);
  RUN(TestTransitionToHalfOpenAfterTimeout);
  RUN(TestHalfOpenLimitsRequests);
  RUN(TestTransitionToClosedAfterSuccesses);
  RUN(TestTransitionBackToOpenOnFailureInHalfOpen);
  RUN(TestClosedSingleSuccessDoesNotReset);
  RUN(TestTransitionCount);
  RUN(TestFullCycle);
  RUN(TestConcurrentAllowRequestInOpen);
  RUN(TestFailureCountExposed);

  std::cout << "===========================\n";
  std::cout << passed << "/" << total << " tests passed.\n";

  return (passed == total) ? 0 : 1;
}
