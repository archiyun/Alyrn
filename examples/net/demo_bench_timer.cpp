// examples/demo_bench_timer.cpp
//
// 微基准:测 TimerQueue 在 in-loop 调用下的 Add / Cancel / Expire 三个操作的吞吐。
// 当前实现是 timerfd + 侵入式红黑树,理论 O(log N)。
//
// 重要前提:TimerQueue 内部用 ObjectPool<Timer, 512>,N>512 时 Acquire 返回 nullptr,
//          Release 构建下 deref nullptr → SEGV。本 bench 默认上限 N=500。
//
// 用法:
//   ./build-perf/examples/demo_bench_timer [N]
//
// 编译:
//   cmake --build build-perf --target demo_bench_timer -j$(nproc)

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <thread>
#include <vector>

#include "runtime/net/event_loop.h"
#include "runtime/net/timer_id.h"

using clk = std::chrono::steady_clock;
using ns_t = std::chrono::nanoseconds;

namespace {

void RunInLoopSync(runtime::net::EventLoop& loop, std::function<void()> fn) {
  std::mutex mu;
  std::condition_variable cv;
  bool done = false;
  loop.RunInLoop([&]() {
    fn();
    std::lock_guard<std::mutex> lock(mu);
    done = true;
    cv.notify_one();
  });
  std::unique_lock<std::mutex> lock(mu);
  cv.wait(lock, [&]() { return done; });
}

void ReportNsPerOp(const char* tag, int N, long long total_ns) {
  const double ns_per_op = static_cast<double>(total_ns) / N;
  const double mops = 1000.0 / ns_per_op;
  std::printf("%-18s N=%-6d total=%9lld ns  ns/op=%8.1f  rate=%6.2f Mop/s\n",
              tag, N, total_ns, ns_per_op, mops);
}

}  // namespace

int main(int argc, char** argv) {
  int N = (argc > 1) ? std::atoi(argv[1]) : 500;

  std::printf("=== TimerQueue bench  N=%d  (pool cap=512) ===\n\n", N);

  runtime::net::EventLoop loop;
  std::thread loop_thr([&loop]() { loop.Loop(); });

  // 等 loop 起来
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // --- 1) AddTimer 速率(远期 timer,不会立刻触发) ---
  std::vector<runtime::net::TimerId> ids;
  ids.reserve(N);
  long long add_ns = 0;
  RunInLoopSync(loop, [&]() {
    auto t0 = clk::now();
    for (int i = 0; i < N; ++i) {
      // 60s 之后到期,确保不会在 bench 期间触发
      ids.push_back(loop.RunAfter(60.0 + i * 1e-6, []() {}));
    }
    auto t1 = clk::now();
    add_ns = std::chrono::duration_cast<ns_t>(t1 - t0).count();
  });
  ReportNsPerOp("AddTimer", N, add_ns);

  // --- 2) Cancel 速率(按插入顺序取消) ---
  long long cancel_ns = 0;
  RunInLoopSync(loop, [&]() {
    auto t0 = clk::now();
    for (auto& id : ids) {
      loop.Cancel(id);
    }
    auto t1 = clk::now();
    cancel_ns = std::chrono::duration_cast<ns_t>(t1 - t0).count();
  });
  ReportNsPerOp("Cancel", N, cancel_ns);

  // --- 3) Expire 处理速率:N 个 timer 几乎同时到期 ---
  std::atomic<int> fired{0};
  long long add_short_ns = 0;
  RunInLoopSync(loop, [&]() {
    auto t0 = clk::now();
    for (int i = 0; i < N; ++i) {
      // 全部 5ms 后到期(同一窗口),小扰动避免完全相同的 key
      loop.RunAfter(0.005 + i * 1e-9, [&fired]() {
        fired.fetch_add(1, std::memory_order_relaxed);
      });
    }
    auto t1 = clk::now();
    add_short_ns = std::chrono::duration_cast<ns_t>(t1 - t0).count();
  });
  ReportNsPerOp("AddTimer(short)", N, add_short_ns);

  // 等所有 timer 触发
  auto t_expire_start = clk::now();
  while (fired.load(std::memory_order_relaxed) < N) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
    if (clk::now() - t_expire_start > std::chrono::seconds(3)) {
      std::printf("  ! timeout: fired=%d/%d\n", fired.load(), N);
      break;
    }
  }
  auto t_expire_end = clk::now();
  long long expire_ns = std::chrono::duration_cast<ns_t>(t_expire_end - t_expire_start).count();
  // 减去 5ms 等到期的时间,粗略剩处理本身
  long long processing_ns = expire_ns - 5'000'000;
  if (processing_ns < 0) processing_ns = expire_ns;
  std::printf("%-18s N=%-6d total=%9lld ns (processing≈%lld ns)  ns/op=%.1f\n",
              "Expire", N, expire_ns, processing_ns,
              static_cast<double>(processing_ns) / N);

  loop.Quit();
  loop_thr.join();
  return 0;
}
