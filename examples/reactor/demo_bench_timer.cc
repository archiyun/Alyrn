// examples/demo_bench_timer.cc
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

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

#include "coropact/reactor/event_loop.h"
#include "coropact/time/timer_id.h"

using clk = std::chrono::steady_clock;
using ns_t = std::chrono::nanoseconds;

namespace {

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

  std::thread loop_thr([N]() {
    coropact::reactor::EventLoop loop;

    // --- 1) AddTimer 速率(远期 timer,不会立刻触发) ---
    std::vector<coropact::time::TimerId> ids;
    ids.reserve(N);
    const auto add_start = clk::now();
    for (int i = 0; i < N; ++i) {
      // 60s 之后到期,确保不会在 bench 期间触发
      ids.push_back(loop.RunAfter(60.0 + i * 1e-6, []() {}));
    }
    const auto add_end = clk::now();
    const long long add_ns = std::chrono::duration_cast<ns_t>(add_end - add_start).count();
    ReportNsPerOp("AddTimer", N, add_ns);

    // --- 2) Cancel 速率(按插入顺序取消) ---
    const auto cancel_start = clk::now();
    for (auto& id : ids) {
      loop.Cancel(id);
    }
    const auto cancel_end = clk::now();
    const long long cancel_ns =
        std::chrono::duration_cast<ns_t>(cancel_end - cancel_start).count();
    ReportNsPerOp("Cancel", N, cancel_ns);

    // --- 3) Expire 处理速率:N 个 timer 几乎同时到期 ---
    int fired = 0;
    const auto short_add_start = clk::now();
    for (int i = 0; i < N; ++i) {
      // 全部 5ms 后到期(同一窗口),小扰动避免完全相同的 key
      loop.RunAfter(0.005 + i * 1e-9, [&fired, &loop, N]() {
        ++fired;
        if (fired == N) {
          loop.Quit();
        }
      });
    }
    const auto short_add_end = clk::now();
    const long long add_short_ns =
        std::chrono::duration_cast<ns_t>(short_add_end - short_add_start).count();
    ReportNsPerOp("AddTimer(short)", N, add_short_ns);

    // The loop is owner-local: start polling only after the benchmark setup
    // has completed, and stop from the final timer callback.
    const auto t_expire_start = clk::now();
    loop.RunAfter(3.0, [&] {
      std::printf("  ! timeout: fired=%d/%d\n", fired, N);
      loop.Quit();
    });
    loop.Loop();
    const auto t_expire_end = clk::now();
    const long long expire_ns =
        std::chrono::duration_cast<ns_t>(t_expire_end - t_expire_start).count();
    // 减去 5ms 等到期的时间,粗略剩处理本身
    long long processing_ns = expire_ns - 5'000'000;
    if (processing_ns < 0) processing_ns = expire_ns;
    std::printf("%-18s N=%-6d total=%9lld ns (processing≈%lld ns)  ns/op=%.1f\n",
                "Expire", N, expire_ns, processing_ns,
                static_cast<double>(processing_ns) / N);
  });
  loop_thr.join();
  return 0;
}
