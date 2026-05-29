// Smoke test for runtime::metrics primitives and GatewayMetrics aggregator.
// 不依赖 GTest, 用断言风格直接验证.

#include <cassert>
#include <cstdio>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "runtime/metrics/gateway_metrics.h"

namespace m = runtime::metrics;

static void test_counter_basics() {
  m::Counter c;
  assert(c.Value() == 0);
  c.Inc();
  c.Add(9);
  assert(c.Value() == 10);
}

static void test_gauge_signed() {
  m::Gauge g;
  g.Set(5);
  g.Inc();
  g.Dec();
  g.Add(-3);
  assert(g.Value() == 2);

  // Gauge must support going negative (使用 int64_t 语义).
  g.Set(0);
  g.Dec();
  assert(g.Value() == -1);
}

static void test_counter_concurrent() {
  m::Counter c;
  constexpr int kThreads = 8;
  constexpr int kPerThread = 10000;
  std::vector<std::thread> ts;
  ts.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    ts.emplace_back([&c] {
      for (int j = 0; j < kPerThread; ++j) c.Inc();
    });
  }
  for (auto& t : ts) t.join();
  assert(c.Value() == static_cast<std::uint64_t>(kThreads * kPerThread));
}

static void test_histogram_buckets() {
  m::Histogram h("test_latency_seconds", "Test latency");
  h.Observe(0.0005);   // <= 0.001
  h.Observe(0.003);    // <= 0.005
  h.Observe(0.2);      // <= 0.25
  h.Observe(100.0);    // 落在 +Inf

  std::ostringstream oss;
  h.WritePrometheus(oss);
  const auto s = oss.str();

  // 必要的 Prometheus 标记
  assert(s.find("# TYPE test_latency_seconds histogram") != std::string::npos);
  assert(s.find("test_latency_seconds_count 4") != std::string::npos);
  assert(s.find("le=\"+Inf\"} 4") != std::string::npos);
  // 1ms 桶只有 1 条样本
  assert(s.find("le=\"0.001\"} 1") != std::string::npos);
}

static void test_gateway_metrics_status_buckets() {
  m::GatewayMetrics gm;
  gm.ObserveStatus(200);
  gm.ObserveStatus(204);
  gm.ObserveStatus(301);
  gm.ObserveStatus(404);
  gm.ObserveStatus(500);
  gm.ObserveStatus(503);

  assert(gm.requests_total.Value() == 6);
  assert(gm.requests_2xx_total.Value() == 2);
  assert(gm.requests_3xx_total.Value() == 1);
  assert(gm.requests_4xx_total.Value() == 1);
  assert(gm.requests_5xx_total.Value() == 2);
}

static void test_gateway_metrics_render() {
  m::GatewayMetrics gm;
  gm.connections_accepted_total.Add(3);
  gm.connections_active.Set(2);
  gm.rate_limited_global_total.Inc();
  gm.upstream_duration.Observe(0.012);

  const std::string text = gm.Render();
  assert(text.find("# TYPE gateway_connections_accepted_total counter") !=
         std::string::npos);
  assert(text.find("gateway_connections_accepted_total 3") != std::string::npos);
  assert(text.find("gateway_connections_active 2") != std::string::npos);
  assert(text.find("gateway_rate_limited_global_total 1") != std::string::npos);
  assert(text.find("gateway_upstream_duration_seconds_count 1") !=
         std::string::npos);
}

int main() {
  test_counter_basics();
  test_gauge_signed();
  test_counter_concurrent();
  test_histogram_buckets();
  test_gateway_metrics_status_buckets();
  test_gateway_metrics_render();
  std::puts("[gateway_metrics_smoke] ok");
  return 0;
}
