// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/metrics/metrics.h"

#include <cstdint>
#include <ostream>
#include <sstream>
#include <string>

namespace runtime::metrics {

// 网关运行时指标. 单例由 GatewayServer 持有, 在 IO 线程的热路径上调用.
//
// 设计要点:
//   - 全部底层原子操作, 不引入新的锁竞争点.
//   - 字段按事件源分组, 命名遵循 Prometheus 推荐:
//       <namespace>_<subsystem>_<name>_<unit>
//     namespace 固定为 "gateway".
//   - 直方图桶继承 Histogram 的默认值 (1ms ~ 60s).
class GatewayMetrics : public runtime::base::NonCopyable {
public:
  GatewayMetrics()
      : request_duration("gateway_request_duration_seconds",
                        "End-to-end request handling latency in seconds"),
        upstream_duration("gateway_upstream_duration_seconds",
                         "Upstream round-trip latency in seconds") {}

  // 连接维度
  Counter connections_accepted_total;
  Counter connections_closed_total;
  Gauge   connections_active;

  // 请求维度 (网关入口侧)
  Counter requests_total;
  Counter requests_2xx_total;
  Counter requests_3xx_total;
  Counter requests_4xx_total;
  Counter requests_5xx_total;
  Counter requests_malformed_total;   // 解析失败, 返回 400
  Counter requests_not_found_total;   // 路由未命中, 返回 404
  Histogram request_duration;

  // 路由分发
  Counter routes_direct_total;
  Counter routes_proxy_total;

  // 反向代理 / 上游
  Counter upstream_requests_total;
  Counter upstream_errors_total;       // 连接失败, 解析失败, 超时等
  Counter upstream_no_peer_total;      // LB 选不出可用 peer, 走降级
  Histogram upstream_duration;

  // 韧性策略
  Counter rate_limited_global_total;
  Counter rate_limited_per_ip_total;
  Counter circuit_breaker_rejected_total;
  Counter fallback_served_total;       // 渲染降级响应 (含 503 兜底)

  // 健康检查
  Counter health_check_runs_total;
  Counter health_check_failures_total;
  Gauge   upstream_peers_healthy;
  Gauge   upstream_peers_total;

  // 按 HTTP 状态码桶累加; 调用方一次请求只调一次.
  void ObserveStatus(int status_code) {
    requests_total.Inc();
    if (status_code >= 200 && status_code < 300) {
      requests_2xx_total.Inc();
    } else if (status_code >= 300 && status_code < 400) {
      requests_3xx_total.Inc();
    } else if (status_code >= 400 && status_code < 500) {
      requests_4xx_total.Inc();
    } else if (status_code >= 500 && status_code < 600) {
      requests_5xx_total.Inc();
    }
  }

  // 渲染整个网关指标集为 Prometheus 文本格式
  void WritePrometheus(std::ostream& os) const {
    // 连接
    WriteCounter(os, "gateway_connections_accepted_total",
                 "Accepted TCP connections", connections_accepted_total);
    WriteCounter(os, "gateway_connections_closed_total",
                 "Closed TCP connections", connections_closed_total);
    WriteGauge(os, "gateway_connections_active",
               "Currently open TCP connections", connections_active);

    // 请求
    WriteCounter(os, "gateway_requests_total",
                 "Total HTTP requests handled", requests_total);
    WriteCounter(os, "gateway_requests_2xx_total",
                 "HTTP 2xx responses", requests_2xx_total);
    WriteCounter(os, "gateway_requests_3xx_total",
                 "HTTP 3xx responses", requests_3xx_total);
    WriteCounter(os, "gateway_requests_4xx_total",
                 "HTTP 4xx responses", requests_4xx_total);
    WriteCounter(os, "gateway_requests_5xx_total",
                 "HTTP 5xx responses", requests_5xx_total);
    WriteCounter(os, "gateway_requests_malformed_total",
                 "Requests rejected due to parse failure",
                 requests_malformed_total);
    WriteCounter(os, "gateway_requests_not_found_total",
                 "Requests with no matching route",
                 requests_not_found_total);
    request_duration.WritePrometheus(os);

    // 路由
    WriteCounter(os, "gateway_routes_direct_total",
                 "Requests served by a direct handler", routes_direct_total);
    WriteCounter(os, "gateway_routes_proxy_total",
                 "Requests served by reverse proxy", routes_proxy_total);

    // 上游
    WriteCounter(os, "gateway_upstream_requests_total",
                 "Requests dispatched to an upstream peer",
                 upstream_requests_total);
    WriteCounter(os, "gateway_upstream_errors_total",
                 "Upstream connection / parsing / timeout failures",
                 upstream_errors_total);
    WriteCounter(os, "gateway_upstream_no_peer_total",
                 "Load balancer returned no available peer",
                 upstream_no_peer_total);
    upstream_duration.WritePrometheus(os);

    // 韧性
    WriteCounter(os, "gateway_rate_limited_global_total",
                 "Requests rejected by the global token bucket",
                 rate_limited_global_total);
    WriteCounter(os, "gateway_rate_limited_per_ip_total",
                 "Requests rejected by per-IP rate limiting",
                 rate_limited_per_ip_total);
    WriteCounter(os, "gateway_circuit_breaker_rejected_total",
                 "Requests short-circuited by an open breaker",
                 circuit_breaker_rejected_total);
    WriteCounter(os, "gateway_fallback_served_total",
                 "Responses served from the fallback path",
                 fallback_served_total);

    // 健康检查
    WriteCounter(os, "gateway_health_check_runs_total",
                 "Number of active health check probes executed",
                 health_check_runs_total);
    WriteCounter(os, "gateway_health_check_failures_total",
                 "Active health checks that reported failure",
                 health_check_failures_total);
    WriteGauge(os, "gateway_upstream_peers_healthy",
               "Currently healthy upstream peers", upstream_peers_healthy);
    WriteGauge(os, "gateway_upstream_peers_total",
               "Total registered upstream peers", upstream_peers_total);
  }

  // 便捷形式: 返回 Prometheus 文本, 供 /metrics handler 直接写回
  std::string Render() const {
    std::ostringstream oss;
    WritePrometheus(oss);
    return oss.str();
  }
};

}  // namespace runtime::metrics
