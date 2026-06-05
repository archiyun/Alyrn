// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "runtime/base/noncopyable.h"
#include "runtime/gateway/fallback_config.h"
#include "runtime/gateway/health_checker.h"
#include "runtime/gateway/load_balancer.h"
#include "runtime/gateway/proxy_pass.h"
#include "runtime/gateway/rate_limiter.h"
#include "runtime/gateway/upstream_conn_pool.h"
#include "runtime/gateway/upstream_registry.h"
#include "runtime/http/http_context.h"
#include "runtime/http/http_response.h"
#include "runtime/http/http_types.h"
#include "runtime/http/router.h"
#include "runtime/metrics/gateway_metrics.h"
#include "runtime/net/inet_address.h"
#include "runtime/net/tcp_server.h"
#include "runtime/time/timestamp.h"

namespace runtime::gateway {

// GatewayServer wraps TcpServer and dispatches incoming HTTP requests to either
// synchronous direct handlers or asynchronous upstream proxy forwarding.
class GatewayServer : public runtime::base::NonCopyable {
public:
  using Handler = runtime::http::Handler;
  using TcpConnectionPtr = runtime::net::TcpConnection::TcpConnectionPtr;

  // Route category used by the unified routing table.
  enum class RouteType : uint8_t {
    Direct,
    Proxy,
  };

  // Path matching mode.
  enum class MatchType : uint8_t {
    Exact,
    Prefix,
  };
  struct Route {
    RouteType type;
    MatchType match_type{MatchType::Exact};

    runtime::http::Method method;
    bool match_all_methods{false};

    std::string path;

    Handler handler;  // Direct route handler.
    std::string upstream_name;  // Proxy upstream name.
    std::unique_ptr<LoadBalancer> lb;  // Proxy load balancer.

    FallbackConfig fallback;
    bool circuit_breaker_enabled{false};
  };

  GatewayServer(runtime::net::EventLoop* loop,
                const runtime::net::InetAddress& addr,
                std::string name,
                UpstreamRegistry& registry);
  void set_thread_num(int num_threads);

  // Register synchronous direct routes.
  void Get(std::string_view path, Handler handler);
  void Post(std::string_view path, Handler handler);

  void AddProxyRoute(std::string_view path,
                     std::string_view upstream_name,
                     std::string_view algo = "p2c");
  // Register a proxy route with fallback and optional circuit breaker support.
  void AddProxyRoute(std::string_view path,
                     std::string_view upstream_name,
                     FallbackConfig fallback,
                     bool circuit_breaker_enabled = false,
                     std::string_view algo = "p2c");
  void EnableHealthCheck(HealthCheckConfig cfg = {});

  // Enable request rate limiting.
  void EnableGlobalRateLimit(double rate, double burst);
  void EnablePerIPRateLimit(double rate, double burst);
  void set_pool_config(PoolConfig cfg) { pool_cfg_ = cfg; }

  // Register a GET direct route that renders GatewayMetrics in Prometheus text
  // format. Call before Start(); the default path is "/metrics".
  void EnableMetricsEndpoint(std::string_view path = "/metrics");

  // Expose metrics for external instrumentation, tests, and custom exporters.
  runtime::metrics::GatewayMetrics&       metrics()       { return metrics_; }
  const runtime::metrics::GatewayMetrics& metrics() const { return metrics_; }

  const Route* MatchRoute(std::string_view path) const;
  void Start();
private:

  // Per-connection context stored in TcpConnection::context_.
  struct ConnCtx {
    runtime::http::HttpContext http_ctx;  // Incremental HTTP parser state.
    // Current upstream request; null while handling direct routes.
    std::shared_ptr<UpstreamRequest> upstream_req;
  };


  void OnConnection(const TcpConnectionPtr& conn);
  void OnMessage(const TcpConnectionPtr& conn,
                 runtime::net::Buffer& buf,
                 runtime::time::Timestamp ts);
  UpstreamConnPool& GetOrCreatePool(runtime::net::EventLoop* loop);
  runtime::http::HttpResponse MakeError(runtime::http::StatusCode code,
                                        std::string_view msg) const;
  std::string RenderFallback(const Route& route,
                             std::string_view reason) const;
private:
  runtime::net::TcpServer server_;
  UpstreamRegistry& registry_;
  std::vector<Route> routes_;
  std::unique_ptr<HealthChecker> health_checker_;
  PoolConfig pool_cfg_;
  // Shared across sub-loops: each sub-loop calls GetOrCreatePool().
  // Only the map structure requires locking; each UpstreamConnPool value is
  // accessed by its owning loop thread. unordered_map references remain valid
  // across insertions, so the lock can be released after obtaining the pool.
  mutable std::mutex pools_mu_;
  std::unordered_map<runtime::net::EventLoop*, UpstreamConnPool> pools_;
  std::unique_ptr<RateLimiter> rate_limiter_;
  std::string rate_limit_response_429_;  // Pre-rendered 429 response.
  RateLimiterConfig rate_limiter_cfg_;  // Accumulated by Enable* calls.
  runtime::metrics::GatewayMetrics metrics_;  // Runtime counters and gauges.
};

}  // namespace runtime::gateway
