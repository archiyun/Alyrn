// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "vexo/gateway/fallback_config.h"
#include "vexo/gateway/health_check_config.h"
#include "vexo/gateway/rate_limiter.h"
#include "vexo/gateway/upstream.h"
#include "vexo/gateway/upstream_peer.h"
#include "vexo/http/http_request.h"
#include "vexo/http/http_response.h"
#include "vexo/http/http_types.h"

namespace vexo::gateway {

class UpstreamRegistry;

class GatewayConfigError : public std::runtime_error {
public:
  explicit GatewayConfigError(const std::string& what_arg) : std::runtime_error(what_arg) {}
};

struct GatewayServerConfig {
  std::string name{"gateway"};
  std::string host{"127.0.0.1"};
  std::uint16_t port{8080};
  int threads{0};
};

struct GatewayStatusEndpointConfig {
  bool enabled{false};
  std::string path{"/healthz"};
  std::string content_type{"application/json; charset=utf-8"};
  std::string body{R"({"status":"ok"})"};
};

struct GatewayMetricsConfig {
  bool enabled{false};
  std::string path{"/metrics"};
};

struct GatewayHealthCheckConfig {
  bool enabled{false};
  HealthCheckConfig config{};
};

struct GatewayRateLimitBucketConfig {
  bool enabled{false};
  double rate{1000.0};
  double burst{2000.0};
  std::size_t max_buckets{65536};
};

struct GatewayRateLimitConfig {
  GatewayRateLimitBucketConfig global{};
  GatewayRateLimitBucketConfig per_ip{.rate = 10.0, .burst = 20.0};
};

struct GatewayUpstreamConfig {
  UpstreamConfig config{};
  std::vector<UpstreamPeerConfig> peers;
};

struct GatewayRouteConfig {
  std::string path;
  std::string upstream_name;
  std::string match{"prefix"};
  std::string load_balance{"p2c"};
  bool circuit_breaker_enabled{false};
  FallbackConfig fallback{};
};

struct GatewayConfig {
  GatewayServerConfig server{};
  GatewayStatusEndpointConfig status_endpoint{};
  GatewayMetricsConfig metrics{};
  GatewayHealthCheckConfig health_check{};
  GatewayRateLimitConfig rate_limit{};
  std::vector<GatewayUpstreamConfig> upstreams;
  std::vector<GatewayRouteConfig> routes;
};

GatewayConfig LoadGatewayConfigFromYaml(std::string_view path);

void ValidateGatewayConfig(const GatewayConfig& config);
void BuildGatewayUpstreamRegistry(const GatewayConfig& config, UpstreamRegistry& registry);

template <class Gateway>
void ApplyGatewayConfig(const GatewayConfig& config, Gateway& gateway) {
  if (config.server.threads > 0) {
    gateway.set_thread_num(config.server.threads);
  }

  if (config.status_endpoint.enabled) {
    const GatewayStatusEndpointConfig endpoint = config.status_endpoint;
    gateway.Get(endpoint.path,
                [endpoint](const vexo::http::HttpRequest&, vexo::http::HttpResponse& resp) {
                  resp.set_status_code(vexo::http::StatusCode::Ok);
                  resp.set_content_type(endpoint.content_type);
                  resp.set_body(endpoint.body);
                });
  }

  if (config.metrics.enabled) {
    gateway.EnableMetricsEndpoint(config.metrics.path);
  }

  if (config.rate_limit.global.enabled || config.rate_limit.per_ip.enabled) {
    RateLimiterConfig limiter;
    limiter.global_enabled = config.rate_limit.global.enabled;
    limiter.global_rate = config.rate_limit.global.rate;
    limiter.global_burst = config.rate_limit.global.burst;
    limiter.per_ip_enabled = config.rate_limit.per_ip.enabled;
    limiter.per_ip_rate = config.rate_limit.per_ip.rate;
    limiter.per_ip_burst = config.rate_limit.per_ip.burst;
    limiter.per_ip_max_buckets = config.rate_limit.per_ip.max_buckets;
    gateway.EnableRateLimit(limiter);
  }

  for (const GatewayRouteConfig& route : config.routes) {
    gateway.AddProxyRoute(route.path, route.upstream_name, route.fallback,
                          route.circuit_breaker_enabled, route.load_balance);
  }

  if (config.health_check.enabled) {
    gateway.EnableHealthCheck(config.health_check.config);
  }
}

}  // namespace vexo::gateway
