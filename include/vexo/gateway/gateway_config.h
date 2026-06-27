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
#include "vexo/gateway/upstream.h"
#include "vexo/gateway/upstream_peer.h"
#include "vexo/net/inet_address.h"

namespace vexo::gateway {

class GatewayServer;
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

vexo::net::InetAddress MakeGatewayListenAddress(const GatewayConfig& config);

void ValidateGatewayConfig(const GatewayConfig& config);
void BuildGatewayUpstreamRegistry(const GatewayConfig& config, UpstreamRegistry& registry);
void ApplyGatewayConfig(const GatewayConfig& config, GatewayServer& gateway);

}  // namespace vexo::gateway
