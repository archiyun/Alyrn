// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include <yaml-cpp/yaml.h>

#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>

#include "coropact/gateway/gateway_config.h"
#include "coropact/gateway/gateway_server.h"
#include "coropact/gateway/load_balancer.h"
#include "coropact/gateway/upstream_registry.h"
#include "coropact/http/http_request.h"
#include "coropact/http/http_response.h"
#include "coropact/net/net_utils.h"

namespace coropact::gateway {
namespace {

std::string ToString(std::string_view value) { return std::string(value); }

std::string JoinPath(std::string_view base, std::string_view key) {
  if (base.empty()) return ToString(key);
  return ToString(base) + "." + ToString(key);
}

[[noreturn]] void Fail(std::string_view ctx, std::string_view msg) {
  throw GatewayConfigError(ToString(ctx) + ": " + ToString(msg));
}

void RequireMap(const YAML::Node& node, std::string_view ctx) {
  if (!node || !node.IsMap()) Fail(ctx, "expected a map");
}

void RequireSequence(const YAML::Node& node, std::string_view ctx) {
  if (!node || !node.IsSequence()) Fail(ctx, "expected a sequence");
}

YAML::Node Field(const YAML::Node& node, std::string_view key) { return node[ToString(key)]; }

template <typename T>
T As(const YAML::Node& node, std::string_view ctx) {
  if (!node) Fail(ctx, "missing required value");
  try {
    return node.as<T>();
  } catch (const YAML::Exception& ex) {
    throw GatewayConfigError(ToString(ctx) + ": " + ex.what());
  }
}

std::string ReadRequiredString(const YAML::Node& node, std::string_view key, std::string_view ctx) {
  return As<std::string>(Field(node, key), JoinPath(ctx, key));
}

void ReadOptionalString(const YAML::Node& node, std::string_view key, std::string_view ctx,
                        std::string* out) {
  YAML::Node field = Field(node, key);
  if (!field) return;
  *out = As<std::string>(field, JoinPath(ctx, key));
}

void ReadOptionalBool(const YAML::Node& node, std::string_view key, std::string_view ctx,
                      bool* out) {
  YAML::Node field = Field(node, key);
  if (!field) return;
  *out = As<bool>(field, JoinPath(ctx, key));
}

long long ReadInteger(const YAML::Node& node, std::string_view ctx, long long min_value,
                      long long max_value) {
  const long long value = As<long long>(node, ctx);
  if (value < min_value || value > max_value) {
    Fail(ctx, "value out of range");
  }
  return value;
}

void ReadOptionalInt(const YAML::Node& node, std::string_view key, std::string_view ctx, int* out,
                     int min_value, int max_value) {
  YAML::Node field = Field(node, key);
  if (!field) return;
  *out = static_cast<int>(ReadInteger(field, JoinPath(ctx, key), min_value, max_value));
}

void ReadOptionalSize(const YAML::Node& node, std::string_view key, std::string_view ctx,
                      std::size_t* out) {
  YAML::Node field = Field(node, key);
  if (!field) return;
  *out = static_cast<std::size_t>(
      ReadInteger(field, JoinPath(ctx, key), 0, std::numeric_limits<long long>::max()));
}

void ReadOptionalDouble(const YAML::Node& node, std::string_view key, std::string_view ctx,
                        double* out) {
  YAML::Node field = Field(node, key);
  if (!field) return;
  *out = As<double>(field, JoinPath(ctx, key));
}

std::uint16_t ReadPort(const YAML::Node& node, std::string_view ctx) {
  return static_cast<std::uint16_t>(ReadInteger(node, ctx, 1, 65535));
}

std::chrono::milliseconds ReadMilliseconds(const YAML::Node& node, std::string_view ctx,
                                           long long min_value = 0) {
  return std::chrono::milliseconds(
      ReadInteger(node, ctx, min_value, std::numeric_limits<int>::max()));
}

coropact::http::StatusCode ParseStatusCode(int code, std::string_view ctx) {
  using coropact::http::StatusCode;
  switch (code) {
    case 400:
      return StatusCode::BadRequest;
    case 401:
      return StatusCode::Unauthorized;
    case 403:
      return StatusCode::Forbidden;
    case 404:
      return StatusCode::NotFound;
    case 405:
      return StatusCode::MethodNotAllowed;
    case 408:
      return StatusCode::RequestTimeout;
    case 413:
      return StatusCode::PayloadTooLarge;
    case 414:
      return StatusCode::UriTooLong;
    case 415:
      return StatusCode::UnsupportedMediaType;
    case 429:
      return StatusCode::TooManyRequests;
    case 431:
      return StatusCode::RequestHeaderFieldsTooLarge;
    case 500:
      return StatusCode::InternalServerError;
    case 501:
      return StatusCode::NotImplemented;
    case 502:
      return StatusCode::BadGateway;
    case 503:
      return StatusCode::ServiceUnavailable;
    case 504:
      return StatusCode::GatewayTimeout;
    case 505:
      return StatusCode::HttpVersionNotSupported;
    default:
      Fail(ctx, "unsupported HTTP status code");
  }
}

void ParseListenScalar(std::string_view value, GatewayServerConfig* server, std::string_view ctx) {
  const std::size_t colon = value.rfind(':');
  if (colon == std::string_view::npos) {
    const long long port = ReadInteger(YAML::Load(ToString(value)), ctx, 1, 65535);
    server->port = static_cast<std::uint16_t>(port);
    return;
  }

  if (value.find(':') != colon) {
    Fail(ctx, "IPv6 listen addresses are not supported yet");
  }

  const std::string_view host = value.substr(0, colon);
  const std::string_view port = value.substr(colon + 1);
  if (host.empty() || port.empty()) {
    Fail(ctx, "expected host:port");
  }

  server->host = ToString(host);
  const long long parsed_port = ReadInteger(YAML::Load(ToString(port)), ctx, 1, 65535);
  server->port = static_cast<std::uint16_t>(parsed_port);
}

void ParseServer(const YAML::Node& root, GatewayConfig* config) {
  YAML::Node node = Field(root, "server");
  if (!node) return;
  RequireMap(node, "server");

  ReadOptionalString(node, "name", "server", &config->server.name);

  YAML::Node listen = Field(node, "listen");
  if (listen) {
    if (listen.IsScalar()) {
      ParseListenScalar(As<std::string>(listen, "server.listen"), &config->server, "server.listen");
    } else if (listen.IsMap()) {
      ReadOptionalString(listen, "host", "server.listen", &config->server.host);
      YAML::Node port = Field(listen, "port");
      if (port) config->server.port = ReadPort(port, "server.listen.port");
    } else {
      Fail("server.listen", "expected a scalar or map");
    }
  }

  ReadOptionalString(node, "host", "server", &config->server.host);
  YAML::Node port = Field(node, "port");
  if (port) config->server.port = ReadPort(port, "server.port");
  ReadOptionalInt(node, "threads", "server", &config->server.threads, 0, 1);
}

void ParseStatusEndpoint(const YAML::Node& root, GatewayConfig* config) {
  YAML::Node node = Field(root, "status_endpoint");
  if (!node) return;
  if (node.IsScalar()) {
    config->status_endpoint.enabled = As<bool>(node, "status_endpoint");
    return;
  }

  RequireMap(node, "status_endpoint");
  config->status_endpoint.enabled = true;
  ReadOptionalBool(node, "enabled", "status_endpoint", &config->status_endpoint.enabled);
  ReadOptionalString(node, "path", "status_endpoint", &config->status_endpoint.path);
  ReadOptionalString(node, "content_type", "status_endpoint",
                     &config->status_endpoint.content_type);
  ReadOptionalString(node, "body", "status_endpoint", &config->status_endpoint.body);
}

void ParseHealthCheck(const YAML::Node& root, GatewayConfig* config) {
  YAML::Node node = Field(root, "health_check");
  if (!node) return;
  if (node.IsScalar()) {
    config->health_check.enabled = As<bool>(node, "health_check");
    return;
  }

  RequireMap(node, "health_check");
  config->health_check.enabled = true;
  ReadOptionalBool(node, "enabled", "health_check", &config->health_check.enabled);
  ReadOptionalString(node, "path", "health_check", &config->health_check.config.path);
  ReadOptionalDouble(node, "interval_sec", "health_check",
                     &config->health_check.config.interval_sec);
  ReadOptionalDouble(node, "timeout_sec", "health_check", &config->health_check.config.timeout_sec);
  ReadOptionalInt(node, "unhealthy_threshold", "health_check",
                  &config->health_check.config.unhealthy_threshold, 1,
                  std::numeric_limits<int>::max());
  ReadOptionalInt(node, "healthy_threshold", "health_check",
                  &config->health_check.config.healthy_threshold, 1,
                  std::numeric_limits<int>::max());
}

GatewayRateLimitBucketConfig ParseRateLimitBucket(const YAML::Node& node, std::string_view ctx,
                                                  GatewayRateLimitBucketConfig defaults) {
  if (node.IsScalar()) {
    defaults.enabled = As<bool>(node, ctx);
    return defaults;
  }

  RequireMap(node, ctx);
  defaults.enabled = true;
  ReadOptionalBool(node, "enabled", ctx, &defaults.enabled);
  ReadOptionalDouble(node, "rate", ctx, &defaults.rate);
  ReadOptionalDouble(node, "burst", ctx, &defaults.burst);
  ReadOptionalSize(node, "max_buckets", ctx, &defaults.max_buckets);
  return defaults;
}

void ParseRateLimit(const YAML::Node& root, GatewayConfig* config) {
  YAML::Node node = Field(root, "rate_limit");
  if (!node) return;
  RequireMap(node, "rate_limit");

  YAML::Node global = Field(node, "global");
  if (global) {
    config->rate_limit.global =
        ParseRateLimitBucket(global, "rate_limit.global", config->rate_limit.global);
  }

  YAML::Node per_ip = Field(node, "per_ip");
  if (per_ip) {
    config->rate_limit.per_ip =
        ParseRateLimitBucket(per_ip, "rate_limit.per_ip", config->rate_limit.per_ip);
  }
}

void ParseCircuitBreaker(const YAML::Node& node, std::string_view ctx, UpstreamConfig* config) {
  if (node.IsScalar()) {
    config->circuit_breaker_enabled = As<bool>(node, ctx);
    return;
  }

  RequireMap(node, ctx);
  config->circuit_breaker_enabled = true;
  ReadOptionalBool(node, "enabled", ctx, &config->circuit_breaker_enabled);
  ReadOptionalInt(node, "failure_threshold", ctx, &config->circuit_breaker.failure_threshold, 1,
                  std::numeric_limits<int>::max());
  ReadOptionalInt(node, "success_threshold", ctx, &config->circuit_breaker.success_threshold, 1,
                  std::numeric_limits<int>::max());
  YAML::Node open_timeout = Field(node, "open_timeout_ms");
  if (open_timeout) {
    config->circuit_breaker.open_timeout =
        ReadMilliseconds(open_timeout, JoinPath(ctx, "open_timeout_ms"), 1);
  }
  ReadOptionalInt(node, "half_open_max_requests", ctx,
                  &config->circuit_breaker.half_open_max_requests, 1,
                  std::numeric_limits<int>::max());
}

UpstreamPeerConfig ParsePeer(const YAML::Node& node, std::string_view ctx) {
  RequireMap(node, ctx);

  UpstreamPeerConfig peer;
  peer.host = ReadRequiredString(node, "host", ctx);
  peer.port = ReadPort(Field(node, "port"), JoinPath(ctx, "port"));
  peer.name = peer.host + ":" + std::to_string(peer.port);
  ReadOptionalString(node, "name", ctx, &peer.name);
  ReadOptionalInt(node, "weight", ctx, &peer.weight, 0, std::numeric_limits<int>::max());
  ReadOptionalInt(node, "max_fails", ctx, &peer.max_fails, 1, std::numeric_limits<int>::max());

  YAML::Node fail_timeout = Field(node, "fail_timeout_ms");
  if (fail_timeout) {
    peer.fail_timeout = ReadMilliseconds(fail_timeout, JoinPath(ctx, "fail_timeout_ms"));
  }

  return peer;
}

void ParseUpstreams(const YAML::Node& root, GatewayConfig* config) {
  YAML::Node node = Field(root, "upstreams");
  if (!node) return;
  RequireSequence(node, "upstreams");

  for (std::size_t i = 0; i < node.size(); ++i) {
    const std::string ctx = "upstreams[" + std::to_string(i) + "]";
    const YAML::Node item = node[i];
    RequireMap(item, ctx);

    GatewayUpstreamConfig upstream;
    upstream.config.name = ReadRequiredString(item, "name", ctx);
    ReadOptionalSize(item, "max_concurrent_requests", ctx,
                     &upstream.config.max_concurrent_requests);

    YAML::Node request_timeout = Field(item, "request_timeout_ms");
    if (request_timeout) {
      upstream.config.request_timeout =
          ReadMilliseconds(request_timeout, JoinPath(ctx, "request_timeout_ms"), 1);
    }

    YAML::Node circuit_breaker = Field(item, "circuit_breaker");
    if (circuit_breaker) {
      ParseCircuitBreaker(circuit_breaker, JoinPath(ctx, "circuit_breaker"), &upstream.config);
    }

    YAML::Node peers = Field(item, "peers");
    if (!peers) Fail(JoinPath(ctx, "peers"), "missing required value");
    RequireSequence(peers, JoinPath(ctx, "peers"));
    for (std::size_t j = 0; j < peers.size(); ++j) {
      upstream.peers.push_back(ParsePeer(peers[j], ctx + ".peers[" + std::to_string(j) + "]"));
    }

    config->upstreams.push_back(std::move(upstream));
  }
}

void ParseFallback(const YAML::Node& node, std::string_view ctx, FallbackConfig* fallback) {
  if (node.IsScalar()) {
    fallback->enabled = As<bool>(node, ctx);
    return;
  }

  RequireMap(node, ctx);
  fallback->enabled = true;
  ReadOptionalBool(node, "enabled", ctx, &fallback->enabled);
  YAML::Node status = Field(node, "status");
  if (!status) status = Field(node, "status_code");
  if (status) {
    fallback->status_code =
        ParseStatusCode(static_cast<int>(ReadInteger(status, JoinPath(ctx, "status"), 100, 599)),
                        JoinPath(ctx, "status"));
  }
  ReadOptionalString(node, "content_type", ctx, &fallback->content_type);
  ReadOptionalString(node, "body", ctx, &fallback->body);
}

void ParseRouteCircuitBreaker(const YAML::Node& node, std::string_view ctx,
                              GatewayRouteConfig* route) {
  if (node.IsScalar()) {
    route->circuit_breaker_enabled = As<bool>(node, ctx);
    return;
  }

  RequireMap(node, ctx);
  route->circuit_breaker_enabled = true;
  ReadOptionalBool(node, "enabled", ctx, &route->circuit_breaker_enabled);
}

void ParseRoutes(const YAML::Node& root, GatewayConfig* config) {
  YAML::Node node = Field(root, "routes");
  if (!node) return;
  RequireSequence(node, "routes");

  for (std::size_t i = 0; i < node.size(); ++i) {
    const std::string ctx = "routes[" + std::to_string(i) + "]";
    const YAML::Node item = node[i];
    RequireMap(item, ctx);

    GatewayRouteConfig route;
    route.path = ReadRequiredString(item, "path", ctx);
    route.upstream_name = ReadRequiredString(item, "upstream", ctx);
    ReadOptionalString(item, "match", ctx, &route.match);
    ReadOptionalString(item, "load_balance", ctx, &route.load_balance);

    YAML::Node circuit_breaker = Field(item, "circuit_breaker");
    if (circuit_breaker) {
      ParseRouteCircuitBreaker(circuit_breaker, JoinPath(ctx, "circuit_breaker"), &route);
    }

    YAML::Node fallback = Field(item, "fallback");
    if (fallback) {
      ParseFallback(fallback, JoinPath(ctx, "fallback"), &route.fallback);
    }

    config->routes.push_back(std::move(route));
  }
}

void ValidatePath(std::string_view path, std::string_view ctx) {
  if (path.empty() || path.front() != '/') {
    Fail(ctx, "path must start with '/'");
  }
}

void ValidateIPv4Endpoint(std::string_view host, std::uint16_t port, std::string_view ctx) {
  auto parsed = coropact::net::ParseIPv4Address(host, port);
  if (!parsed) {
    Fail(ctx, "expected a numeric IPv4 address");
  }
}

}  // namespace

GatewayConfig LoadGatewayConfigFromYaml(std::string_view path) {
  const std::string path_string(path);
  try {
    YAML::Node root = YAML::LoadFile(path_string);
    if (!root || !root.IsMap()) {
      throw GatewayConfigError("gateway config root must be a map");
    }

    GatewayConfig config;
    ParseServer(root, &config);
    ParseStatusEndpoint(root, &config);
    ParseHealthCheck(root, &config);
    ParseRateLimit(root, &config);
    ParseUpstreams(root, &config);
    ParseRoutes(root, &config);
    ValidateGatewayConfig(config);
    return config;
  } catch (const GatewayConfigError& ex) {
    throw GatewayConfigError("failed to load gateway config '" + path_string + "': " + ex.what());
  } catch (const YAML::Exception& ex) {
    throw GatewayConfigError("failed to load gateway config '" + path_string + "': " + ex.what());
  }
}

void ValidateGatewayConfig(const GatewayConfig& config) {
  if (config.server.name.empty()) Fail("server.name", "must not be empty");
  ValidateIPv4Endpoint(config.server.host, config.server.port, "server.listen");
  if (config.server.threads < 0 || config.server.threads > 1) {
    Fail("server.threads", "must be 0 or 1 until ReactorServer owns worker creation");
  }

  if (config.status_endpoint.enabled) {
    ValidatePath(config.status_endpoint.path, "status_endpoint.path");
  }
  if (config.health_check.enabled) {
    ValidatePath(config.health_check.config.path, "health_check.path");
    if (config.health_check.config.interval_sec <= 0.0) {
      Fail("health_check.interval_sec", "must be > 0");
    }
    if (config.health_check.config.timeout_sec <= 0.0) {
      Fail("health_check.timeout_sec", "must be > 0");
    }
  }

  auto validate_bucket = [](const GatewayRateLimitBucketConfig& bucket, std::string_view ctx) {
    if (!bucket.enabled) return;
    if (bucket.rate <= 0.0) Fail(JoinPath(ctx, "rate"), "must be > 0");
    if (bucket.burst <= 0.0) Fail(JoinPath(ctx, "burst"), "must be > 0");
  };
  validate_bucket(config.rate_limit.global, "rate_limit.global");
  validate_bucket(config.rate_limit.per_ip, "rate_limit.per_ip");

  std::unordered_set<std::string> upstream_names;
  for (std::size_t i = 0; i < config.upstreams.size(); ++i) {
    const GatewayUpstreamConfig& upstream = config.upstreams[i];
    const std::string ctx = "upstreams[" + std::to_string(i) + "]";
    if (upstream.config.name.empty()) Fail(JoinPath(ctx, "name"), "must not be empty");
    if (!upstream_names.insert(upstream.config.name).second) {
      Fail(JoinPath(ctx, "name"), "duplicate upstream name");
    }
    if (upstream.peers.empty()) Fail(JoinPath(ctx, "peers"), "must not be empty");
    if (upstream.config.request_timeout.count() <= 0) {
      Fail(JoinPath(ctx, "request_timeout_ms"), "must be > 0");
    }

    std::unordered_set<std::string> peer_names;
    for (std::size_t j = 0; j < upstream.peers.size(); ++j) {
      const UpstreamPeerConfig& peer = upstream.peers[j];
      const std::string peer_ctx = ctx + ".peers[" + std::to_string(j) + "]";
      if (peer.name.empty()) Fail(JoinPath(peer_ctx, "name"), "must not be empty");
      if (!peer_names.insert(peer.name).second) {
        Fail(JoinPath(peer_ctx, "name"), "duplicate peer name in upstream");
      }
      if (peer.host.empty()) Fail(JoinPath(peer_ctx, "host"), "must not be empty");
      ValidateIPv4Endpoint(peer.host, peer.port, JoinPath(peer_ctx, "host"));
      if (peer.weight < 0) Fail(JoinPath(peer_ctx, "weight"), "must be >= 0");
      if (peer.max_fails <= 0) Fail(JoinPath(peer_ctx, "max_fails"), "must be > 0");
      if (peer.fail_timeout.count() < 0) {
        Fail(JoinPath(peer_ctx, "fail_timeout_ms"), "must be >= 0");
      }
    }
  }

  std::unordered_set<std::string> route_keys;
  for (std::size_t i = 0; i < config.routes.size(); ++i) {
    const GatewayRouteConfig& route = config.routes[i];
    const std::string ctx = "routes[" + std::to_string(i) + "]";
    ValidatePath(route.path, JoinPath(ctx, "path"));
    if (route.upstream_name.empty()) Fail(JoinPath(ctx, "upstream"), "must not be empty");
    if (!upstream_names.contains(route.upstream_name)) {
      Fail(JoinPath(ctx, "upstream"), "unknown upstream");
    }
    if (route.match != "prefix") {
      Fail(JoinPath(ctx, "match"), "only 'prefix' proxy routes are supported");
    }
    if (!CreateLoadBalancer(route.load_balance)) {
      Fail(JoinPath(ctx, "load_balance"), "unknown load balancer");
    }
    const std::string route_key = route.match + ":" + route.path;
    if (!route_keys.insert(route_key).second) {
      Fail(JoinPath(ctx, "path"), "duplicate route");
    }
  }
}

void BuildGatewayUpstreamRegistry(const GatewayConfig& config, UpstreamRegistry& registry) {
  for (const GatewayUpstreamConfig& upstream_config : config.upstreams) {
    auto upstream = std::make_shared<Upstream>(upstream_config.config);
    for (const UpstreamPeerConfig& peer_config : upstream_config.peers) {
      upstream->AddPeer(std::make_shared<UpstreamPeer>(peer_config));
    }
    registry.Add(std::move(upstream));
  }
}

}  // namespace coropact::gateway
