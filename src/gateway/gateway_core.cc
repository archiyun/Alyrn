// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/gateway/gateway_core.h"

#include <atomic>
#include <cstdio>
#include <exception>
#include <memory>
#include <utility>

namespace coropact::gateway {

GatewayCore::GatewayCore(std::string name, UpstreamRegistry& registry)
    : name_(std::move(name)), registry_(registry) {
  coropact::http::HttpResponse rate_limit_resp(true);
  rate_limit_resp.SetStatusCode(coropact::http::StatusCode::TooManyRequests);
  rate_limit_resp.SetContentType("application/json; charset=utf-8");
  rate_limit_resp.SetBody(R"({"error":"rate limit exceeded"})");
  rate_limit_response_429_ = rate_limit_resp.ToString();
}

void GatewayCore::Get(std::string_view path, Handler handler) {
  routes_.push_back(Route{
      .type = RouteType::Direct,
      .method = coropact::http::Method::Get,
      .path = std::string(path),
      .handler = std::move(handler),
  });
}

void GatewayCore::Post(std::string_view path, Handler handler) {
  routes_.push_back(Route{
      .type = RouteType::Direct,
      .method = coropact::http::Method::Post,
      .path = std::string(path),
      .handler = std::move(handler),
  });
}

void GatewayCore::AddProxyRoute(std::string_view path, std::string_view upstream_name,
                                std::string_view algo) {
  routes_.push_back(Route{
      .type = RouteType::Proxy,
      .match_type = MatchType::Prefix,
      .match_all_methods = true,
      .path = std::string(path),
      .upstream_name = std::string(upstream_name),
      .lb = CreateLoadBalancer(algo),
  });
}

void GatewayCore::AddProxyRoute(std::string_view path, std::string_view upstream_name,
                                FallbackConfig fallback, bool circuit_breaker_enabled,
                                std::string_view algo) {
  fallback.Init();
  if (circuit_breaker_enabled) {
    auto resolved = registry_.Resolve(upstream_name);
    if (resolved.has_value()) {
      auto upstream = *resolved;
      if (upstream && upstream->config().circuit_breaker_enabled && !upstream->GetCircuitBreaker()) {
        upstream->SetCircuitBreaker(
            std::make_shared<CircuitBreaker>(upstream->config().circuit_breaker));
      }
    }
  }
  routes_.push_back(Route{
      .type = RouteType::Proxy,
      .match_type = MatchType::Prefix,
      .match_all_methods = true,
      .path = std::string(path),
      .upstream_name = std::string(upstream_name),
      .lb = CreateLoadBalancer(algo),
      .fallback = std::move(fallback),
      .circuit_breaker_enabled = circuit_breaker_enabled,
  });
}

void GatewayCore::EnableRateLimit(RateLimiterConfig cfg) {
  rate_limiter_cfg_ = cfg;
  if (rate_limiter_cfg_.global_enabled || rate_limiter_cfg_.per_ip_enabled) {
    rate_limiter_ = std::make_unique<RateLimiter>(rate_limiter_cfg_);
  } else {
    rate_limiter_.reset();
  }
}

void GatewayCore::EnableGlobalRateLimit(double rate, double burst) {
  rate_limiter_cfg_.global_enabled = true;
  rate_limiter_cfg_.global_rate = rate;
  rate_limiter_cfg_.global_burst = burst;
  rate_limiter_ = std::make_unique<RateLimiter>(rate_limiter_cfg_);
}

void GatewayCore::EnablePerIPRateLimit(double rate, double burst) {
  rate_limiter_cfg_.per_ip_enabled = true;
  rate_limiter_cfg_.per_ip_rate = rate;
  rate_limiter_cfg_.per_ip_burst = burst;
  rate_limiter_ = std::make_unique<RateLimiter>(rate_limiter_cfg_);
}

const GatewayCore::Route* GatewayCore::MatchRoute(std::string_view path) const {
  for (const auto& route : routes_) {
    if (route.match_type == MatchType::Exact && route.path == path) return &route;
  }
  for (const auto& route : routes_) {
    if (route.match_type != MatchType::Prefix) continue;
    if (!path.starts_with(route.path)) continue;
    if (path.size() == route.path.size() || route.path.empty() || route.path.back() == '/' ||
        path[route.path.size()] == '/') {
      return &route;
    }
  }
  return nullptr;
}

const GatewayCore::Route* GatewayCore::MatchRoute(std::string_view path,
                                                  coropact::http::Method method) const {
  bool has_exact_path = false;
  for (const auto& route : routes_) {
    if (route.match_type != MatchType::Exact || route.path != path) continue;
    has_exact_path = true;
    if (route.match_all_methods || route.method == method) return &route;
  }

  // An exact path takes precedence over every prefix route, even when the
  // exact route rejects the method. The caller can then return 405 instead of
  // unexpectedly falling through to a less specific proxy route.
  if (has_exact_path) return nullptr;

  for (const auto& route : routes_) {
    if (route.match_type != MatchType::Prefix) continue;
    if (!path.starts_with(route.path)) continue;
    if (path.size() != route.path.size() && !route.path.empty() &&
        route.path.back() != '/' && path[route.path.size()] != '/') {
      continue;
    }
    if (route.match_all_methods || route.method == method) return &route;
  }
  return nullptr;
}

GatewayCore::Action GatewayCore::HandleParseError(coropact::http::ParseStatus parse_status) {
  const coropact::http::StatusCode code = coropact::http::ParseStatusToStatusCode(parse_status);
  return SendResponse(MakeError(code, coropact::http::StatusMessage(code)).ToString(), true);
}

GatewayCore::Action GatewayCore::HandleRequest(const coropact::http::HttpRequest& req,
                                               std::string_view client_ip) {
  if (rate_limiter_) {
    const bool global_ok = rate_limiter_->AllowGlobal();
    const bool per_ip_ok = global_ok && rate_limiter_->AllowPerIP(client_ip);
    if (!global_ok || !per_ip_ok) {
      return SendResponse(rate_limit_response_429_);
    }
  }

  const Route* route = MatchRoute(req.path(), req.method());
  if (!route) {
    if (MatchRoute(req.path()) != nullptr) {
      return SendResponse(
          MakeError(coropact::http::StatusCode::MethodNotAllowed, "method not allowed").ToString());
    }
    return SendResponse(MakeError(coropact::http::StatusCode::NotFound, "not found").ToString());
  }

  if (route->type == RouteType::Proxy) {
    auto resolved = registry_.Resolve(route->upstream_name);
    if (!resolved.has_value()) {
      return SendResponse(
          RenderFallback(*route, std::string("upstream not found: ") + route->upstream_name));
    }
    auto upstream = *resolved;

    CircuitBreaker* circuitbreaker = nullptr;
    if (route->circuit_breaker_enabled) {
      circuitbreaker = upstream->GetCircuitBreaker().get();
    }
    if (circuitbreaker && !circuitbreaker->AllowRequest()) {
      return SendResponse(RenderFallback(*route, "circuit open"));
    }

    ProxyTarget proxy{
        .route = route,
        .upstream = std::move(upstream),
        .load_balancer = route->lb.get(),
        .request_ctx =
            RequestContext{
                .client_ip = std::string(client_ip),
                .uri = std::string(req.path()),
            },
        .circuit_breaker = circuitbreaker,
        .client_ip = std::string(client_ip),
        .request_id = GenRequestId(),
    };
    return Action{
        .kind = GatewayActionKind::Proxy,
        .proxy = std::move(proxy),
    };
  }

  const bool keep_alive = req.KeepAlive();
  coropact::http::HttpResponse resp(!keep_alive);
  try {
    route->handler(req, resp);
  } catch (const std::exception& ex) {
    resp = MakeError(coropact::http::StatusCode::InternalServerError, ex.what());
    resp.SetCloseConnection(true);
  }
  return SendResponse(resp.ToString(), resp.CloseConnection());
}

GatewayCore::Action GatewayCore::ProxyUnavailable(const Route& route, std::string_view reason) {
  return SendResponse(RenderFallback(route, reason));
}

ForwardedHeaderContext GatewayCore::MakeForwardedContext(const ProxyTarget& proxy) const {
  return ForwardedHeaderContext{
      .client_ip = proxy.client_ip,
      .scheme = "http",
      .gateway_name = name_,
      .request_id = proxy.request_id,
  };
}

coropact::http::HttpResponse GatewayCore::MakeError(coropact::http::StatusCode code,
                                                std::string_view msg) const {
  coropact::http::HttpResponse resp(true);
  resp.SetStatusCode(code);
  resp.SetContentType("application/json; charset=utf-8");
  resp.SetBody("{\"error\":\"" + std::string(msg) + "\"}");
  return resp;
}

std::string GatewayCore::RenderFallback(const Route& route, std::string_view reason) const {
  if (route.fallback.enabled) {
    return route.fallback.pre_rendered;
  }
  return MakeError(coropact::http::StatusCode::ServiceUnavailable, reason).ToString();
}

GatewayCore::Action GatewayCore::SendResponse(std::string response, bool close_after_send) const {
  return Action{
      .kind = GatewayActionKind::Send,
      .response = std::move(response),
      .close_after_send = close_after_send,
  };
}

std::string GatewayCore::GenRequestId() {
  static std::atomic<std::uint64_t> seq{0};
  const auto ts =
      static_cast<std::uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
  const auto n = seq.fetch_add(1, std::memory_order_relaxed);
  char buf[34];
  std::snprintf(buf, sizeof(buf), "%016lx-%016lx", static_cast<unsigned long>(ts),
                static_cast<unsigned long>(n));
  return std::string(buf, 33);
}

}  // namespace coropact::gateway
