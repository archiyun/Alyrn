// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/gateway/gateway_core.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <exception>
#include <memory>
#include <utility>

namespace vexo::gateway {

GatewayCore::GatewayCore(std::string name, UpstreamRegistry& registry)
    : name_(std::move(name)), registry_(registry) {
  vexo::http::HttpResponse rate_limit_resp(true);
  rate_limit_resp.set_status_code(vexo::http::StatusCode::TooManyRequests);
  rate_limit_resp.set_content_type("application/json; charset=utf-8");
  rate_limit_resp.set_body(R"({"error":"rate limit exceeded"})");
  rate_limit_response_429_ = rate_limit_resp.ToString();
}

void GatewayCore::Get(std::string_view path, Handler handler) {
  routes_.push_back(Route{
      .type = RouteType::Direct,
      .path = std::string(path),
      .handler = std::move(handler),
  });
}

void GatewayCore::Post(std::string_view path, Handler handler) {
  routes_.push_back(Route{
      .type = RouteType::Direct,
      .path = std::string(path),
      .handler = std::move(handler),
  });
}

void GatewayCore::AddProxyRoute(std::string_view path, std::string_view upstream_name,
                                std::string_view algo) {
  routes_.push_back(Route{
      .type = RouteType::Proxy,
      .match_type = MatchType::Prefix,
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
    auto upstream = registry_.Find(upstream_name);
    if (upstream && upstream->config().circuit_breaker_enabled && !upstream->circuit_breaker()) {
      upstream->set_circuit_breaker(
          std::make_shared<CircuitBreaker>(upstream->config().circuit_breaker));
    }
  }
  routes_.push_back(Route{
      .type = RouteType::Proxy,
      .match_type = MatchType::Prefix,
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

void GatewayCore::EnableMetricsEndpoint(std::string_view path) {
  Get(path, [this](const vexo::http::HttpRequest&, vexo::http::HttpResponse& resp) {
    resp.set_status_code(vexo::http::StatusCode::Ok);
    resp.set_content_type("text/plain; version=0.0.4; charset=utf-8");
    resp.set_body(metrics_.Render());
  });
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

void GatewayCore::OnConnectionOpen() {
  metrics_.connections_accepted_total.Inc();
  metrics_.connections_active.Inc();
}

void GatewayCore::OnConnectionClosed() {
  metrics_.connections_closed_total.Inc();
  metrics_.connections_active.Dec();
}

GatewayCore::Action GatewayCore::HandleParseError(vexo::http::ParseStatus parse_status) {
  const vexo::http::StatusCode code = vexo::http::ParseStatusToStatusCode(parse_status);
  metrics_.requests_malformed_total.Inc();
  metrics_.ObserveStatus(static_cast<int>(code));
  return SendResponse(MakeError(code, vexo::http::StatusMessage(code)).ToString(), true);
}

GatewayCore::Action GatewayCore::HandleRequest(const vexo::http::HttpRequest& req,
                                               std::string_view client_ip) {
  const auto req_start = std::chrono::steady_clock::now();

  if (rate_limiter_) {
    const bool global_ok = rate_limiter_->AllowGlobal();
    const bool per_ip_ok = global_ok && rate_limiter_->AllowPerIP(client_ip);
    if (!global_ok || !per_ip_ok) {
      if (!global_ok) {
        metrics_.rate_limited_global_total.Inc();
      } else {
        metrics_.rate_limited_per_ip_total.Inc();
      }
      metrics_.ObserveStatus(static_cast<int>(vexo::http::StatusCode::TooManyRequests));
      const auto elapsed =
          std::chrono::duration<double>(std::chrono::steady_clock::now() - req_start).count();
      metrics_.request_duration.Observe(elapsed);
      return SendResponse(rate_limit_response_429_);
    }
  }

  const Route* route = MatchRoute(req.path());
  if (!route) {
    metrics_.requests_not_found_total.Inc();
    metrics_.ObserveStatus(static_cast<int>(vexo::http::StatusCode::NotFound));
    const auto elapsed =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - req_start).count();
    metrics_.request_duration.Observe(elapsed);
    return SendResponse(MakeError(vexo::http::StatusCode::NotFound, "not found").ToString());
  }

  if (route->type == RouteType::Proxy) {
    metrics_.routes_proxy_total.Inc();

    auto upstream = registry_.Find(route->upstream_name);
    if (!upstream) {
      metrics_.upstream_no_peer_total.Inc();
      metrics_.fallback_served_total.Inc();
      metrics_.ObserveStatus(static_cast<int>(vexo::http::StatusCode::ServiceUnavailable));
      return SendResponse(
          RenderFallback(*route, std::string("upstream not found: ") + route->upstream_name));
    }

    CircuitBreaker* cb = nullptr;
    if (route->circuit_breaker_enabled) {
      cb = upstream->circuit_breaker().get();
    }
    if (cb && !cb->AllowRequest()) {
      metrics_.circuit_breaker_rejected_total.Inc();
      metrics_.fallback_served_total.Inc();
      metrics_.ObserveStatus(static_cast<int>(vexo::http::StatusCode::ServiceUnavailable));
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
        .circuit_breaker = cb,
        .client_ip = std::string(client_ip),
        .request_id = GenRequestId(),
    };
    return Action{
        .kind = GatewayActionKind::Proxy,
        .proxy = std::move(proxy),
    };
  }

  metrics_.routes_direct_total.Inc();
  const bool keep_alive = req.keep_alive();
  vexo::http::HttpResponse resp(!keep_alive);
  try {
    route->handler(req, resp);
  } catch (const std::exception& ex) {
    resp = MakeError(vexo::http::StatusCode::InternalServerError, ex.what());
    resp.set_close_connection(true);
  }
  metrics_.ObserveStatus(static_cast<int>(resp.status_code()));
  const auto elapsed =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - req_start).count();
  metrics_.request_duration.Observe(elapsed);
  return SendResponse(resp.ToString(), resp.close_connection());
}

GatewayCore::Action GatewayCore::ProxyUnavailable(const Route& route, std::string_view reason,
                                                  bool count_no_peer) {
  if (count_no_peer) {
    metrics_.upstream_no_peer_total.Inc();
  }
  metrics_.fallback_served_total.Inc();
  metrics_.ObserveStatus(static_cast<int>(vexo::http::StatusCode::ServiceUnavailable));
  return SendResponse(RenderFallback(route, reason));
}

void GatewayCore::OnProxyStarted() { metrics_.upstream_requests_total.Inc(); }

ForwardedHeaderContext GatewayCore::MakeForwardedContext(const ProxyTarget& proxy) const {
  return ForwardedHeaderContext{
      .client_ip = proxy.client_ip,
      .scheme = "http",
      .gateway_name = name_,
      .request_id = proxy.request_id,
  };
}

vexo::http::HttpResponse GatewayCore::MakeError(vexo::http::StatusCode code,
                                                std::string_view msg) const {
  vexo::http::HttpResponse resp(true);
  resp.set_status_code(code);
  resp.set_content_type("application/json; charset=utf-8");
  resp.set_body("{\"error\":\"" + std::string(msg) + "\"}");
  return resp;
}

std::string GatewayCore::RenderFallback(const Route& route, std::string_view reason) const {
  if (route.fallback.enabled) {
    return route.fallback.pre_rendered;
  }
  return MakeError(vexo::http::StatusCode::ServiceUnavailable, reason).ToString();
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

}  // namespace vexo::gateway
