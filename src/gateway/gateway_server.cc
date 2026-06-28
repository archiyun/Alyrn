// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/gateway/gateway_server.h"

#include <atomic>
#include <chrono>

#include "vexo/http/http_types.h"
#include "vexo/log/logger.h"
#include "vexo/net/event_loop.h"
#include "vexo/net/io_backend.h"
#include "vexo/net/reactor_backend.h"

namespace vexo::gateway {

namespace {
// Monotonic timestamp plus an in-process atomic sequence formatted as
// 16-hex + '-' + 16-hex. This avoids a UUID dependency and is only intended
// to be unique within one process.
std::string GenRequestId() {
  static std::atomic<std::uint64_t> seq{0};
  const auto ts =
      static_cast<std::uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
  const auto n = seq.fetch_add(1, std::memory_order_relaxed);
  char buf[34];
  std::snprintf(buf, sizeof(buf), "%016lx-%016lx", static_cast<unsigned long>(ts),
                static_cast<unsigned long>(n));
  return std::string(buf, 33);
}
}  // namespace

GatewayServer::GatewayServer(vexo::net::EventLoop* loop, const vexo::net::InetAddress& addr,
                             std::string name, UpstreamRegistry& registry,
                             vexo::net::Backend backend)
    : base_loop_(loop),
      server_(vexo::net::MakeServer(backend, loop, addr, std::move(name))),
      registry_(registry) {
  server_->set_connection_callback([this](const vexo::net::ConnPtr& conn) { OnConnection(conn); });
  server_->set_message_callback([this](const vexo::net::ConnPtr& conn, vexo::net::Buffer& buf,
                                       vexo::time::Timestamp ts) { OnMessage(conn, buf, ts); });
  // Pre-render the 429 response used by rate limiting to avoid per-request
  // allocation on the hot path.
  vexo::http::HttpResponse rate_limit_resp(true);
  rate_limit_resp.set_status_code(vexo::http::StatusCode::TooManyRequests);
  rate_limit_resp.set_content_type("application/json; charset=utf-8");
  rate_limit_resp.set_body(R"({"error":"rate limit exceeded"})");
  rate_limit_response_429_ = rate_limit_resp.ToString();
}

void GatewayServer::set_thread_num(int num_threads) { server_->set_thread_num(num_threads); }

void GatewayServer::Get(std::string_view path, Handler handler) {
  routes_.push_back(Route{
      .type = RouteType::Direct,
      .path = std::string(path),
      .handler = std::move(handler),
  });
}

void GatewayServer::Post(std::string_view path, Handler handler) {
  routes_.push_back(Route{
      .type = RouteType::Direct,
      .path = std::string(path),
      .handler = std::move(handler),
  });
}

void GatewayServer::AddProxyRoute(std::string_view path, std::string_view upstream_name,
                                  std::string_view algo) {
  routes_.push_back(Route{
      .type = RouteType::Proxy,
      .match_type = MatchType::Prefix,
      .path = std::string(path),
      .upstream_name = std::string(upstream_name),
      .lb = CreateLoadBalancer(algo),
  });
}

void GatewayServer::AddProxyRoute(std::string_view path, std::string_view upstream_name,
                                  FallbackConfig fallback, bool circuit_breaker_enabled,
                                  std::string_view algo) {
  fallback.Init();
  // Eagerly initialise the circuit breaker here (single-threaded, before Start())
  // to avoid a data race in OnMessage where multiple IO threads could race on
  // the lazy circuit_breaker()/set_circuit_breaker() check-then-act sequence.
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
void GatewayServer::EnableHealthCheck(HealthCheckConfig cfg) {
  health_checker_ = std::make_unique<HealthChecker>(base_loop_, registry_, std::move(cfg));
}

void GatewayServer::EnableRateLimit(RateLimiterConfig cfg) {
  rate_limiter_cfg_ = cfg;
  if (rate_limiter_cfg_.global_enabled || rate_limiter_cfg_.per_ip_enabled) {
    rate_limiter_ = std::make_unique<RateLimiter>(rate_limiter_cfg_);
  } else {
    rate_limiter_.reset();
  }
}

void GatewayServer::EnableGlobalRateLimit(double rate, double burst) {
  rate_limiter_cfg_.global_enabled = true;
  rate_limiter_cfg_.global_rate = rate;
  rate_limiter_cfg_.global_burst = burst;
  rate_limiter_ = std::make_unique<RateLimiter>(rate_limiter_cfg_);
}

void GatewayServer::EnablePerIPRateLimit(double rate, double burst) {
  rate_limiter_cfg_.per_ip_enabled = true;
  rate_limiter_cfg_.per_ip_rate = rate;
  rate_limiter_cfg_.per_ip_burst = burst;
  rate_limiter_ = std::make_unique<RateLimiter>(rate_limiter_cfg_);
}

void GatewayServer::EnableMetricsEndpoint(std::string_view path) {
  // Register as a direct route and render the current metrics snapshot
  // synchronously in the handler.
  Get(path, [this](const vexo::http::HttpRequest&, vexo::http::HttpResponse& resp) {
    resp.set_status_code(vexo::http::StatusCode::Ok);
    // Content-Type for Prometheus text format 0.0.4.
    resp.set_content_type("text/plain; version=0.0.4; charset=utf-8");
    resp.set_body(metrics_.Render());
  });
}

void GatewayServer::Start() {
  if (health_checker_) health_checker_->Start();
  server_->Start();
  LOG_INFO() << "gateway: started";
}

void GatewayServer::OnConnection(const vexo::net::ConnPtr& conn) {
  if (conn->Connected()) {
    // ConnCtx contains HttpContext, which is move-only because it owns a pool.
    // Store it behind shared_ptr because std::any requires copyable values.
    conn->set_context(std::make_shared<ConnCtx>());
    metrics_.connections_accepted_total.Inc();
    metrics_.connections_active.Inc();
  } else {
    metrics_.connections_closed_total.Inc();
    metrics_.connections_active.Dec();
  }
}

void GatewayServer::OnMessage(const vexo::net::ConnPtr& conn, vexo::net::Buffer& buf,
                              vexo::time::Timestamp ts) {
  auto& ctx = *std::any_cast<std::shared_ptr<ConnCtx>&>(conn->context());
  const vexo::http::ParseStatus parse_status = ctx.http_ctx.ParseRequest(buf, ts);
  if (parse_status != vexo::http::ParseStatus::Continue &&
      parse_status != vexo::http::ParseStatus::GotAll) {
    const vexo::http::StatusCode code = vexo::http::ParseStatusToStatusCode(parse_status);
    metrics_.requests_malformed_total.Inc();
    metrics_.ObserveStatus(static_cast<int>(code));
    conn->Send(MakeError(code, vexo::http::StatusMessage(code)).ToString());
    conn->Shutdown();
    return;
  }

  while (ctx.http_ctx.GotAll()) {
    vexo::http::HttpRequest req = ctx.http_ctx.TakeRequest();
    ctx.http_ctx.Reset();

    // Direct routes record end-to-end latency here.
    // Asynchronous routes only record per-event counters at this layer.
    const auto req_start = std::chrono::steady_clock::now();

    const std::string client_ip = conn->peer_address().ToIp();

    // Apply rate limiting before route matching.
    if (rate_limiter_) {
      // Global and per-IP limit checks.
      const bool global_ok = rate_limiter_->AllowGlobal();
      const bool per_ip_ok = global_ok && rate_limiter_->AllowPerIP(client_ip);
      if (!global_ok || !per_ip_ok) {
        if (!global_ok)
          metrics_.rate_limited_global_total.Inc();
        else
          metrics_.rate_limited_per_ip_total.Inc();
        metrics_.ObserveStatus(static_cast<int>(vexo::http::StatusCode::TooManyRequests));
        const auto elapsed =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - req_start).count();
        metrics_.request_duration.Observe(elapsed);
        conn->Send(rate_limit_response_429_);
        continue;
      }
    }

    const Route* route = MatchRoute(req.path());
    if (!route) {
      metrics_.requests_not_found_total.Inc();
      metrics_.ObserveStatus(static_cast<int>(vexo::http::StatusCode::NotFound));
      const auto elapsed =
          std::chrono::duration<double>(std::chrono::steady_clock::now() - req_start).count();
      metrics_.request_duration.Observe(elapsed);
      conn->Send(MakeError(vexo::http::StatusCode::NotFound, "not found").ToString());
      continue;
    }

    if (route->type == RouteType::Proxy) {
      metrics_.routes_proxy_total.Inc();

      auto native = vexo::net::ReactorNativeConn(conn);
      if (!native) {
        metrics_.fallback_served_total.Inc();
        metrics_.ObserveStatus(static_cast<int>(vexo::http::StatusCode::ServiceUnavailable));
        conn->Send(RenderFallback(*route, "proxy requires the epoll backend"));
        continue;
      }
      auto upstream = registry_.Find(route->upstream_name);
      if (!upstream) {
        metrics_.upstream_no_peer_total.Inc();
        metrics_.fallback_served_total.Inc();
        metrics_.ObserveStatus(static_cast<int>(vexo::http::StatusCode::ServiceUnavailable));
        conn->Send(RenderFallback(*route, "upstream not found: " + route->upstream_name));
        continue;
      }

      // CB is initialised eagerly in AddProxyRoute; just read the shared_ptr here.
      CircuitBreaker* cb = nullptr;
      if (route->circuit_breaker_enabled) {
        cb = upstream->circuit_breaker().get();
      }
      // Circuit breaker check: fail fast before load-balancer selection.
      if (cb && !cb->AllowRequest()) {
        metrics_.circuit_breaker_rejected_total.Inc();
        metrics_.fallback_served_total.Inc();
        metrics_.ObserveStatus(static_cast<int>(vexo::http::StatusCode::ServiceUnavailable));
        conn->Send(RenderFallback(*route, "circuit open"));
        continue;
      }

      auto& pool = GetOrCreatePool(native->loop());
      RequestContext req_ctx{
          .client_ip = client_ip,
          .uri = std::string(req.path()),
      };
      const std::string request_id = GenRequestId();
      const ForwardedHeaderContext forwarded{
          .client_ip = client_ip,
          .scheme = "http",
          .gateway_name = server_->name(),
          .request_id = request_id,
      };
      ctx.upstream_req =
          ProxyPass::Forward(native, req, *upstream, *route->lb, pool, req_ctx, cb, forwarded);
      if (!ctx.upstream_req) {
        metrics_.upstream_no_peer_total.Inc();
        metrics_.fallback_served_total.Inc();
        metrics_.ObserveStatus(static_cast<int>(vexo::http::StatusCode::ServiceUnavailable));
        conn->Send(RenderFallback(*route, "no available upstream peer"));
      } else {
        metrics_.upstream_requests_total.Inc();
      }
    } else {
      // Direct
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
      conn->Send(resp.ToString());
      if (resp.close_connection()) {
        conn->Shutdown();
        return;
      }
    }
  }
}

const GatewayServer::Route* GatewayServer::MatchRoute(std::string_view path) const {
  for (const auto& route : routes_) {
    if (route.match_type == MatchType::Exact && route.path == path) return &route;
  }
  for (const auto& route : routes_) {
    if (route.match_type != MatchType::Prefix) continue;
    if (!path.starts_with(route.path)) continue;
    // Match on path segment boundaries so /api does not match /apifoo.
    if (path.size() == route.path.size() || route.path.empty() || route.path.back() == '/' ||
        path[route.path.size()] == '/') {
      return &route;
    }
  }
  return nullptr;
}

UpstreamConnPool& GatewayServer::GetOrCreatePool(vexo::net::EventLoop* loop) {
  // Protect only the map structure. After a pool reference is returned, callers
  // access that pool exclusively from its owning loop thread.
  std::unique_lock lk{pools_mu_};
  auto it = pools_.find(loop);
  if (it != pools_.end()) return it->second;

  auto [inserted, ok] = pools_.emplace(loop, UpstreamConnPool{pool_cfg_});
  UpstreamConnPool& pool_ref = inserted->second;
  lk.unlock();

  // EvictStale runs on the owning loop thread and touches only that pool.
  loop->RunEvery(30.0, [&pool_ref] { pool_ref.EvictStale(); });
  return pool_ref;
}

vexo::http::HttpResponse GatewayServer::MakeError(vexo::http::StatusCode code,
                                                  std::string_view msg) const {
  vexo::http::HttpResponse resp(true);
  resp.set_status_code(code);
  resp.set_content_type("application/json; charset=utf-8");
  resp.set_body("{\"error\":\"" + std::string(msg) + "\"}");
  return resp;
}

std::string GatewayServer::RenderFallback(const Route& route, std::string_view reason) const {
  if (route.fallback.enabled) {
    return route.fallback.pre_rendered;
  }
  return MakeError(vexo::http::StatusCode::ServiceUnavailable, reason).ToString();
}

}  // namespace vexo::gateway
