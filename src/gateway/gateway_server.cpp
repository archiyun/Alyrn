#include "runtime/gateway/gateway_server.h"

#include "runtime/log/logger.h"

namespace runtime::gateway {

GatewayServer::GatewayServer(runtime::net::EventLoop* loop,
                             const runtime::net::InetAddress& addr,
                             std::string name,
                             UpstreamRegistry& registry)
  : server_(loop, addr, std::move(name)), registry_(registry) {
  server_.SetConnectionCallback(
    [this](const TcpConnectionPtr& conn) { OnConnection(conn); });
  server_.SetMessageCallback(
    [this](const TcpConnectionPtr& conn, 
           runtime::net::Buffer& buf, 
           runtime::time::Timestamp ts) { OnMessage(conn, buf, ts); });
  // 预渲染 429 响应， 用于限流，避免每次都动态分配
  runtime::http::HttpResponse rate_limit_resp(true);
  rate_limit_resp.SetStatusCode(runtime::http::StatusCode::TooManyRequests);
  rate_limit_resp.SetContentType("application/json; charset=utf-8");
  rate_limit_resp.SetBody(R"({"error":"rate limit exceeded"})");
  rate_limit_response_429_ = rate_limit_resp.ToString();
} 

void GatewayServer::SetThreadNum(int num_threads) {
  server_.SetThreadNum(num_threads);
}

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


void GatewayServer::AddProxyRoute(std::string_view path,
                                  std::string_view upstream_name,
                                  std::string_view algo) {
  routes_.push_back(Route{
    .type = RouteType::Proxy,
    .match_type = MatchType::Prefix,
    .path = std::string(path),
    .upstream_name = std::string(upstream_name),
    .lb = CreateLoadBalancer(algo),
  });
}

void GatewayServer::AddProxyRoute(std::string_view path,
                                  std::string_view upstream_name,
                                  FallbackConfig fallback,
                                  bool circuit_breaker_enabled,
                                  std::string_view algo) {
  fallback.Init();
  // Eagerly initialise the circuit breaker here (single-threaded, before Start())
  // to avoid a data race in OnMessage where multiple IO threads could race on
  // the lazy GetCircuitBreaker()/SetCircuitBreaker() check-then-act sequence.
  if (circuit_breaker_enabled) {
    auto upstream = registry_.Find(upstream_name);
    if (upstream && upstream->Config().circuit_breaker_enabled &&
        !upstream->GetCircuitBreaker()) {
      upstream->SetCircuitBreaker(
          std::make_shared<CircuitBreaker>(upstream->Config().circuit_breaker));
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
void GatewayServer::AddStaticRoute(std::string_view url_prefix,
                                   std::string_view root_dir) {
  std::string prefix(url_prefix);
  routes_.push_back(Route{
    .type        = RouteType::Static,
    .match_type  = MatchType::Prefix,
    .match_all_methods = true,
    .path        = prefix,
    .static_root = std::filesystem::canonical(root_dir),
  });
}

void GatewayServer::EnableHealthCheck(HealthCheckConfig cfg) {
  health_checker_ = std::make_unique<HealthChecker>
    (server_.GetLoop(),registry_, std::move(cfg));
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

void GatewayServer::Start() {
  if (health_checker_) health_checker_->Start();
  server_.Start();
  LOG_INFO() << "gateway: started";
}

void GatewayServer::OnConnection(const TcpConnectionPtr& conn) {
  if (conn->Connected()) {
    conn->SetContext(ConnCtx{});
  }
}

void GatewayServer::OnMessage(const TcpConnectionPtr& conn,
                              runtime::net::Buffer& buf,
                              runtime::time::Timestamp ts) {
  auto& ctx = std::any_cast<ConnCtx&>(conn->GetContext());
  if (!ctx.http_ctx.ParseRequest(buf, ts)) {
    conn->Send(MakeError(runtime::http::StatusCode::BadRequest, "malformed request").ToString());
    conn->Shutdown();
    return;
  }

  while (ctx.http_ctx.GotAll()) {
    runtime::http::HttpRequest req = ctx.http_ctx.Request();
    ctx.http_ctx.Reset();

    // 注入真实的客户端信息,让上游知道真实 IP
    const std::string client_ip = conn->PeerAddress().ToIp();
    const auto existing_xff = req.GetHeader("x-forwarded-for");
    const std::string xff = existing_xff.empty() 
                                ? client_ip
                                : std::string(existing_xff) + ", " + client_ip;
    req.AddHeader("x-forwarded-for", xff);

    // 限流, 路由匹配之前按照流量选择过滤
    if (rate_limiter_) {
      // 全局流量和 IP 流量过滤
      if (!rate_limiter_->AllowGlobal() ||
          !rate_limiter_->AllowPerIP(client_ip)) {
        conn->Send(rate_limit_response_429_);
        continue;
      }
    }

    const Route* route = MatchRoute(req.Path());
    if (!route) {
      conn->Send(MakeError(runtime::http::StatusCode::NotFound, "not found").ToString());
      continue;
    }

    if (route->type == RouteType::Proxy) {
      auto upstream = registry_.Find(route->upstream_name);
      if (!upstream) {
        conn->Send(RenderFallback(*route, "upstream not found: " + route->upstream_name));
        continue;
      }

      // CB is initialised eagerly in AddProxyRoute; just read the shared_ptr here.
      CircuitBreaker* cb = nullptr;
      if (route->circuit_breaker_enabled) {
        cb = upstream->GetCircuitBreaker().get();
      }
      // 熔断检查: 在 LB 选择之前快速失败
      if (cb && !cb->AllowRequest()) {
        conn->Send(RenderFallback(*route, "circuit open"));
        continue;
      }

      auto& pool = GetOrCreatePool(conn->GetLoop());
      RequestContext req_ctx{
        .client_ip = client_ip,
        .uri = std::string(req.Path()),
      };
      ctx.upstream_req = ProxyPass::Forward(conn, req, *upstream, *route->lb, pool, req_ctx, cb);
      if (!ctx.upstream_req) {
        conn->Send(RenderFallback(*route, "no available upstream peer"));
      }
    } else if (route->type == RouteType::Static) {
      // Strip url prefix in the IO thread (pure string op, no syscall).
      std::string rel(req.Path());
      rel.erase(0, route->path.size());
      if (rel.empty()) rel = "/";

      auto* loop = conn->GetLoop();
      std::weak_ptr<runtime::net::TcpConnection> weak = conn;
      const bool keep_alive = req.KeepAlive();
      const std::filesystem::path root = route->static_root;

      static_pool_.Submit([loop, weak, root, rel = std::move(rel), keep_alive] {
        runtime::http::HttpResponse resp(!keep_alive);
        if (!ServeFile(root, rel, resp)) {
          resp.SetStatusCode(runtime::http::StatusCode::NotFound);
          resp.SetContentType("text/plain");
          resp.SetBody("not found");
        }
        std::string wire = resp.ToString();
        loop->RunInLoop([weak, wire = std::move(wire), keep_alive] {
          if (auto conn = weak.lock()) {
            conn->Send(wire);
            if (!keep_alive) conn->Shutdown();
          }
        });
      });
    } else {
      // Direct
      const bool keep_alive = req.KeepAlive();
      runtime::http::HttpResponse resp(!keep_alive);
      try {
        route->handler(req, resp);
      } catch (const std::exception& ex) {
        resp = MakeError(runtime::http::StatusCode::InternalServerError, ex.what());
        resp.SetCloseConnection(true);
      }
      conn->Send(resp.ToString());
      if (resp.CloseConnection()) { conn->Shutdown(); return; }
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
    // 严格按 '/' 段对齐：避免 /api 匹配 /apifoo
    if (path.size() == route.path.size() ||
        route.path.empty() || route.path.back() == '/' ||
        path[route.path.size()] == '/') {
      return &route;
    }
  }
  return nullptr;
}

UpstreamConnPool& GatewayServer::GetOrCreatePool(runtime::net::EventLoop* loop) {
  // map 结构竞争用锁保护；返回 reference 后调用方在该 loop 的 IO 线程上独占 pool 实例。
  std::unique_lock lk{pools_mu_};
  auto it = pools_.find(loop);
  if (it != pools_.end()) return it->second;

  auto [inserted, ok] = pools_.emplace(loop, UpstreamConnPool{pool_cfg_});
  UpstreamConnPool& pool_ref = inserted->second;
  lk.unlock();

  // EvictStale 仅在 loop 自己的线程跑，访问独占的 pool 实例。
  loop->RunEvery(30.0, [&pool_ref] { pool_ref.EvictStale(); });
  return pool_ref;
}

runtime::http::HttpResponse
GatewayServer::MakeError(runtime::http::StatusCode code, std::string_view msg) const {
  runtime::http::HttpResponse resp(true);
  resp.SetStatusCode(code);
  resp.SetContentType("application/json; charset=utf-8");
  resp.SetBody("{\"error\":\"" + std::string(msg) + "\"}");
  return resp;
}


std::string GatewayServer::RenderFallback(const Route& route,
                                          std::string_view reason) const {
  if (route.fallback.enabled) {
    return route.fallback.pre_rendered;
  }
  return MakeError(runtime::http::StatusCode::ServiceUnavailable, reason).ToString();
}
} // namespace runtime::gateway
