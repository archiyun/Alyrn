// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "runtime/gateway/gateway_server.h"

#include <array>
#include <atomic>
#include <chrono>

#include "runtime/log/logger.h"

namespace runtime::gateway {

namespace {
// RFC 7230 §6.1 hop-by-hop headers — 不能透传给上游
constexpr std::array<std::string_view, 9> kHopByHop = {
  "connection", "keep-alive", "proxy-connection",
  "proxy-authenticate", "proxy-authorization",
  "te", "trailer", "transfer-encoding", "upgrade",
};

// 客户端可能伪造的 forwarded 系列 - 信任策略: 剥掉重写, 不要追加
constexpr std::array<std::string_view, 4> kClientSpoofable = {
  "x-real-ip", "x-forwarded-proto", "x-forwarded-host", "x-forwarded-port",
};

// 单调时间戳 (ns) + 进程内原子序号, 拼成 16-hex + '-' + 16-hex
// 不依赖 UUID 库, 单机内唯一即可 (跨节点请在网关前置层做)
std::string GenRequestId() {
  static std::atomic<std::uint64_t> seq{0};
  const auto ts = static_cast<std::uint64_t>(
      std::chrono::steady_clock::now().time_since_epoch().count());
  const auto n = seq.fetch_add(1, std::memory_order_relaxed);
  char buf[34];
  std::snprintf(buf, sizeof(buf), "%016lx-%016lx",
                static_cast<unsigned long>(ts),
                static_cast<unsigned long>(n));
  return std::string(buf, 33);
}

void RewriteForUpstream(runtime::http::HttpRequest& req,
                        const std::string& client_ip,
                        std::string_view scheme,
                        std::string_view gateway_name,
                        std::string_view request_id) {
  // 保存原始 Host, 写入 X-Forwarded-Host (BuildRequest 会改写真正的 Host)
  std::string orig_host(req.GetHeader("host"));

  // Connection 列举的字段视同 hop-by-hop, 一并删除
  const auto conn_hdr = req.GetHeader("connection");
  if (!conn_hdr.empty()) {
    std::size_t i = 0;
    while (i < conn_hdr.size()) {
      const auto comma = conn_hdr.find(',', i);
      auto tok = conn_hdr.substr(
          i, comma == std::string_view::npos ? std::string_view::npos : comma - i);
      while (!tok.empty() && (tok.front() == ' ' || tok.front() == '\t')) tok.remove_prefix(1);
      while (!tok.empty() && (tok.back()  == ' ' || tok.back()  == '\t')) tok.remove_suffix(1);
      if (!tok.empty()) req.RemoveHeader(tok);
      if (comma == std::string_view::npos) break;
      i = comma + 1;
    }
  }

  for (auto h : kHopByHop) req.RemoveHeader(h);

  // 剥离客户端伪造的 forwarded 系列 (XFF 单独处理: append)
  for (auto h : kClientSpoofable) req.RemoveHeader(h);

  // XFF: 追加而非替换, 用 SetHeader 绕开 emplace 静默丢弃语义
  std::string xff(req.GetHeader("x-forwarded-for"));
  if (xff.empty()) {
    xff = client_ip;
  } else {
    xff += ", ";
    xff += client_ip;
  }
  req.SetHeader("x-forwarded-for", xff);

  req.SetHeader("x-real-ip", client_ip);
  req.SetHeader("x-forwarded-proto", scheme);
  if (!orig_host.empty()) req.SetHeader("x-forwarded-host", orig_host);

  // Via: RFC 7230 §5.7.1
  std::string via;
  if (const auto prev = req.GetHeader("via"); !prev.empty()) {
    via.assign(prev);
    via += ", ";
  }
  via += "1.1 ";
  via.append(gateway_name);
  req.SetHeader("via", via);

  // 全链路追踪 ID: 有则透传, 无则生成
  if (req.GetHeader("x-request-id").empty() && !request_id.empty()) {
    req.SetHeader("x-request-id", request_id);
  }
}
}  // namespace



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

void GatewayServer::EnableMetricsEndpoint(std::string_view path) {
  // 直接路由形式注册, handler 内同步渲染当前 metrics 快照.
  Get(path, [this](const runtime::http::HttpRequest&,
                   runtime::http::HttpResponse& resp) {
    resp.SetStatusCode(runtime::http::StatusCode::Ok);
    // Prometheus text format 0.0.4 的 Content-Type
    resp.SetContentType("text/plain; version=0.0.4; charset=utf-8");
    resp.SetBody(metrics_.Render());
  });
}

void GatewayServer::Start() {
  if (health_checker_) health_checker_->Start();
  server_.Start();
  LOG_INFO() << "gateway: started";
}

void GatewayServer::OnConnection(const TcpConnectionPtr& conn) {
  if (conn->Connected()) {
    // ConnCtx 包含 HttpContext (含 Pool unique_ptr) 是 move-only,
    // std::any 要求可拷贝, 因此包成 shared_ptr.
    conn->SetContext(std::make_shared<ConnCtx>());
    metrics_.connections_accepted_total.Inc();
    metrics_.connections_active.Inc();
  } else {
    metrics_.connections_closed_total.Inc();
    metrics_.connections_active.Dec();
  }
}

void GatewayServer::OnMessage(const TcpConnectionPtr& conn,
                              runtime::net::Buffer& buf,
                              runtime::time::Timestamp ts) {
  auto& ctx = *std::any_cast<std::shared_ptr<ConnCtx>&>(conn->GetContext());
  const runtime::http::ParseStatus parse_status = ctx.http_ctx.ParseRequest(buf, ts);
  if (parse_status != runtime::http::ParseStatus::Continue &&
      parse_status != runtime::http::ParseStatus::GotAll) {
    const runtime::http::StatusCode code =
        runtime::http::ParseStatusToStatusCode(parse_status);
    metrics_.requests_malformed_total.Inc();
    metrics_.ObserveStatus(static_cast<int>(code));
    conn->Send(MakeError(code, runtime::http::StatusMessage(code)).ToString());
    conn->Shutdown();
    return;
  }

  while (ctx.http_ctx.GotAll()) {
    runtime::http::HttpRequest req = ctx.http_ctx.TakeRequest();
    ctx.http_ctx.Reset();

    // 同步路径下记录端到端耗时; 异步 (proxy / static) 路径只埋单次事件计数.
    const auto req_start = std::chrono::steady_clock::now();

    // 注入真实的客户端信息,让上游知道真实 IP
    const std::string client_ip = conn->PeerAddress().ToIp();
    RewriteForUpstream(req, client_ip,
                       /*scheme=*/"http",
                       /*gateway_name=*/server_.Name(),
                       /*request_id=*/GenRequestId());

    // 限流, 路由匹配之前按照流量选择过滤
    if (rate_limiter_) {
      // 全局流量和 IP 流量过滤
      const bool global_ok  = rate_limiter_->AllowGlobal();
      const bool per_ip_ok  = global_ok && rate_limiter_->AllowPerIP(client_ip);
      if (!global_ok || !per_ip_ok) {
        if (!global_ok) metrics_.rate_limited_global_total.Inc();
        else            metrics_.rate_limited_per_ip_total.Inc();
        metrics_.ObserveStatus(
            static_cast<int>(runtime::http::StatusCode::TooManyRequests));
        const auto elapsed = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - req_start).count();
        metrics_.request_duration.Observe(elapsed);
        conn->Send(rate_limit_response_429_);
        continue;
      }
    }

    const Route* route = MatchRoute(req.GetPath());
    if (!route) {
      metrics_.requests_not_found_total.Inc();
      metrics_.ObserveStatus(static_cast<int>(runtime::http::StatusCode::NotFound));
      const auto elapsed = std::chrono::duration<double>(
          std::chrono::steady_clock::now() - req_start).count();
      metrics_.request_duration.Observe(elapsed);
      conn->Send(MakeError(runtime::http::StatusCode::NotFound, "not found").ToString());
      continue;
    }

    if (route->type == RouteType::Proxy) {
      metrics_.routes_proxy_total.Inc();
      auto upstream = registry_.Find(route->upstream_name);
      if (!upstream) {
        metrics_.upstream_no_peer_total.Inc();
        metrics_.fallback_served_total.Inc();
        metrics_.ObserveStatus(
            static_cast<int>(runtime::http::StatusCode::ServiceUnavailable));
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
        metrics_.circuit_breaker_rejected_total.Inc();
        metrics_.fallback_served_total.Inc();
        metrics_.ObserveStatus(
            static_cast<int>(runtime::http::StatusCode::ServiceUnavailable));
        conn->Send(RenderFallback(*route, "circuit open"));
        continue;
      }

      auto& pool = GetOrCreatePool(conn->GetLoop());
      RequestContext req_ctx{
        .client_ip = client_ip,
        .uri = std::string(req.GetPath()),
      };
      ctx.upstream_req = ProxyPass::Forward(conn, req, *upstream, *route->lb, pool, req_ctx, cb);
      if (!ctx.upstream_req) {
        metrics_.upstream_no_peer_total.Inc();
        metrics_.fallback_served_total.Inc();
        metrics_.ObserveStatus(
            static_cast<int>(runtime::http::StatusCode::ServiceUnavailable));
        conn->Send(RenderFallback(*route, "no available upstream peer"));
      } else {
        metrics_.upstream_requests_total.Inc();
      }
    } else {
      // Direct
      metrics_.routes_direct_total.Inc();
      const bool keep_alive = req.KeepAlive();
      runtime::http::HttpResponse resp(!keep_alive);
      try {
        route->handler(req, resp);
      } catch (const std::exception& ex) {
        resp = MakeError(runtime::http::StatusCode::InternalServerError, ex.what());
        resp.SetCloseConnection(true);
      }
      metrics_.ObserveStatus(static_cast<int>(resp.GetStatusCode()));
      const auto elapsed = std::chrono::duration<double>(
          std::chrono::steady_clock::now() - req_start).count();
      metrics_.request_duration.Observe(elapsed);
      conn->Send(resp.ToString());
      if (resp.GetCloseConnection()) { conn->Shutdown(); return; }
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
