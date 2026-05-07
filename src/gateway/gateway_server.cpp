#include "runtime/gateway/gateway_server.h"
#include "runtime/log/logger.h"

namespace runtime::gateway {

GatewayServer::GatewayServer(runtime::net::EventLoop* loop,
                             const runtime::net::InetAddress& addr,
                             std::string name,
                             ServiceRegistry& registry)
  : server_(loop, addr, std::move(name)), registry_(registry) {
  server_.SetConnectionCallback(
    [this](const TcpConnectionPtr& conn) { OnConnection(conn); });
  server_.SetMessageCallback(
    [this](const TcpConnectionPtr& conn, 
           runtime::net::Buffer& buf, 
           runtime::time::Timestamp ts) { OnMessage(conn, buf, ts); });
} 

void GatewayServer::SetThreadNum(int num_threads) {
  server_.SetThreadNum(num_threads);
}

void GatewayServer::Get(std::string_view path, Handler handler) {
  direct_routes_.emplace(std::string(path), std::move(handler));
}

void GatewayServer::Post(std::string_view path, Handler handler) {
  direct_routes_.emplace(std::string(path), std::move(handler));
}

void GatewayServer::AddProxyRoute(std::string_view path,
                                  std::string_view service_name,
                                  std::string_view algo){ 
  proxy_routes_.emplace(std::string(path), 
                ProxyRoute{std::string(service_name), 
                           MakeLoadBalancer(algo)});
}

void GatewayServer::EnableHealthCheck(HealthCheckConfig cfg) {
  health_checker_ = std::make_unique<HealthChecker>
    (server_.GetLoop(),registry_, std::move(cfg));
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

    // 跑代理路由
    for (const auto& [prefix, route] : proxy_routes_) {
      if (req.Path().starts_with(prefix)) {
        auto group = registry_.Resolve(route.service_name);
        if (!group) {
          conn->Send(MakeError(runtime::http::StatusCode::InternalServerError,
                               "service not found: " + route.service_name).ToString());
          break;
        }
        auto backend = route.lb->Select(*group);
        if (!backend) {
          conn->Send(MakeError(runtime::http::StatusCode::ServiceUnavailable,
                               "no healthy backend").ToString());
          break;
        }
        ctx.session = ProxyPass::Forward(conn, req, backend);
        goto next_request;
      }
    }

    {
      if (auto it = direct_routes_.find(req.Path()); 
               it != direct_routes_.end()) {
        const bool keep_alive = req.KeepAlive();
        runtime::http::HttpResponse resp(!keep_alive);
        try {
          it->second(req, resp);
        } catch (const std::exception& ex) {
          resp = MakeError(runtime::http::StatusCode::InternalServerError, ex.what());
          resp.SetCloseConnection(true);
        }
        conn->Send(resp.ToString());
        if (resp.CloseConnection()) { conn->Shutdown(); return; }
      } else {
        conn->Send(MakeError(runtime::http::StatusCode::NotFound, "not found").ToString());
      }
    }
    next_request:;
  }
}


runtime::http::HttpResponse
GatewayServer::MakeError(runtime::http::StatusCode code, std::string_view msg) const {
  runtime::http::HttpResponse resp(true);
  resp.SetStatusCode(code);
  resp.SetContentType("application/json; charset=utf-8");
  resp.SetBody("{\"error\":\"" + std::string(msg) + "\"}");
  return resp;
}
} // namespace runtime::gateway