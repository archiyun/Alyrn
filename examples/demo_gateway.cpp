#include "runtime/gateway/gateway_server.h"
#include "runtime/gateway/upstream.h"
#include "runtime/gateway/upstream_peer.h"
#include "runtime/gateway/upstream_registry.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"

int main() {
  runtime::gateway::UpstreamRegistry reg;
  auto upstream = std::make_shared<runtime::gateway::Upstream>(
      runtime::gateway::UpstreamConfig{.name = "user_service"});
  upstream->AddPeer(std::make_shared<runtime::gateway::UpstreamPeer>(
      runtime::gateway::UpstreamPeerConfig{.name = "127.0.0.1:9001", .host = "127.0.0.1", .port = 9001}));
  upstream->AddPeer(std::make_shared<runtime::gateway::UpstreamPeer>(
      runtime::gateway::UpstreamPeerConfig{.name = "127.0.0.1:9002", .host = "127.0.0.1", .port = 9002}));
  reg.Add(upstream);

  // 2. 创建网关
  runtime::net::EventLoop loop;
  runtime::net::InetAddress addr(8080);
  runtime::gateway::GatewayServer gw(&loop, addr, "gateway", reg);

  gw.SetThreadNum(4);

  // 直接路由
  gw.Get("/healthz", [](const runtime::http::HttpRequest&, runtime::http::HttpResponse& resp){
    resp.SetStatusCode(runtime::http::StatusCode::Ok);
    resp.SetContentType("application/json");
    resp.SetBody("{\"status\":\"ok\"}");
  });

  // 代理路由：把 /api/health 和 /api/kv 转发到 user_service（demo_http_server 有这两条路由）
  gw.AddProxyRoute("/api/health", "user_service", "round_robin");
  gw.AddProxyRoute("/api/kv",     "user_service", "round_robin");

  // 3. 启动主动健康检查（上游的 /api/health 返回 200，符合探针预期）
  gw.EnableHealthCheck({.path = "/api/health", .interval_sec = 10.0});

  gw.Start();
  loop.Loop();
}
