/**
 * 示例:
 * 与 nginx 的配置驱动不同，采用代码驱动。
 * 1. 创建 UpstreamRegistry
 * 2. 创建 user_service upstream
 * 3. 往 user_service 里加两个 peer: 9001 / 9002
 * 4. 创建 GatewayServer，监听 8080
 * 5. 注册本地 direct route: /healthz
 * 6. 注册代理 route: /api/health 和 /api/kv -> user_service
 * 7. 开健康检查
 * 8. 启动事件循环
 *
 * 测试:
 * 开三个终端，终端 1、2 启动两个 HTTP 服务模拟上游，
 * 终端 3 启动代理网关。
 * 终端 1、2: 启动监听 9001 / 9002 的 HTTP 服务
 * 终端 3: ./build-tests/examples/demo_gateway
 *
 * 再开一个客户端 当作客户端
 * 健康测试
 * curl -i http://127.0.0.1:8080/healthz
 * 测试代理路由
 * curl -i http://127.0.0.1:8080/api/health
 * 测试KV代理路由
 * curl -i http://127.0.0.1:8080/api/kv
 *
 * ... 自行扩展
 */
#include <cstdio>
#include <utility>

#include "coropact/gateway/gateway_server.h"
#include "coropact/gateway/upstream.h"
#include "coropact/gateway/upstream_peer.h"
#include "coropact/gateway/upstream_registry.h"
#include "coropact/net/event_loop.h"
#include "coropact/net/event_loop_scheduler.h"
#include "coropact/net/inet_address.h"
#include "coropact/net/reactor_connect.h"
#include "coropact/net/reactor_listener.h"

int main() {
  // 1. 创建服务注册中心 和 配置上游
  coropact::gateway::UpstreamRegistry reg;

  auto upstream = std::make_shared<coropact::gateway::Upstream>(
      coropact::gateway::UpstreamConfig{.name = "user_service"});

  upstream->AddPeer(std::make_shared<coropact::gateway::UpstreamPeer>(coropact::gateway::UpstreamPeerConfig{
      .name = "127.0.0.1:9001", .host = "127.0.0.1", .port = 9001}));

  upstream->AddPeer(std::make_shared<coropact::gateway::UpstreamPeer>(coropact::gateway::UpstreamPeerConfig{
      .name = "127.0.0.1:9002", .host = "127.0.0.1", .port = 9002}));

  reg.Add(upstream);

  // 2. 创建网关
  coropact::net::EventLoop loop;
  coropact::net::EventLoopScheduler scheduler(&loop);
  coropact::net::InetAddress addr(8080);
  auto listener_result = coropact::net::ReactorListener::Create(&loop, addr);
  if (!listener_result.has_value()) {
    std::fprintf(stderr, "failed to create listener: %s\n",
                 listener_result.error().message().c_str());
    return 1;
  }
  auto listener = std::move(*listener_result);

  auto connector_result = coropact::net::ReactorConnector::Create(&loop);
  if (!connector_result.has_value()) {
    std::fprintf(stderr, "failed to create connector: %s\n",
                 connector_result.error().message().c_str());
    return 1;
  }
  auto connector = std::move(*connector_result);
  coropact::gateway::GatewayServer<coropact::net::ReactorListener, coropact::net::ReactorConnector> gw(
      listener, scheduler, "gateway", reg, connector);

  // 直接路由
  gw.Get("/healthz", [](const coropact::http::HttpRequest&, coropact::http::HttpResponse& resp) {
    resp.set_status_code(coropact::http::StatusCode::Ok);
    resp.set_content_type("application/json");
    resp.set_body("{\"status\":\"ok\"}");
  });

  // 代理路由：把 /api/health 和 /api/kv 转发到 user_service
  gw.AddProxyRoute("/api/health", "user_service", "round_robin");
  gw.AddProxyRoute("/api/kv", "user_service", "round_robin");

  // 3. 启动主动健康检查（上游的 /api/health 返回 200，符合探针预期）
  gw.EnableHealthCheck({.path = "/api/health", .interval_sec = 10.0});

  gw.Start();
  loop.Loop();
}
