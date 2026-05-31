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
 * 开三个终端，终端 1、2 启动 demo_http_server 模拟上游，
 * 终端 3 启动代理网关。
 * 终端 1: PORT=9001 ./build-tests/examples/demo_http_server
 * 终端 2: PORT=9002 ./build-tests/examples/demo_http_server
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
#include "runtime/gateway/gateway_server.h"
#include "runtime/gateway/upstream.h"
#include "runtime/gateway/upstream_peer.h"
#include "runtime/gateway/upstream_registry.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"

int main() {
  // 1. 创建服务注册中心 和 配置上游
  runtime::gateway::UpstreamRegistry reg;
  
  auto upstream = std::make_shared<runtime::gateway::Upstream>(
    runtime::gateway::UpstreamConfig{.name = "user_service"});
  
  upstream->AddPeer(std::make_shared<runtime::gateway::UpstreamPeer>(
    runtime::gateway::UpstreamPeerConfig{
      .name = "127.0.0.1:9001",
      .host = "127.0.0.1",
      .port = 9001}));

  upstream->AddPeer(std::make_shared<runtime::gateway::UpstreamPeer>(
    runtime::gateway::UpstreamPeerConfig{
      .name = "127.0.0.1:9002",
      .host = "127.0.0.1",
      .port = 9002}));
  
  reg.Add(upstream);

  // 2. 创建网关
  runtime::net::EventLoop loop;
  runtime::net::InetAddress addr(8080);
  runtime::gateway::GatewayServer gw(&loop, addr, "gateway", reg);

  gw.set_thread_num(4);

  // 直接路由
  gw.Get("/healthz", [](const runtime::http::HttpRequest&, 
                        runtime::http::HttpResponse& resp){
    resp.set_status_code(runtime::http::StatusCode::Ok);
    resp.set_content_type("application/json");
    resp.set_body("{\"status\":\"ok\"}");
  });

  // 代理路由：把 /api/health 和 /api/kv 转发到 user_service（demo_http_server 有这两条路由）
  gw.AddProxyRoute("/api/health", "user_service", "round_robin");
  gw.AddProxyRoute("/api/kv",     "user_service", "round_robin");

  // 3. 启动主动健康检查（上游的 /api/health 返回 200，符合探针预期）
  gw.EnableHealthCheck({.path = "/api/health", .interval_sec = 10.0});

  // 暴露 Prometheus 指标端点
  gw.EnableMetricsEndpoint();

  gw.Start();
  loop.Loop();
}
