/**
 * 底层基于 Reactor 网络库
 * 同目录下 demo_gateway_luring.cc 基于 luring 模块 (io_uring).
 * 示例:
 * 与 nginx 的配置驱动不同，采用代码驱动。
 * 1. 创建 UpstreamRegistry
 * 2. 创建 user_service upstream
 * 3. 往 user_service 里加两个 peer: 9001 / 9002
 * 4. 创建 GatewaySessionService，把监听和 accept 交给 ReactorWorkerGroup
 * 5. 注册本地 direct route: /healthz
 * 6. 注册代理 route: /api/health 和 /api/kv -> user_service
 * 7. 启动 worker group
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
#include <iostream>
#include <memory>
#include <utility>

#include "coropact/gateway/gateway_session_service.h"
#include "coropact/gateway/upstream.h"
#include "coropact/gateway/upstream_peer.h"
#include "coropact/gateway/upstream_registry.h"
#include "coropact/http/http_request.h"
#include "coropact/http/http_response.h"
#include "coropact/http/http_types.h"
#include "coropact/net/inet_address.h"
#include "coropact/net/reactor_connect.h"
#include "coropact/net/reactor_worker_group.h"

int main() {
  // 1. 创建服务注册中心 和 配置上游
  coropact::gateway::UpstreamRegistry reg;

  auto upstream = std::make_shared<coropact::gateway::Upstream>(
      coropact::gateway::UpstreamConfig{.name = "user_service"});

  upstream->AddPeer(std::make_shared<coropact::gateway::UpstreamPeer>(
    coropact::gateway::UpstreamPeerConfig{
      .name = "127.0.0.1:9001", .host = "127.0.0.1", .port = 9001}
    ));

  upstream->AddPeer(std::make_shared<coropact::gateway::UpstreamPeer>(coropact::gateway::UpstreamPeerConfig{
      .name = "127.0.0.1:9002", .host = "127.0.0.1", .port = 9002}));

  auto registered = reg.Register(std::move(upstream));
  if (!registered.has_value()) {
    std::fprintf(stderr, "failed to register upstream: %s\n",
                 registered.error().message().c_str());
    return 1;
  }

  // 2. 创建网关 session service。net拥有 listener/accept/worker 生命周期。
  using Service =
      coropact::gateway::GatewaySessionService<coropact::net::ReactorStream,
                                               coropact::net::ReactorConnector>;
  Service gateway("gateway", reg);

  // 直接路由
  gateway.Get("/healthz", [](const coropact::http::HttpRequest&, coropact::http::HttpResponse& resp) {
    resp.set_status_code(coropact::http::StatusCode::Ok);
    resp.set_content_type("application/json");
    resp.set_body(R"({"status":"ok"})");
  });

  // 代理路由：把 /api/health 和 /api/kv 转发到 user_service
  gateway.AddProxyRoute("/api/health", "user_service", "round_robin");
  gateway.AddProxyRoute("/api/kv", "user_service", "round_robin");

  coropact::net::ReactorWorkerGroupOptions options;
  options.worker_num = 1;

  auto on_worker_init = [](coropact::net::ReactorWorkerContext& context) {
    std::printf("reactor worker %zu initialized\n", context.index);
  };
  auto on_worker_exit = [](coropact::net::ReactorWorkerContext& context) {
    std::printf("reactor worker %zu exited\n", context.index);
  };

  coropact::net::ReactorWorkerGroup workers(
      coropact::net::InetAddress(8080), std::move(options), std::move(on_worker_init),
      [&gateway](coropact::net::ReactorWorkerContext& context,
                 coropact::net::ReactorStream stream) -> coropact::coro::Task<void> {
        co_await gateway.Serve(std::move(stream),
                               coropact::net::ReactorConnector(&context.loop));
      },
      std::move(on_worker_exit));

  auto started = workers.Start();
  if (!started.has_value()) {
    std::fprintf(stderr, "failed to start worker group: %s\n",
                 started.error().message().c_str());
    return 1;
  }

  std::printf("gateway listening on 127.0.0.1:8080; press Enter to stop\n");
  std::cin.get();
  workers.Stop();
  return 0;
}
