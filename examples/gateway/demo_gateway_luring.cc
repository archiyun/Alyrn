/**
 * 底层基于 io_uring
 * 同目录下 demo_gateway.cc 基于 reactor 模块 (Reactor/epoll)
 * 示例:
 *   配置同 demo_gateway.cc, 详情参考上方注释.
 *
 * 测试前提: 安装 python3 和 curl
 * Run:
   mkdir -p /tmp/coropact-upstream/api
   printf 'upstream health\n' > /tmp/coropact-upstream/api/health
   printf 'upstream kv\n' > /tmp/coropact-upstream/api/kv

   Terminal 1:
   cd /tmp/coropact-upstream
   python3 -m http.server 9001 --bind 127.0.0.1

   Terminal 2:
   cd /tmp/coropact-upstream
   python3 -m http.server 9002 --bind 127.0.0.1

   Start Gateway:
   ./build-coropact-uring/examples/gateway/demo_gateway_luring

   Terminal 3:

   curl -i --noproxy 'x' http://127.0.0.1:8080/healthz
   curl -i --noproxy 'x' http://127.0.0.1:8080/api/health
   curl -i --noproxy 'x' http://127.0.0.1:8080/api/kv

 */

#include <cstdio>
#include <memory>
#include <print>
#include <iostream>
#include <stdexcept>
#include <utility>

#include "coropact/gateway/gateway_session_service.h"
#include "coropact/gateway/gateway_health_service.h"
#include "coropact/gateway/upstream.h"
#include "coropact/gateway/upstream_peer.h"
#include "coropact/gateway/upstream_registry.h"
#include "coropact/http/http_request.h"
#include "coropact/http/http_response.h"
#include "coropact/http/http_types.h"
#include "coropact/luring/stream.h"
#include "coropact/luring/worker.h"
#include "coropact/net/endpoint.h"
#include "coropact/luring/connector.h"
#include "coropact/luring/worker_group.h"

int main() {
  // 1. 创建服务注册中心 和 配置上游.
  coropact::gateway::UpstreamRegistry reg;

  // 创建上游集群
  auto upstream = std::make_shared<coropact::gateway::Upstream>(
    coropact::gateway::UpstreamConfig{.name = "user_service"}
  );

  // 部署两个集群的节点
  upstream->AddPeer(std::make_shared<coropact::gateway::UpstreamPeer>(
    coropact::gateway::UpstreamPeerConfig{
    .name = "127.0.0.1:9001", .host = "127.0.0.1", .port = 9001}
  ));

  upstream->AddPeer(std::make_shared<coropact::gateway::UpstreamPeer>(
    coropact::gateway::UpstreamPeerConfig{
    .name = "127.0.0.1:9002", .host = "127.0.0.1", .port = 9002
    }
  ));

  // 注册中心挂载服务
  auto reg_result = reg.Register(upstream);
  if (!reg_result.has_value()) {
    std::println(stderr, "failed to register upstream: {}", reg_result.error().message());
    return 1;
  }

  // 2. 创建网关 session service. luring 拥有 listener/accept/worker lifetime
  using Service =
    coropact::gateway::GatewaySessionService<coropact::luring::LUringStream,
                                             coropact::luring::LUringConnector>;

  Service gateway("gateway", reg);
  Service::Pool pool;
  coropact::gateway::GatewayHealthService<coropact::luring::LUringConnector> health(
      reg, coropact::gateway::HealthCheckConfig{.path = "/api/health",
                                                .interval_sec = 5.0,
                                                .timeout_sec = 1.0,
                                                .unhealthy_threshold = 3,
                                                .healthy_threshold = 2});

  // 直接路由
  gateway.Get("/healthz",
    [](const coropact::http::HttpRequest&,
    coropact::http::HttpResponse& resp) {
      resp.SetStatusCode(coropact::http::StatusCode::Ok);
      resp.SetContentType("application/json");
      resp.SetBody(R"({"status":"ok"})");
    });

  // 代理路由
  gateway.AddProxyRoute("/api/health", "user_service", "round_robin");
  gateway.AddProxyRoute("/api/kv", "user_service", "round_robin");

  coropact::luring::LUringWorkerGroupOptions options;
  options.worker_num = 1;

  auto on_worker_init = [&health](coropact::luring::LUringWorkerContext& context) {
    std::println("luring worker {} initialized", context.index);
    if (context.index == 0 &&
        !health.Start(context.loop,
                      coropact::luring::LUringConnector(&context.loop))) {
      throw std::runtime_error("gateway health service already started");
    }
  };
  auto on_worker_exit = [](coropact::luring::LUringWorkerContext& context) {
    std::println("luring worker {} exited", context.index);
  };

  coropact::luring::LUringWorkerGroup workers(
      coropact::net::Endpoint(8080), std::move(options), std::move(on_worker_init),
      [&gateway, &pool](coropact::luring::LUringWorkerContext& context,
                        coropact::luring::LUringStream stream) -> coropact::coro::DetachedTask {
        co_await gateway.Serve(std::move(stream),
                               coropact::luring::LUringConnector(&context.loop), pool);
      },
      std::move(on_worker_exit));

  auto started = workers.Start();
  if (!started.has_value()) {
    std::println(stderr, "failed to start worker group: {}", started.error().message());
    return 1;
  }

  std::println("gateway listening on 127.0.0.1:8080; press Enter to stop");
  std::cin.get();
  health.StopAndJoin();
  workers.Stop();
  return 0;
}
