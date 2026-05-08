#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/gateway/health_checker.h"
#include "runtime/gateway/load_balancer.h"
#include "runtime/gateway/proxy_pass.h"
#include "runtime/gateway/upstream_registry.h"
#include "runtime/http/http_context.h"
#include "runtime/http/http_response.h"
#include "runtime/http/http_types.h"
#include "runtime/http/router.h"
#include "runtime/net/inet_address.h"
#include "runtime/net/tcp_server.h"
#include "runtime/time/timestamp.h"

#include <memory>
#include <string>
#include <string_view>


namespace runtime::gateway {


// GateServer 封装 TcpServer, 拦截 proxy 路由走异步转发
// 直接路由走同步 handler
class GatewayServer : public runtime::base::NonCopyable {
public:
  using Handler = runtime::http::Handler;
  using TcpConnectionPtr = runtime::net::TcpConnection::TcpConnectionPtr;

  // 统一直接路由和代理路由
  enum class RouteType {
    Direct,
    Proxy,
  };

  enum class MatchType {
    Exact,
    Prefix,
  };
  struct Route {
    RouteType type;
    MatchType match_type{MatchType::Exact};

    runtime::http::Method method;
    bool match_all_methods{false};

    std::string path;

    Handler handler;  // Direct
    std::string upstream_name;  // Proxy
    std::unique_ptr<LoadBalancer> lb; // Proxy
  };

  GatewayServer(runtime::net::EventLoop* loop, 
                const runtime::net::InetAddress& addr, 
                std::string name, 
                UpstreamRegistry& registry);
  void SetThreadNum(int num_threads);

  // 注册直接处理路由 (同步 Handler)
  void Get(std::string_view path, Handler handler);
  void Post(std::string_view path, Handler handler);

  void AddProxyRoute(std::string_view path, 
                     std::string_view upstream_name,
                     std::string_view algo = "round_robin");
  void EnableHealthCheck(HealthCheckConfig cfg = {});
  const Route* MatchRoute(std::string_view path) const;
  void Start();
private:

  // 每个 TCP 连接独享的上下文，存在 conn->context_ (std::any) 里
  struct ConnCtx {
    runtime::http::HttpContext    http_ctx;  // 增量 HTTP 解析状态机
    std::shared_ptr<UpstreamRequest> upstream_req;  // 当前 upstream 请求，直接路由时为 nullptr
  };


  void OnConnection(const TcpConnectionPtr& conn);
  void OnMessage(const TcpConnectionPtr& conn,
                 runtime::net::Buffer& buf,
                 runtime::time::Timestamp ts);
                 
  runtime::http::HttpResponse MakeError(runtime::http::StatusCode code,
                                        std::string_view msg) const;
private:
  runtime::net::TcpServer server_;
  UpstreamRegistry& registry_;
  std::vector<Route> routes_; 
  std::unique_ptr<HealthChecker> health_checker_;
};

} // namespace runtime::gateway
