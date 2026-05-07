#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/gateway/health_checker.h"
#include "runtime/gateway/load_balancer.h"
#include "runtime/gateway/proxy_pass.h"
#include "runtime/gateway/service_registry.h"
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
#include <unordered_map>

namespace runtime::gateway {

using Handler = runtime::http::Handler;
using TcpConnectionPtr = runtime::net::TcpConnection::TcpConnectionPtr;
// GateServer 封装 TcpServer, 拦截 proxy 路由走异步转发
// 直接路由走同步 handler
class GatewayServer : public runtime::base::NonCopyable {
public:

  GatewayServer(runtime::net::EventLoop* loop, 
                const runtime::net::InetAddress& addr, 
                std::string name, 
                ServiceRegistry& registry);
  void SetThreadNum(int num_threads);

  // 注册直接处理路由 (同步 Handler)
  void Get(std::string_view path, Handler handler);
  void Post(std::string_view path, Handler handler);

  void AddProxyRoute(std::string_view path, 
                     std::string_view serive_name, 
                     std::string_view algo = "round_robin");
  void EnableHealthCheck(HealthCheckConfig cfg = {});

  void Start();
private:
  
  struct ProxyRoute {
    std::string service_name;
    std::unique_ptr<LoadBalancer> lb;
  };

  // 每个 TCP 连接独享的上下文，存在 conn->context_ (std::any) 里
  struct ConnCtx {
    runtime::http::HttpContext    http_ctx;  // 增量 HTTP 解析状态机
    std::shared_ptr<ProxySession> session;   // 当前代理会话，直接路由时为 nullptr
  };


  void OnConnection(const TcpConnectionPtr& conn);
  void OnMessage(const TcpConnectionPtr& conn,
                 runtime::net::Buffer& buf,
                 runtime::time::Timestamp ts);
                 
  runtime::http::HttpResponse MakeError(runtime::http::StatusCode code,
                                        std::string_view msg) const;
private:
  runtime::net::TcpServer server_;
  ServiceRegistry& registry_;
  // 直接路由表 : path -> handler
  std::unordered_map<std::string, Handler> direct_routes_;
  // 代理路由表 : path -> ProxySession
  std::unordered_map<std::string, ProxyRoute> proxy_routes_;
  std::unique_ptr<HealthChecker> health_checker_;
};

} // namespace runtime::gateway