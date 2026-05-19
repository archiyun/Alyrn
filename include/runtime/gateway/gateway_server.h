#pragma once

#include "runtime/gateway/rate_limiter.h"
#include "runtime/gateway/fallback_config.h"
#include "runtime/gateway/health_checker.h"
#include "runtime/gateway/load_balancer.h"
#include "runtime/gateway/proxy_pass.h"
#include "runtime/gateway/upstream_conn_pool.h"
#include "runtime/gateway/upstream_registry.h"
#include "runtime/metrics/gateway_metrics.h"
#include "runtime/http/http_context.h"
#include "runtime/http/http_response.h"
#include "runtime/http/http_types.h"
#include "runtime/http/router.h"
#include "runtime/net/inet_address.h"
#include "runtime/net/tcp_server.h"
#include "runtime/time/timestamp.h"
#include "runtime/base/noncopyable.h"

#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

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

  // 匹配类型
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

    FallbackConfig fallback; // fallback
    bool circuit_breaker_enabled{false}; // circuit_break
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
  // NEW: 带降级和熔断配置
  void AddProxyRoute(std::string_view path,
                     std::string_view upstream_name,
                     FallbackConfig fallback,
                     bool circuit_breaker_enabled = false,
                     std::string_view algo = "round_robin");
  void EnableHealthCheck(HealthCheckConfig cfg = {});
  // NEW: 限流配置
  void EnableGlobalRateLimit(double rate, double burst);
  void EnablePerIPRateLimit(double rate, double burst);
  void SetPoolConfig(PoolConfig cfg) { pool_cfg_ = cfg; }

  // 注册一条 GET 直接路由, 返回当前 GatewayMetrics 的 Prometheus 文本.
  // 调用方应在 Start() 之前调用. path 默认 "/metrics".
  void EnableMetricsEndpoint(std::string_view path = "/metrics");

  // 暴露指标对象, 调用方可读可写 (用于外部埋点 / 测试 / 自定义导出).
  runtime::metrics::GatewayMetrics&       Metrics()       { return metrics_; }
  const runtime::metrics::GatewayMetrics& Metrics() const { return metrics_; }

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
  UpstreamConnPool& GetOrCreatePool(runtime::net::EventLoop* loop);
  runtime::http::HttpResponse MakeError(runtime::http::StatusCode code,
                                        std::string_view msg) const;
  std::string RenderFallback(const Route& route,
                             std::string_view reason) const;
private:
  runtime::net::TcpServer server_;
  UpstreamRegistry& registry_;
  std::vector<Route> routes_;
  std::unique_ptr<HealthChecker> health_checker_;
  PoolConfig pool_cfg_;
  // pools_ 在 sub-loop 之间共享：每条 sub-loop 都会调 GetOrCreatePool。
  // 仅 map 结构本身需要保护，value (UpstreamConnPool) 的访问是 per-loop 单线程的。
  // unordered_map 的 reference 在插入时不会失效，所以拿到 reference 后即可释放锁。
  mutable std::mutex pools_mu_;
  std::unordered_map<runtime::net::EventLoop*, UpstreamConnPool> pools_;
  std::unique_ptr<RateLimiter> rate_limiter_;  // 限流器
  std::string rate_limit_response_429_; // 预渲染 429 响应
  RateLimiterConfig rate_limiter_cfg_;  // accumulated config, committed in Enable* calls
  runtime::metrics::GatewayMetrics metrics_;  // 运行时观测点, header-only 原子操作
};

} // namespace runtime::gateway
