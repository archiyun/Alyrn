// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "coropact/gateway/circuit_breaker.h"
#include "coropact/gateway/fallback_config.h"
#include "coropact/gateway/forwarded_header_context.h"
#include "coropact/gateway/load_balancer.h"
#include "coropact/gateway/rate_limiter.h"
#include "coropact/gateway/upstream.h"
#include "coropact/gateway/upstream_registry.h"
#include "coropact/http/http_request.h"
#include "coropact/http/http_response.h"
#include "coropact/http/http_types.h"
#include "coropact/http/parse_status.h"
#include "coropact/http/router.h"
#include "coropact/utils/macros.h"

namespace coropact::gateway {

enum class GatewayActionKind : uint8_t {
  Send,
  Proxy,
};

class GatewayCore {
public:
  COROPACT_DELETE_COPY_MOVE(GatewayCore);

  using Handler = coropact::http::Handler;

  enum class RouteType : uint8_t {
    Direct,
    Proxy,
  };

  enum class MatchType : uint8_t {
    Exact,
    Prefix,
  };

  struct Route {
    RouteType type;
    MatchType match_type{MatchType::Exact};

    coropact::http::Method method;
    bool match_all_methods{false};

    std::string path;

    Handler handler;
    std::string upstream_name;
    std::unique_ptr<LoadBalancer> lb;

    FallbackConfig fallback;
    bool circuit_breaker_enabled{false};
  };

  struct ProxyTarget {
    const Route* route{nullptr};
    std::shared_ptr<Upstream> upstream;
    LoadBalancer* load_balancer{nullptr};
    RequestContext request_ctx;
    CircuitBreaker* circuit_breaker{nullptr};
    std::string client_ip;
    std::string request_id;
  };

  struct Action {
    GatewayActionKind kind{GatewayActionKind::Send};
    std::string response;
    bool close_after_send{false};
    ProxyTarget proxy;
  };

  GatewayCore(std::string name, UpstreamRegistry& registry);

  const std::string& name() const { return name_; }

  void Get(std::string_view path, Handler handler);
  void Post(std::string_view path, Handler handler);

  void AddProxyRoute(std::string_view path, std::string_view upstream_name,
                     std::string_view algo = "p2c");
  void AddProxyRoute(std::string_view path, std::string_view upstream_name, FallbackConfig fallback,
                     bool circuit_breaker_enabled = false, std::string_view algo = "p2c");

  void EnableRateLimit(RateLimiterConfig cfg);
  void EnableGlobalRateLimit(double rate, double burst);
  void EnablePerIPRateLimit(double rate, double burst);

  const Route* MatchRoute(std::string_view path) const;

  Action HandleParseError(coropact::http::ParseStatus parse_status);
  Action HandleRequest(const coropact::http::HttpRequest& req, std::string_view client_ip);

  Action ProxyUnavailable(const Route& route, std::string_view reason);

  ForwardedHeaderContext MakeForwardedContext(const ProxyTarget& proxy) const;

private:
  coropact::http::HttpResponse MakeError(coropact::http::StatusCode code, std::string_view msg) const;
  std::string RenderFallback(const Route& route, std::string_view reason) const;
  Action SendResponse(std::string response, bool close_after_send = false) const;
  static std::string GenRequestId();

  std::string name_;
  UpstreamRegistry& registry_;
  std::vector<Route> routes_;
  std::unique_ptr<RateLimiter> rate_limiter_;
  std::string rate_limit_response_429_;
  RateLimiterConfig rate_limiter_cfg_;
};

}  // namespace coropact::gateway
