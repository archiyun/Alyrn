#pragma once

#include <array>
#include <cstddef>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include "vexo/coro/task.h"
#include "vexo/gateway/fallback_config.h"
#include "vexo/gateway/gateway_core.h"
#include "vexo/gateway/proxy_pass.h"
#include "vexo/gateway/rate_limiter.h"
#include "vexo/gateway/upstream_conn_pool.h"
#include "vexo/gateway/upstream_registry.h"
#include "vexo/http/http_parser.h"
#include "vexo/http/http_request.h"
#include "vexo/http/parse_status.h"
#include "vexo/io/async_stream.h"
#include "vexo/io/stream_algorithms.h"

namespace vexo::gateway {

// Backend-neutral gateway session layer.
//
// The owning server supplies one accepted client stream and one connector
// bound to that stream's I/O loop. This class intentionally owns neither an
// accept loop nor a backend object. Callers that have a worker-local lifetime
// boundary should use the overload taking an UpstreamStreamPool reference so
// loop-bound upstream streams can be reused by all sessions on that worker
// without crossing worker/ring boundaries. The two-argument overload remains
// available and keeps the historical per-session pool behavior.
template <vexo::io::AsyncStream ClientStream, UpstreamConnector Connector>
class GatewaySessionService {
public:
  using Handler = GatewayCore::Handler;
  using Route = GatewayCore::Route;
  using UpstreamStream = typename Connector::Stream;
  using Pool = UpstreamStreamPool<UpstreamStream>;

  GatewaySessionService(std::string name, UpstreamRegistry& registry, PoolConfig pool_config = {})
      : core_(std::move(name), registry), pool_config_(pool_config) {}

  void Get(std::string_view path, Handler handler) { core_.Get(path, std::move(handler)); }

  void Post(std::string_view path, Handler handler) { core_.Post(path, std::move(handler)); }

  void AddProxyRoute(std::string_view path, std::string_view upstream_name,
                     std::string_view algo = "p2c") {
    core_.AddProxyRoute(path, upstream_name, algo);
  }

  void AddProxyRoute(std::string_view path, std::string_view upstream_name, FallbackConfig fallback,
                     bool circuit_breaker_enabled = false, std::string_view algo = "p2c") {
    core_.AddProxyRoute(path, upstream_name, std::move(fallback), circuit_breaker_enabled, algo);
  }

  void EnableRateLimit(RateLimiterConfig cfg) { core_.EnableRateLimit(std::move(cfg)); }

  void EnableGlobalRateLimit(double rate, double burst) {
    core_.EnableGlobalRateLimit(rate, burst);
  }

  void EnablePerIPRateLimit(double rate, double burst) { core_.EnablePerIPRateLimit(rate, burst); }

  void set_pool_config(PoolConfig cfg) { pool_config_ = cfg; }

  const Route* MatchRoute(std::string_view path) const { return core_.MatchRoute(path); }

  // Serve one accepted connection. The connector is moved into the coroutine
  // frame so a loop-bound connector cannot dangle after the handler returns.
  // The caller must construct it from the same loop that owns stream.
  coro::Task<void> Serve(std::unique_ptr<ClientStream> stream, Connector connector) {
    Pool pool(pool_config_);
    co_await ServeWithPool(std::move(stream), std::move(connector), pool);
  }

  // Serve one accepted connection using a pool owned by the current worker.
  // The pool and all streams acquired from it must remain alive until every
  // session on that worker has stopped. It must not be shared across I/O
  // loops because upstream streams are loop-bound.
  coro::Task<void> Serve(std::unique_ptr<ClientStream> stream, Connector connector, Pool& pool) {
    co_await ServeWithPool(std::move(stream), std::move(connector), pool);
  }

private:
  coro::Task<void> ServeWithPool(std::unique_ptr<ClientStream> stream, Connector connector,
                                 Pool& pool) {
    if (!stream) {
      co_return;
    }

    http::HttpParser parser;
    // Requests are fed incrementally, so the per-connection buffer does not
    // need to be large enough for the complete HTTP request. Keeping this at
    // 4 KiB materially reduces the frame footprint at high connection counts.
    std::array<std::byte, 4096> read_buffer{};
    const std::string client_ip = ClientIp(*stream);

    for (;;) {
      auto read = co_await stream->ReadSome(read_buffer);
      if (!read.has_value() || *read == 0) {
        break;
      }

      const std::string_view bytes(reinterpret_cast<const char*>(read_buffer.data()), *read);
      auto parse_status = parser.Feed(bytes);
      if (!co_await HandleParseStatus(*stream, parse_status)) {
        co_await stream->Close();
        co_return;
      }

      while (parser.GotAll()) {
        http::HttpRequest request = parser.TakeRequest();
        auto action = core_.HandleRequest(request, client_ip);
        if (!co_await ApplyAction(*stream, request, std::move(action), pool, connector)) {
          co_await stream->Close();
          co_return;
        }

        parse_status = parser.ParseAvailable();
        if (!co_await HandleParseStatus(*stream, parse_status)) {
          co_await stream->Close();
          co_return;
        }
      }
    }

    co_await stream->Close();
  }

  static std::span<const std::byte> Bytes(std::string_view text) {
    return std::as_bytes(std::span<const char>(text.data(), text.size()));
  }

  static std::string ClientIp(const ClientStream& stream) {
    if constexpr (requires { stream.PeerAddress().ToIp(); }) {
      return stream.PeerAddress().ToIp();
    } else {
      return {};
    }
  }

  coro::Task<bool> HandleParseStatus(ClientStream& stream, http::ParseStatus status) {
    if (status == http::ParseStatus::Continue || status == http::ParseStatus::GotAll) {
      co_return true;
    }

    auto action = core_.HandleParseError(status);
    auto written = co_await io::WriteAll(stream, Bytes(action.response));
    if (action.close_after_send && written.has_value()) {
      co_await stream.Shutdown();
    }
    co_return false;
  }

  coro::Task<bool> ApplyAction(ClientStream& stream, const http::HttpRequest& request,
                               GatewayCore::Action action, Pool& pool,
                               Connector& connector) {
    if (action.kind == GatewayActionKind::Send) {
      auto written = co_await io::WriteAll(stream, Bytes(action.response));
      if (!written.has_value()) {
        co_return false;
      }
      if (action.close_after_send) {
        co_await stream.Shutdown();
        co_return false;
      }
      co_return true;
    }

    auto result = co_await ProxyPass::Forward(
        stream, request, *action.proxy.upstream, *action.proxy.load_balancer, pool, connector,
        action.proxy.request_ctx, action.proxy.circuit_breaker,
        core_.MakeForwardedContext(action.proxy));

    if (result.status == ProxyForwardStatus::kNoPeer) {
      auto fallback = core_.ProxyUnavailable(*action.proxy.route, "no available upstream peer");
      co_await io::WriteAll(stream, Bytes(fallback.response));
      co_return true;
    }

    if (result.status == ProxyForwardStatus::kClientClosed ||
        result.status == ProxyForwardStatus::kUpstreamFailed) {
      co_return false;
    }
    co_return true;
  }

  GatewayCore core_;
  PoolConfig pool_config_{};
};

}  // namespace vexo::gateway
