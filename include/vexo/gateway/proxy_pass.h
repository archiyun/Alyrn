// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <memory>
#include <string>

#include "vexo/gateway/load_balancer.h"
#include "vexo/gateway/upstream.h"
#include "vexo/gateway/upstream_conn_pool.h"
#include "vexo/gateway/upstream_peer.h"
#include "vexo/http/http_request.h"
#include "vexo/http/http_types.h"
#include "vexo/net/buffer.h"
#include "vexo/net/tcp_client.h"
#include "vexo/net/tcp_connection.h"
#include "vexo/time/timer_id.h"

namespace vexo::gateway {

struct ForwardedHeaderContext {
  std::string_view client_ip;
  std::string_view scheme;
  std::string_view gateway_name;
  std::string_view request_id;
};

enum class Phase : uint8_t {
  kConnecting,      // TcpClient::Connect() has been called.
  kSendingRequest,  // Upstream is connected; request bytes have been sent.
  kReadingHeaders,  // First bytes received; parsing upstream response headers.
  kForwardingBody,  // Headers forwarded to the client; streaming body bytes.
  kDone,            // Response is complete or the connection has closed.
};

// Response-body framing mode. Determines when the upstream connection can be
// returned to the keepalive pool.
enum class BodyFraming : uint8_t {
  kCloseDelimited,  // No Content-Length and not chunked; wait for upstream EOF.
  kContentLength,   // Count body bytes according to Content-Length.
  kChunked,         // Transfer-Encoding: chunked; currently not pooled.
  kNoBody,          // 1xx, 204, 304, and HEAD responses never carry a body.
};

class UpstreamRequest : public std::enable_shared_from_this<UpstreamRequest> {
public:
  using TcpConnectionPtr = vexo::net::TcpConnection::TcpConnectionPtr;

  UpstreamRequest(const TcpConnectionPtr& client_conn, Upstream& upstream, LoadBalancer& lb,
                  UpstreamConnPool& pool, std::shared_ptr<UpstreamPeer> first_peer,
                  RequestContext request_ctx, std::string request_bytes,
                  CircuitBreaker* cb = nullptr, int max_retries = 2,
                  vexo::http::Method request_method = vexo::http::Method::Get);
  ~UpstreamRequest();

  void Start();

private:
  static void RewriteHeaders(std::string_view raw_headers, std::string& out);
  void ParseFraming(std::string_view raw_headers, int status);
  void ConnectTo(std::shared_ptr<UpstreamPeer> peer);
  void ConnectToWithPool(std::shared_ptr<UpstreamPeer> peer,
                         std::unique_ptr<vexo::net::TcpClient> pooled_client);
  void AttachCallbacks();
  void OnUpstreamConnChange(const TcpConnectionPtr& up_conn);
  void OnUpstreamMessage(const TcpConnectionPtr& up_conn, vexo::net::Buffer& buf,
                         vexo::time::Timestamp ts);
  void Finalize();
  void Send502();
  void ArmDeadline();
  void CancelDeadline();
  void OnDeadline();
  void ReleaseAccounting();
  // Reports the request outcome to the circuit breaker exactly once. A breaker
  // that admitted this request (especially a HALF_OPEN probe) requires a single
  // OnSuccess/OnFailure; the cb_reported_ guard makes every exit path idempotent.
  void ReportToBreaker(bool success);
  // Retry helper: pick any *other* available peer. lb_.Select() with the same
  // ctx re-picks the same node for hash-based strategies, so retries need this
  // to actually fail over.
  std::shared_ptr<UpstreamPeer> SelectFailoverPeer();

  std::weak_ptr<vexo::net::TcpConnection> client_weak_;
  std::unique_ptr<vexo::net::TcpClient> upstream_conn_;
  std::shared_ptr<UpstreamPeer> peer_;
  Upstream& upstream_;
  LoadBalancer& lb_;
  UpstreamConnPool& pool_;
  RequestContext request_ctx_;
  std::string request_bytes_;
  int retries_left_;
  Phase phase_{Phase::kConnecting};
  CircuitBreaker* cb_{nullptr};
  bool cb_reported_{false};
  BodyFraming framing_{BodyFraming::kCloseDelimited};
  uint64_t body_remaining_{0};
  bool upstream_keepalive_{false};
  vexo::http::Method request_method_{vexo::http::Method::Invalid};
  vexo::net::EventLoop* request_loop_{nullptr};
  vexo::time::TimerId deadline_timer_;
  bool deadline_armed_{false};
  bool accounting_released_{false};
};
// Stateless proxy factory. Each forwarded request is represented by one
// UpstreamRequest instance.
//
// Forward returns the request object to the caller, which must keep it alive
// until completion; otherwise the upstream connection may be destroyed before
// Start() can finish wiring callbacks.
class ProxyPass {
public:
  using TcpConnectionPtr = vexo::net::TcpConnection::TcpConnectionPtr;

  static std::shared_ptr<UpstreamRequest> Forward(
      const TcpConnectionPtr& client_conn, const vexo::http::HttpRequest& request,
      Upstream& upstream, LoadBalancer& lb, UpstreamConnPool& pool, const RequestContext& ctx = {},
      CircuitBreaker* cb = nullptr, ForwardedHeaderContext forwarded = {});

  static std::string BuildRequest(const vexo::http::HttpRequest& req, const UpstreamPeer& peer,
                                  ForwardedHeaderContext forwarded = {});
};

}  // namespace vexo::gateway
