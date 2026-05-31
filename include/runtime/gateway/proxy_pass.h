// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <memory>
#include <string>

#include "runtime/gateway/load_balancer.h"
#include "runtime/gateway/upstream.h"
#include "runtime/gateway/upstream_conn_pool.h"
#include "runtime/gateway/upstream_peer.h"
#include "runtime/http/http_request.h"
#include "runtime/http/http_types.h"
#include "runtime/net/buffer.h"
#include "runtime/net/tcp_client.h"
#include "runtime/net/tcp_connection.h"

namespace runtime::gateway {

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
  using TcpConnectionPtr = runtime::net::TcpConnection::TcpConnectionPtr;

  UpstreamRequest(const TcpConnectionPtr& client_conn,
                  Upstream& upstream,
                  LoadBalancer& lb,
                  UpstreamConnPool& pool,
                  std::shared_ptr<UpstreamPeer> first_peer,
                  RequestContext request_ctx,
                  std::string request_bytes,
                  CircuitBreaker* cb = nullptr,
                  int max_retries = 2,
                  runtime::http::Method request_method = runtime::http::Method::Get);
  ~UpstreamRequest();

  void Start();
private:
  static void RewriteHeaders(std::string_view raw_headers, std::string& out);
  void ParseFraming(std::string_view raw_headers, int status);
  void ConnectTo(std::shared_ptr<UpstreamPeer> peer);
  void ConnectToWithPool(std::shared_ptr<UpstreamPeer> peer,
                         std::unique_ptr<runtime::net::TcpClient> pooled_client);
  void AttachCallbacks();
  void OnUpstreamConnChange(const TcpConnectionPtr& up_conn);
  void OnUpstreamMessage(const TcpConnectionPtr& up_conn, runtime::net::Buffer& buf, runtime::time::Timestamp ts);
  void Finalize();
  void Send502();

  std::weak_ptr<runtime::net::TcpConnection> client_weak_;
  std::unique_ptr<runtime::net::TcpClient>   upstream_conn_;
  std::shared_ptr<UpstreamPeer>              peer_;
  Upstream&                                  upstream_;
  LoadBalancer&                              lb_;
  UpstreamConnPool&                          pool_;
  RequestContext                             request_ctx_;
  std::string                                request_bytes_;
  int                                        retries_left_;
  Phase                                      phase_{Phase::kConnecting};
  CircuitBreaker*                            cb_{nullptr};
  BodyFraming                                framing_{BodyFraming::kCloseDelimited};
  uint64_t                                   body_remaining_{0};
  bool                                       upstream_keepalive_{false};
  runtime::http::Method                      request_method_{runtime::http::Method::Invalid};
};
// Stateless proxy factory. Each forwarded request is represented by one
// UpstreamRequest instance.
//
// Forward returns the request object to the caller, which must keep it alive
// until completion; otherwise the upstream connection may be destroyed before
// Start() can finish wiring callbacks.
class ProxyPass {
public:
  using TcpConnectionPtr = runtime::net::TcpConnection::TcpConnectionPtr;

  static std::shared_ptr<UpstreamRequest>
  Forward(const TcpConnectionPtr& client_conn,
          const runtime::http::HttpRequest& request,
          Upstream& upstream,
          LoadBalancer& lb,
          UpstreamConnPool& pool,
          const RequestContext& ctx = {},
          CircuitBreaker* cb = nullptr);

  static std::string BuildRequest(const runtime::http::HttpRequest& req,
                                  const UpstreamPeer& peer);
};

}  // namespace runtime::gateway
