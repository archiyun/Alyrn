// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "runtime/gateway/load_balancer.h"
#include "runtime/gateway/upstream.h"
#include "runtime/gateway/upstream_peer.h"
#include "runtime/gateway/upstream_conn_pool.h"
#include "runtime/http/http_request.h"
#include "runtime/http/http_types.h"
#include "runtime/net/buffer.h"
#include "runtime/net/tcp_connection.h"
#include "runtime/net/tcp_client.h"

#include <memory>
#include <string>

namespace runtime::gateway {

enum class Phase : uint8_t {
  kConnecting,    // TcpClient::Connect() 已调用， 等待 OnUpstreamConnChange(connected);
  kSendingRequest,// upstream 连接建立， upstream_request_ 已 Send, 等待响应
  kReadingHeaders,// 收到第一批数据， 正在解析 upstream 响应头
  kForwardingBody,// 响应头已转发给 client, 流式透传 body
  kDone,          // 响应完成或者连接已关闭。
};

// 响应体的边界确定方式。决定何时把上游连接归还到 pool。
enum class BodyFraming : uint8_t {
  kCloseDelimited,  // 没有 Content-Length 也不是 chunked -> 等 upstream 关闭
  kContentLength,   // 按 Content-Length 计字节
  kChunked,         // Transfer-Encoding: chunked，v1 仍按 close 处理（不复用）
  kNoBody,          // 1xx / 204 / 304 / HEAD 响应：肯定没 body
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
// 无状态工厂：为每个代理请求创建一个 UpstreamRequest。
// Forward 返回 upstream_request，调用方必须持有它，否则上游连接在 Start() 前就析构了。
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
