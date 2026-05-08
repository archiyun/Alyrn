#pragma once

#include "runtime/gateway/upstream_peer.h"
#include "runtime/gateway/upstream.h"
#include "runtime/net/buffer.h"
#include "runtime/net/tcp_connection.h"
#include "runtime/net/tcp_client.h"
#include "runtime/http/http_request.h"

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
class UpstreamRequest : public std::enable_shared_from_this<UpstreamRequest> {
public:
  using TcpConnectionPtr = runtime::net::TcpConnection::TcpConnectionPtr;
  
  UpstreamRequest(const TcpConnectionPtr& client_conn, 
               std::shared_ptr<UpstreamPeer> peer,
               std::string request_bytes);
  ~UpstreamRequest();

  void Start();
private:
  static std::string RewriteHeaders(std::string_view raw_headers);
  void OnUpstreamConnChange(const TcpConnectionPtr& up_conn);
  void OnUpstreamMessage(const TcpConnectionPtr& up_conn, runtime::net::Buffer& buf, runtime::time::Timestamp ts);
  void Send502();

  std::weak_ptr<runtime::net::TcpConnection> client_weak_;
  std::unique_ptr<runtime::net::TcpClient> upstream_;
  std::shared_ptr<UpstreamPeer> peer_;
  std::string request_bytes_;
  Upstream& upstream_;

  Phase phase_{Phase::kConnecting};
};
// 无状态工厂：为每个代理请求创建一个 UpstreamRequest。
// Forward 返回 upstream_request，调用方必须持有它，否则上游连接在 Start() 前就析构了。
class ProxyPass {
public:
  using TcpConnectionPtr = runtime::net::TcpConnection::TcpConnectionPtr;

  static std::shared_ptr<UpstreamRequest>
  Forward(const TcpConnectionPtr& client_conn,
          const runtime::http::HttpRequest& request,
          std::shared_ptr<UpstreamPeer> peer);

  static std::string BuildRequest(const runtime::http::HttpRequest& req,
                                  const UpstreamPeer& peer);
};

}  // namespace runtime::gateway
