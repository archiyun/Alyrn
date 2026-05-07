#pragma once

#include "runtime/gateway/backend.h"
#include "runtime/net/buffer.h"
#include "runtime/net/tcp_connection.h"
#include "runtime/net/tcp_client.h"
#include "runtime/http/http_request.h"

#include <memory>
#include <string>

namespace runtime::gateway {

using TcpConnectionPtr = runtime::net::TcpConnection::TcpConnectionPtr;

class ProxySession : public std::enable_shared_from_this<ProxySession> {
public:
  ProxySession(const TcpConnectionPtr& client_conn, 
               std::shared_ptr<Backend> backend, 
               std::string upstream_request);
  ~ProxySession();

  void Start();
private:
  void OnUpstreamConnChange(const TcpConnectionPtr& up_conn);
  void OnUpstreamMessage(const TcpConnectionPtr& up_conn, runtime::net::Buffer& buf, runtime::time::Timestamp ts);
  void Send502();

  std::weak_ptr<runtime::net::TcpConnection> client_weak_;
  std::unique_ptr<runtime::net::TcpClient> upstream_;
  std::shared_ptr<Backend> backend_;
  std::string upstream_request_;
  bool responded_;
};
// 无状态工厂：为每个代理请求创建一个 ProxySession。
// Forward 返回 session，调用方必须持有它，否则上游连接在 Start() 前就析构了。
class ProxyPass {
public:
  using TcpConnectionPtr = runtime::net::TcpConnection::TcpConnectionPtr;

  static std::shared_ptr<ProxySession>
  Forward(const TcpConnectionPtr& client_conn,
          const runtime::http::HttpRequest& request,
          std::shared_ptr<Backend> backend);

  static std::string BuildRequest(const runtime::http::HttpRequest& req,
                                  const Backend& backend);
};

}  // namespace runtime::gateway