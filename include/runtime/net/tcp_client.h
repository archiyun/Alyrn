// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/net/inet_address.h"
#include "runtime/net/tcp_connection.h"

#include <atomic>
#include <memory>
#include <utility>
#include <string>

namespace runtime::net {

class Connector;   
class EventLoop;

// TcpClient manages one client-side TCP connection.
//
// Connector is responsible for non-blocking connect/retry.
// After the connection is established, TcpClient wraps the socket fd into
// TcpConnection and installs user callbacks.
class TcpClient : public runtime::base::NonCopyable {
public:
  using TcpConnectionPtr      = TcpConnection::TcpConnectionPtr;
  using ConnectionCallback    = TcpConnection::ConnectionCallback;
  using MessageCallback       = TcpConnection::MessageCallback;
  using WriteCompleteCallback = TcpConnection::WriteCompleteCallback;

  TcpClient(EventLoop* loop, const InetAddress& server_addr, std::string name);
  ~TcpClient();

  void Connect();
  void Disconnect();

  TcpConnectionPtr connection() const { return connection_; }

  void SetConnectionCallback(ConnectionCallback cb) {
    connection_callback_ = std::move(cb);
  }
  void SetMessageCallback(MessageCallback cb) {
    message_callback_ = std::move(cb);
  }
  void SetWriteCompleteCallback(WriteCompleteCallback cb) {
    write_complete_callback_ = std::move(cb);
  }

private:
  // Connector 建立连接后回调此函数，把 sockfd 包装成 TcpConnection
  void NewConnection(int sockfd);

  // TcpConnection 关闭时的清理回调
  void RemoveConnection(const TcpConnectionPtr& conn);

  EventLoop*        loop_;
  const InetAddress server_addr_;
  std::string       name_;
  std::atomic<bool> connect_{false};

  std::shared_ptr<Connector> connector_;  // shared_ptr：Connector 内部用 shared_from_this
  TcpConnectionPtr           connection_;

  ConnectionCallback    connection_callback_;
  MessageCallback       message_callback_;
  WriteCompleteCallback write_complete_callback_;
};

}  // namespace runtime::net
