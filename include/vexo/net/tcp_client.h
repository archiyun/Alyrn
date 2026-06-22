// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <utility>

#include "vexo/base/noncopyable.h"
#include "vexo/net/inet_address.h"
#include "vexo/net/tcp_connection.h"

namespace vexo::net {

class Connector;   
class EventLoop;

// TcpClient manages one client-side TCP connection.
//
// Connector is responsible for non-blocking connect/retry.
// After the connection is established, TcpClient wraps the socket fd into
// TcpConnection and installs user callbacks.
class TcpClient : public vexo::base::NonCopyable {
public:
  using TcpConnectionPtr      = TcpConnection::TcpConnectionPtr;
  using ConnectionCallback    = TcpConnection::ConnectionCallback;
  using MessageCallback       = TcpConnection::MessageCallback;
  using WriteCompleteCallback = TcpConnection::WriteCompleteCallback;

  TcpClient(EventLoop* loop, const InetAddress& server_addr, std::string name);
  ~TcpClient();

  void Connect();
  void Disconnect();
  void set_retry_enabled(bool enabled);

  TcpConnectionPtr connection() const { return connection_; }

  void set_connection_callback(ConnectionCallback cb) {
    connection_callback_ = std::move(cb);
  }
  void set_message_callback(MessageCallback cb) {
    message_callback_ = std::move(cb);
  }
  void set_write_complete_callback(WriteCompleteCallback cb) {
    write_complete_callback_ = std::move(cb);
  }

private:
  void NewConnection(int sockfd);

  void RemoveConnection(const TcpConnectionPtr& conn);

  EventLoop*        loop_;
  const InetAddress server_addr_;
  std::string       name_;
  std::atomic<bool> connect_{false};

  std::shared_ptr<Connector> connector_;
  TcpConnectionPtr           connection_;

  ConnectionCallback    connection_callback_;
  MessageCallback       message_callback_;
  WriteCompleteCallback write_complete_callback_;
};

}  // namespace vexo::net
