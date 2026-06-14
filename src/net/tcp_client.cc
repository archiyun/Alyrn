// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "runtime/net/tcp_client.h"

#include <atomic>
#include <cstdlib>
#include <cstdio>

#include "runtime/log/logger.h"
#include "runtime/net/connector.h"
#include "runtime/net/net_utils.h"

namespace runtime::net {

TcpClient::TcpClient(EventLoop* loop,
                     const InetAddress& server_addr,
                     std::string name)
    : loop_(loop),
      server_addr_(server_addr),
      name_(std::move(name)),
      connector_(std::make_shared<Connector>(loop, server_addr)) {
  connector_->set_connection_callback(
      [this](int sockfd) { NewConnection(sockfd); });
  LOG_INFO() << "tcp client created: name=" << name_
             << " server=" << server_addr_.ToIpPort();
}

TcpClient::~TcpClient() {
  // 停止 Connector（防止重试定时器在析构后触发）
  connector_->Stop();

  TcpConnectionPtr conn = connection_;
  if (conn) {
    conn->loop()->RunInLoop([conn] { conn->ConnectDestroyed(); });
  }
}

void TcpClient::Connect() {
  connect_.store(true);
  connector_->Start();
  LOG_INFO() << "tcp client connecting: name=" << name_
             << " server=" << server_addr_.ToIpPort();
}

void TcpClient::Disconnect() {
  connect_.store(false);
  if (connection_) {
    connection_->Shutdown();
  }
}


void TcpClient::NewConnection(int sockfd) {
  // 此函数在 loop_ 线程中被调用（由 Connector::handleWrite 触发）
  // 与 TcpServer::NewConnection 对称，区别是没有线程池，直接用 loop_

  auto local_addr = get_local_addr(sockfd);
  if (!local_addr) {
    LOG_FATAL() << "getsockname failed for connected socket: fd=" << sockfd
                << " error=" << local_addr.error.value()
                << " message=" << local_addr.error.message();
    std::abort();
  }
  InetAddress peer_addr(server_addr_);

  // 生成唯一连接名，便于日志追踪
  static std::atomic<int> conn_id{1};
  char buf[32];
  std::snprintf(buf, sizeof(buf), "#%d", conn_id.fetch_add(1));
  std::string conn_name = name_ + buf;

  TcpConnectionPtr conn = std::make_shared<TcpConnection>(
      loop_, conn_name, sockfd, *local_addr.value, peer_addr);

  conn->set_connection_callback(connection_callback_);
  conn->set_message_callback(message_callback_);
  conn->set_write_complete_callback(write_complete_callback_);
  conn->set_close_callback(
      [this](const TcpConnectionPtr& c) { RemoveConnection(c); });

  connection_ = conn;

  // ConnectEstablished 向 epoll 注册读事件，并触发 connection_callback_
  conn->ConnectEstablished();

  LOG_INFO() << "tcp client new connection: name=" << conn_name
             << " local=" << local_addr.value->ToIpPort()
             << " peer=" << peer_addr.ToIpPort();
}

void TcpClient::RemoveConnection(const TcpConnectionPtr& conn) {
  // CloseCallback 在 conn 所在的 loop_ 线程中触发，此处直接清理
  connection_.reset();

  // ConnectDestroyed 从 epoll 注销 channel，必须在 loop 线程执行
  loop_->QueueInLoop([conn] { conn->ConnectDestroyed(); });

  LOG_INFO() << "tcp client connection closed: name=" << conn->name()
             << " peer=" << conn->peer_address().ToIpPort();
}

}  // namespace runtime::net
