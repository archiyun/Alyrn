// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/net/reactor_backend.h"

#include <memory>
#include <utility>

#include "vexo/log/logger.h"
#include "vexo/net/tcp_connection.h"
#include "vexo/time/timestamp.h"

namespace vexo::net {

void ReactorServer::Start() {
  server_.set_connection_callback(
    [this](const TcpConnection::TcpConnectionPtr& c) { OnConnection(c); });
  server_.set_message_callback(
    [this](const TcpConnection::TcpConnectionPtr& c, Buffer& b,
           vexo::time::Timestamp t) { OnMessage(c, b, t); });
  server_.Start();
}

void ReactorServer::OnConnection(const TcpConnection::TcpConnectionPtr& conn) {
  if (conn->Connected()) {
    auto adapter = std::make_shared<ReactorConn>(conn);
    conn->set_context(adapter);
    if (connection_callback_) connection_callback_(adapter);
  } else if (connection_callback_ && conn->context().has_value()) {
    connection_callback_(
      std::any_cast<const std::shared_ptr<ReactorConn>&>(conn->context()));
  }
}

void ReactorServer::OnMessage(const TcpConnection::TcpConnectionPtr& conn,
                              Buffer& buf, vexo::time::Timestamp ts) {
  if (!message_callback_) return;
  message_callback_(
    std::any_cast<const std::shared_ptr<ReactorConn>&>(conn->context()), buf, ts);
}

std::unique_ptr<IServer> MakeServer(Backend backend, EventLoop* loop,
                                    const InetAddress& addr, std::string name) {
  switch (backend) {
    case Backend::kEpoll:
      return std::make_unique<ReactorServer>(loop, addr, std::move(name));
    case Backend::kUring:
      LOG_ERROR() << "io_backend: kUring not yet available; using epoll";
      return std::make_unique<ReactorServer>(loop, addr, std::move(name));
  }
  return nullptr;
}
};
