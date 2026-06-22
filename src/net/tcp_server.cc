// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/net/tcp_server.h"

#include <cstdlib>
#include <cstdio>

#include "vexo/log/logger.h"
#include "vexo/net/event_loop.h"
#include "vexo/net/net_utils.h"

namespace vexo::net {

TcpServer::TcpServer(
    EventLoop* loop,
    const InetAddress& listenaddr,
    const std::string& name)
    : loop_(loop),
      name_(name),
      acceptor_(std::make_unique<Acceptor>(loop, listenaddr, false)),
      started_(false),
      et_mode_(false),
      next_conn_id_(1),
      sub_loop_num_(0) {
    LOG_INFO() << "tcp server created: name=" << name_;

    acceptor_->set_new_connection_callback(
        [this](int sockfd, const InetAddress& peeraddr) {
            NewConnection(sockfd, peeraddr);
        });
}

TcpServer::~TcpServer() {
    LOG_INFO() << "tcp server destroying: name=" << name_
               << " active_connections=" << connections_.size();
  for (auto& [_, conn_ptr] : connections_) {
    TcpConnectionPtr conn(conn_ptr);
    conn->ConnectDestroyed();
  }
}

void TcpServer::Start() {
  if (!started_) {
    started_ = true;

    thread_pool_ = std::make_unique<EventLoopThreadPool>(loop_, sub_loop_num_);
    if (thread_init_callback_) {
      thread_pool_->Start(thread_init_callback_);
    } else {
      thread_pool_->Start();
    }
    LOG_INFO() << "tcp server starting: name=" << name_
               << " sub_loops=" << sub_loop_num_;
    loop_->RunInLoop([this] {
      acceptor_->set_edge_triggered(et_mode_);
      acceptor_->Listen();
    });
  }
}

void TcpServer::NewConnection(int sockfd, const InetAddress& peeraddr) {
  EventLoop* ioLoop = thread_pool_->GetNextLoop();

  char buf[64];
  std::snprintf(buf, sizeof(buf), "#%d", next_conn_id_++);
  std::string conn_name = name_ + buf;

  auto localaddr = get_local_addr(sockfd);
  if (!localaddr) {
    LOG_FATAL() << "getsockname failed for accepted socket: fd=" << sockfd
                << " error=" << localaddr.error.value()
                << " message=" << localaddr.error.message();
    std::abort();
  }

  TcpConnectionPtr conn = std::make_shared<TcpConnection>(
      ioLoop, conn_name, sockfd, *localaddr.value, peeraddr);

  connections_[conn_name] = conn;

  LOG_INFO() << "new tcp connection: name=" << conn_name
             << " local=" << localaddr.value->ToIpPort()
             << " peer=" << peeraddr.ToIpPort();

  conn->set_connection_callback(connection_callback_);
  conn->set_message_callback(message_callback_);
  conn->set_write_complete_callback(write_complete_callback_);
  conn->set_edge_triggered(et_mode_);
  conn->set_tcp_no_delay(true);

  conn->set_close_callback(
      [this](const TcpConnectionPtr& connection) { RemoveConnection(connection); });

  // Connection establishment must run in the owning I/O loop so Channel
  // registration and callback dispatch happen on the correct thread.
  ioLoop->RunInLoop([conn] { conn->ConnectEstablished(); });
}

void TcpServer::RemoveConnection(const TcpConnectionPtr& conn) {
  // Connection ownership lives in TcpServer, so removal is always serialized
  // back onto the base loop before the final destruction step is queued to the
  // connection's owning I/O loop.
  loop_->RunInLoop([this, conn] { RemoveConnectionInLoop(conn); });
}

void TcpServer::RemoveConnectionInLoop(const TcpConnectionPtr& conn) {
  LOG_INFO() << "removing tcp connection: name=" << conn->name()
             << " peer=" << conn->peer_address().ToIpPort();
  connections_.erase(conn->name());

  EventLoop* ioLoop = conn->loop();
  ioLoop->QueueInLoop([conn] { conn->ConnectDestroyed(); });
}

}  // namespace vexo::net
