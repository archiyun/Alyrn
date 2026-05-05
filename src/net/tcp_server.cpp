#include "runtime/net/tcp_server.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/net_utils.h"
#include "runtime/log/logger.h"

#include <cstdio>

namespace runtime::net {

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
      thread_num_(0) {
    LOG_INFO() << "tcp server created: name=" << name_;

    acceptor_->SetNewConnectionCallback(
        [this](int sockfd, const InetAddress& peeraddr) {
            NewConnection(sockfd, peeraddr);
        });
}

TcpServer::~TcpServer() {
    LOG_INFO() << "tcp server destroying: name=" << name_
               << " active_connections=" << connections_.size();
  for (auto& item : connections_) {
    TcpConnectionPtr conn(item.second);
    conn->ConnectDestroyed();
  }
}

void TcpServer::Start() {
  if (!started_) {
    started_ = true;

    thread_pool_ = std::make_unique<EventLoopThreadPool>(loop_, thread_num_);
    thread_pool_->Start(thread_init_callback_);
    LOG_INFO() << "tcp server starting: name=" << name_
               << " io_threads=" << thread_num_;
    loop_->RunInLoop([this] {
      acceptor_->SetEdgeTriggered(et_mode_);
      acceptor_->Listen();
    });
  }
}

void TcpServer::NewConnection(int sockfd, const InetAddress& peeraddr) {
  EventLoop* ioLoop = thread_pool_->GetNextLoop();

  char buf[64];
  std::snprintf(buf, sizeof(buf), "#%d", next_conn_id_++);
  std::string conn_name = name_ + buf;

  InetAddress localaddr(GetLocalAddr(sockfd));

  TcpConnectionPtr conn = std::make_shared<TcpConnection>(
      ioLoop, conn_name, sockfd, localaddr, peeraddr);

  connections_[conn_name] = conn;

  LOG_INFO() << "new tcp connection: name=" << conn_name
             << " local=" << localaddr.ToIpPort()
             << " peer=" << peeraddr.ToIpPort();

  conn->SetConnectionCallback(connection_callback_);
  conn->SetMessageCallback(message_callback_);
  conn->SetWriteCompleteCallback(write_complete_callback_);
  conn->SetEdgeTriggered(et_mode_);

  conn->SetCloseCallback(
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
  LOG_INFO() << "removing tcp connection: name=" << conn->Name()
             << " peer=" << conn->PeerAddress().ToIpPort();
  connections_.erase(conn->Name());

  EventLoop* ioLoop = conn->GetLoop();
  ioLoop->QueueInLoop([conn] { conn->ConnectDestroyed(); });
}

}  // namespace runtime::net
