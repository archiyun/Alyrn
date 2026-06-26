// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/net/connector.h"

#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstring>

#include "vexo/log/logger.h"
#include "vexo/net/channel.h"
#include "vexo/net/net_utils.h"

namespace vexo::net {

static constexpr double kMaxRetryDelaySec = 30.0;

Connector::Connector(EventLoop* loop, const InetAddress& server_addr)
    : loop_(loop), server_addr_(server_addr) {}

Connector::~Connector() { assert(!channel_); }

void Connector::Start() {
  stopped_.store(false, std::memory_order_release);
  auto self = shared_from_this();
  loop_->RunInLoop([self] { self->StartInLoop(); });
}

void Connector::Stop() {
  stopped_.store(true, std::memory_order_release);
  auto self = shared_from_this();
  loop_->RunInLoop([self] {
    if (self->state_ == ConnectorState::kConnecting) {
      self->state_ = ConnectorState::kDisConnected;
      int sockfd = self->RemoveAndResetChannel();
      ::close(sockfd);
    }
  });
}

void Connector::StartInLoop() {
  assert(loop_->IsInLoopThread());
  if (stopped_.load(std::memory_order_acquire)) return;
  assert(state_ == ConnectorState::kDisConnected);
  Connect();
}

void Connector::Connect() {
  if (stopped_.load(std::memory_order_acquire)) return;

  auto socket = CreateNonBlockingSocket();
  if (!socket) {
    LOG_ERROR() << "connector: CreateNonBlockingSocket failed: error="
                << socket.error().value() << " message=" << socket.error().message();
    return;
  }
  const int sockfd = *socket;

  const sockaddr_in addr = server_addr_.sock_addr();
  int ret = ::connect(sockfd, reinterpret_cast<const sockaddr*>(&addr),
                      static_cast<socklen_t>(sizeof(addr)));
  int saved_errno = (ret == 0) ? 0 : errno;

  switch (saved_errno) {  // 注意：用 saved_errno，不是 errno（errno 会被后续调用修改）
    case 0:
    case EINPROGRESS:  // 非阻塞 connect 的正常路径：握手进行中
    case EINTR:
    case EISCONN:
      Connecting(sockfd);
      break;

    case EAGAIN:
    case EADDRINUSE:
    case EADDRNOTAVAIL:
    case ECONNREFUSED:
    case ENETUNREACH:
      Retry(sockfd);
      break;

    default:
      LOG_ERROR() << "connector: connect to " << server_addr_.ToIpPort()
                  << " fatal error: " << std::strerror(saved_errno);
      ::close(sockfd);
      break;
  }
}

void Connector::Connecting(int sockfd) {
  state_ = ConnectorState::kConnecting;
  assert(!channel_);

  channel_ = std::make_unique<Channel>(loop_, sockfd);

  auto self = shared_from_this();
  channel_->set_write_callback([self] { self->handleWrite(); });
  channel_->set_error_callback([self] { self->handleError(); });
  channel_->EnableWriting();
}

void Connector::handleWrite() {
  assert(loop_->IsInLoopThread());

  if (state_ != ConnectorState::kConnecting) {
    return;  // Stop() 在触发前已将状态改为 kDisConnected
  }

  // 先从 epoll 摘除 channel，拿到 fd
  // 必须在交给 TcpConnection 之前完成，否则同一 fd 会有两个 channel 注册
  int sockfd = RemoveAndResetChannel();

  // EPOLLOUT 触发不代表 connect 成功，必须用 getsockopt 确认
  int err = 0;
  socklen_t len = static_cast<socklen_t>(sizeof(err));
  if (::getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
    err = errno;
  }

  if (err != 0) {
    LOG_WARN() << "connector: connect to " << server_addr_.ToIpPort()
               << " failed: " << std::strerror(err);
    Retry(sockfd);
    return;
  }

  // loopback 上内核可能将本地端口分配成和目标端口相同，形成自连接
  auto self_connect = IsSelfConnect(sockfd);
  if (!self_connect) {
    LOG_ERROR() << "connector: failed to inspect connected socket: fd=" << sockfd
                << " error=" << self_connect.error().value()
                << " message=" << self_connect.error().message();
    Retry(sockfd);
    return;
  }
  if (*self_connect) {
    LOG_WARN() << "connector: self-connect on " << server_addr_.ToIpPort() << ", retrying";
    Retry(sockfd);
    return;
  }

  state_ = ConnectorState::kConnected;
  retry_delay_sec_ = 0.5;  // 连接成功，重置退避计时器

  if (stopped_.load(std::memory_order_acquire)) {
    state_ = ConnectorState::kDisConnected;
    ::close(sockfd);
    return;
  }

  LOG_INFO() << "connector: connected to " << server_addr_.ToIpPort() << " fd=" << sockfd;

  if (new_connection_cb_) {
    new_connection_cb_(sockfd);
  } else {
    ::close(sockfd);
  }
}

void Connector::handleError() {
  assert(loop_->IsInLoopThread());

  if (state_ == ConnectorState::kConnecting) {
    int sockfd = RemoveAndResetChannel();
    int err = 0;
    socklen_t len = static_cast<socklen_t>(sizeof(err));
    ::getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &err, &len);
    LOG_ERROR() << "connector: error on " << server_addr_.ToIpPort() << ": " << std::strerror(err);
    Retry(sockfd);
  }
}

void Connector::Retry(int sockfd) {
  ::close(sockfd);
  state_ = ConnectorState::kDisConnected;

  if (stopped_.load(std::memory_order_acquire) || !retry_enabled_.load(std::memory_order_acquire) ||
      !new_connection_cb_) {
    return;
  }

  // 指数退避：0.5s → 1s → 2s → ... → 30s
  retry_delay_sec_ = std::min(retry_delay_sec_ * 2.0, kMaxRetryDelaySec);

  LOG_INFO() << "connector: retrying " << server_addr_.ToIpPort() << " in " << retry_delay_sec_
             << "s";

  auto self = shared_from_this();
  loop_->RunAfter(retry_delay_sec_, [self] {
    if (!self->stopped_.load(std::memory_order_acquire) &&
        self->retry_enabled_.load(std::memory_order_acquire) &&
        self->state_ == ConnectorState::kDisConnected && self->new_connection_cb_) {
      self->Connect();
    }
  });
}

int Connector::RemoveAndResetChannel() {
  channel_->DisableAll();
  channel_->Remove();
  int sockfd = channel_->fd();

  auto self = shared_from_this();
  loop_->QueueInLoop([self] { self->channel_.reset(); });

  return sockfd;
}

}  // namespace vexo::net
