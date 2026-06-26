// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/net/acceptor.h"

#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <cstdlib>
#include <cstring>

#include "vexo/log/logger.h"
#include "vexo/net/event_loop.h"
#include "vexo/net/inet_address.h"
#include "vexo/net/net_utils.h"

namespace vexo::net {
namespace {

int CreateAcceptSocket() {
  auto fd = CreateNonBlockingSocket();
  if (!fd) {
    LOG_FATAL() << "failed to create accept socket: error=" << fd.error().value()
                << " message=" << fd.error().message();
    std::abort();
  }
  return *fd;
}

}  // namespace

Acceptor::Acceptor(EventLoop* loop, const InetAddress& listen_addr, bool reuse_port)
    : loop_(loop),
      accept_socket_(CreateAcceptSocket()),
      accept_channel_(loop, accept_socket_.fd()),
      listening_(false) {
  accept_socket_.set_reuse_addr(true);
  accept_socket_.set_reuse_port(reuse_port);
  accept_socket_.BindAddress(listen_addr);

  accept_channel_.set_read_callback(
      [this](vexo::time::Timestamp receive_time) { HandleRead(receive_time); });
}

Acceptor::~Acceptor() {
  assert(loop_->IsInLoopThread());
  accept_channel_.DisableAll();
  accept_channel_.Remove();
}

void Acceptor::Listen() {
  assert(loop_->IsInLoopThread());
  listening_ = true;
  accept_socket_.Listen();
  accept_channel_.EnableReading();
  LOG_INFO() << "acceptor listening on fd=" << accept_socket_.fd();
}

void Acceptor::HandleRead(vexo::time::Timestamp) {
  auto do_accept = [this]() -> bool {
    InetAddress peer_addr(0);
    int connfd = accept_socket_.Accept(&peer_addr);
    if (connfd >= 0) {
      if (new_connection_callback_) {
        LOG_INFO() << "accepted connection fd=" << connfd << " peer=" << peer_addr.ToIpPort();
        new_connection_callback_(connfd, peer_addr);
      } else {
        LOG_WARN() << "accepted connection without callback, closing fd=" << connfd;
        ::close(connfd);
      }
      return true;
    }
    if (errno != EAGAIN && errno != EWOULDBLOCK) {
      LOG_ERROR() << "accept failed: errno=" << errno << " message=" << std::strerror(errno);
    }
    return false;
  };

  // Edge-triggered mode must drain all pending completed handshakes before
  // returning. Level-triggered mode can accept once and rely on epoll to
  // notify again if more connections remain.
  if (accept_channel_.IsEdgeTriggered()) {
    while (do_accept()) {
    }
  } else {
    do_accept();
  }
}

}  // namespace vexo::net
