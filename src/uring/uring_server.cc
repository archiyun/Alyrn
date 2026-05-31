// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "runtime/uring/uring_server.h"

#include <liburing.h>
#include <sys/socket.h>

#include <cerrno>
#include <cstring>

#include "runtime/log/logger.h"
#include "runtime/net/inet_address.h"
#include "runtime/net/net_utils.h"
#include "runtime/uring/completion.h"
#include "runtime/uring/uring_loop.h"

namespace runtime::uring {

namespace {}
UringServer::UringServer(UringLoop* loop, const runtime::net::InetAddress& listen_addr,
                         std::string name)
    : loop_(loop),
      accept_socket_(runtime::net::CreateNonBlockingSocket()),
      listen_addr_(listen_addr),
      name_(std::move(name)) {
  accept_socket_.set_reuse_addr(true);
  accept_socket_.set_reuse_port(true);
  accept_socket_.BindAddress(listen_addr);
}

UringServer::~UringServer() = default;

void UringServer::Start() {
  accept_socket_.Listen();
  LOG_INFO() << name_ << " listening on " << listen_addr_.ToIpPort();
  ArmAccept();
}

void UringServer::ArmAccept() {
  io_uring_sqe* sqe = loop_->get_ring().get_sqe();
  io_uring_prep_accept(sqe, accept_socket_.fd(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);

  auto* comp = new Completion{};
  comp->OnComplete = [this](int res, unsigned flags) { OnAccept(res, flags); };
  io_uring_sqe_set_data(sqe, comp);
}

void UringServer::OnAccept(int res, unsigned flags) {
  if (res >= 0) {
    const int connfd = res;
    runtime::net::InetAddress peer(runtime::net::GetPeerAddr(connfd));
    std::string conn_name = name_ + "#" + std::to_string(next_id_++);

    auto conn = std::make_shared<UringConnection>(loop_, connfd, conn_name, peer);
    conn->set_message_callback(message_callback_);
    conn->set_close_callback([this](const UringConnPtr& c) { RemoveConnection(c); });
    conns_.emplace(conn_name, conn);

    LOG_INFO() << name_ << " new connection " << conn_name
               << " peer=" << peer.ToIpPort();
    if (connection_callback_) connection_callback_(conn);
    conn->Start();
  } else if (res != -ECANCELED) {
    LOG_ERROR() << name_ << " accept error " << std::strerror(-res);
  }

  if (!(flags & IORING_CQE_F_MORE)) ArmAccept();
}

void UringServer::RemoveConnection(const UringConnPtr& conn) { conns_.erase(conn->name()); }

}  // namespace runtime::uring
