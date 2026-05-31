// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "runtime/uring/uring_connection.h"

#include <liburing.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "runtime/log/logger.h"
#include "runtime/uring/completion.h"
#include "runtime/uring/uring_loop.h"

namespace runtime::uring {

UringConnection::UringConnection(UringLoop* loop, int fd, std::string name,
                                 const runtime::net::InetAddress& peer)
    : loop_(loop), fd_(fd), name_(std::move(name)), peer_(peer) {}

UringConnection::~UringConnection() {
  if (fd_ >= 0) ::close(fd_);
}

void UringConnection::Start() {
  // Start() is the "connection established" transition (mirrors
  // TcpConnection::ConnectEstablished): the socket is already accepted, so move
  // to kConnected and arm the first recv.
  state_ = TCPState::kConnected;
  ArmRecv();
}

void UringConnection::ArmRecv() {
  input_buffer_.EnsureWritableBytes(kRecvChunk);
  io_uring_sqe* sqe = loop_->get_ring().get_sqe();
  io_uring_prep_recv(sqe, fd_, input_buffer_.BeginWrite(), input_buffer_.writable_bytes(), 0);

  auto* comp = new Completion{};
  comp->OnComplete = [self = shared_from_this()](int res, unsigned flags) {
    self->OnRecv(res, flags);
  };
  io_uring_sqe_set_data(sqe, comp);
}

void UringConnection::OnRecv(int res, unsigned /*flags*/) {
  if (res > 0) {
    input_buffer_.HasWritten(static_cast<std::size_t>(res));
    if (message_callback_)
      message_callback_(shared_from_this(), input_buffer_, runtime::time::Timestamp::Now());
    if (state_ == TCPState::kConnected) ArmRecv();
    return;
  }
  if (res == 0) {
    HandleClose();
    return;
  }
  if (res != -ECANCELED) {
    LOG_ERROR() << name_ << " recv error: " << std::strerror(-res);
  }
  HandleClose();
}

void UringConnection::Send(std::string_view message) {
  if (state_ != TCPState::kConnected) return;
  output_buffer_.Append(message.data(), message.size());
  if (!sending_) ArmSend();
}

void UringConnection::ArmSend() {
  io_uring_sqe* sqe = loop_->get_ring().get_sqe();
  io_uring_prep_send(sqe, fd_, output_buffer_.Peek(), output_buffer_.readable_bytes(), 0);

  auto* comp = new Completion{};
  comp->OnComplete = [self = shared_from_this()](int res, unsigned flags) {
    self->OnSend(res, flags);
  };
  io_uring_sqe_set_data(sqe, comp);
  sending_ = true;
}

void UringConnection::OnSend(int res, unsigned /*flags*/) {
  if (res < 0) {
    sending_ = false;
    if (res != -ECANCELED) {
      LOG_ERROR() << name_ << " send error: " << std::strerror(-res);
    }
    HandleClose();
    return;
  }
  output_buffer_.Retrieve(static_cast<std::size_t>(res));
  if (output_buffer_.readable_bytes() > 0) {
    ArmSend();
    return;
  }
  sending_ = false;
  if (write_complete_callback_) write_complete_callback_(shared_from_this());
  if (state_ == TCPState::kDisconnecting) ::shutdown(fd_, SHUT_WR);
}

void UringConnection::Shutdown() {
  if (state_ != TCPState::kConnected) return;
  state_ = TCPState::kDisconnecting;
  if (!sending_) ::shutdown(fd_, SHUT_WR);
}

void UringConnection::HandleClose() {
  if (state_ == TCPState::kDisconnected) return;
  state_ = TCPState::kDisconnected;
  LOG_INFO() << name_ << " closed";
  if (close_callback_) close_callback_(shared_from_this());
}

}  // namespace runtime::uring
