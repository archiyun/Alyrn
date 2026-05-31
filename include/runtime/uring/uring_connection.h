// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include "runtime/base/noncopyable.h"
#include "runtime/net/buffer.h"
#include "runtime/net/inet_address.h"
#include "runtime/time/timestamp.h"

namespace runtime::uring {

class UringLoop;

class UringConnection : public runtime::base::NonCopyable,
                        public std::enable_shared_from_this<UringConnection> {
public:
  using UringConnPtr = std::shared_ptr<UringConnection>;
  using MessageCallback =
      std::function<void(const UringConnPtr, runtime::net::Buffer&, runtime::time::Timestamp)>;
  using CloseCallback = std::function<void(const UringConnPtr&)>;
  using WriteCompleteCallback = std::function<void(const UringConnPtr&)>;

  UringConnection(UringLoop* loop, int sockfd, std::string name,
                  const runtime::net::InetAddress& peer);
  ~UringConnection();

  int fd() const { return fd_; }
  const std::string& name() const { return name_; }
  const runtime::net::InetAddress& peer_address() const { return peer_; }
  bool Connected() const { return state_ == TCPState::kConnected; }

  void set_message_callback(MessageCallback cb) { message_callback_ = std::move(cb); }
  void set_close_callback(CloseCallback cb) { close_callback_ = std::move(cb); }
  void set_write_complete_callback(WriteCompleteCallback cb) {
    write_complete_callback_ = std::move(cb);
  }
  void Start();
  void Send(std::string_view message);
  void Shutdown();

private:
  enum class TCPState : uint8_t {
    kDisconnected,
    kConnecting,
    kConnected,
    kDisconnecting,
  };

  void ArmRecv();
  void OnRecv(int res, unsigned flags);
  void ArmSend();
  void OnSend(int res, unsigned flags);
  void HandleClose();

  static constexpr std::size_t kRecvChunk = 16 * 1024;

  UringLoop* loop_;
  const int fd_;
  const std::string name_;
  const runtime::net::InetAddress peer_;
  TCPState state_{TCPState::kDisconnected};

  runtime::net::Buffer input_buffer_;
  runtime::net::Buffer output_buffer_;
  bool sending_{false};

  MessageCallback message_callback_;
  CloseCallback close_callback_;
  WriteCompleteCallback write_complete_callback_;
};

}  // namespace runtime::uring
