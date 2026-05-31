// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <functional>
#include <string>
#include <unordered_map>

#include "runtime/base/noncopyable.h"
#include "runtime/net/inet_address.h"
#include "runtime/net/socket.h"
#include "runtime/uring/uring_connection.h"

namespace runtime::uring {

class UringLoop;

class UringServer : public runtime::base::NonCopyable {
public:
  using UringConnPtr = UringConnection::UringConnPtr;
  using MessageCallback = UringConnection::MessageCallback;
  using ConnectionCallback = std::function<void(const UringConnPtr&)>;

  UringServer(UringLoop* loop, const runtime::net::InetAddress& listen_addr, std::string name);
  ~UringServer();

  void set_message_callback(MessageCallback cb) { message_callback_ = std::move(cb); }
  void set_connection_callback(ConnectionCallback cb) { connection_callback_ = std::move(cb); }

  void Start();

private:
  using ConnectionMap = std::unordered_map<std::string, UringConnPtr>;

  void ArmAccept();
  void OnAccept(int res, unsigned flags);
  void RemoveConnection(const UringConnPtr& conn);

  UringLoop* loop_;
  const std::string name_;

  runtime::net::Socket accept_socket_;
  runtime::net::InetAddress listen_addr_;
  int next_id_{1};

  ConnectionMap conns_;

  MessageCallback message_callback_;
  ConnectionCallback connection_callback_;
};

}  // namespace runtime::uring
