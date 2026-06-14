// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>

#include "runtime/base/noncopyable.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"

namespace runtime::net {

// Connector manages asynchronous outbound TCP connection establishment.
//
// It is typically used by TcpClient or upstream connection pools.
// Connector initiates a non-blocking connect(2) operation and monitors
// the socket through EventLoop. 
//
// Connector also maintains connection state and supports retry with
// exponential backoff after connection failures.
class Connector : public runtime::base::NonCopyable,
                  public std::enable_shared_from_this<Connector> {
public:
  using NewConnectionCallback = std::function<void(int sockfd)>;

  Connector(EventLoop* loop, const InetAddress& server_addr);
  ~Connector();

  void set_connection_callback(NewConnectionCallback cb) {
    new_connection_cb_ = std::move(cb);
  }

  void Start();
  void Stop();

private:
  enum class ConnectorState : uint8_t { 
    kDisConnected = 0, 
    kConnecting, 
    kConnected 
  };

  void StartInLoop();
  void Connect();
  void Connecting(int sockfd);
  void handleWrite();
  void handleError();
  void Retry(int sockfd);
  int RemoveAndResetChannel();

private:
  EventLoop*               loop_;
  InetAddress              server_addr_;
  ConnectorState           state_{ConnectorState::kDisConnected};
  std::unique_ptr<Channel> channel_;
  double                   retry_delay_sec_{0.5};
  NewConnectionCallback    new_connection_cb_;
  std::atomic<bool>        stopped_{false};
};

} // namespace runtime::net
