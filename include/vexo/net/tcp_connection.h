// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "vexo/net/buffer.h"
#include "vexo/net/inet_address.h"
#include "vexo/time/timestamp.h"
#include "vexo/utils/macros.h"

#include <any>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace vexo::net {

class Channel;
class EventLoop;
class Socket;

// TcpConnection represents one established TCP connection.
//
// It owns the connected socket and its Channel, buffers inbound and outbound
// data, and drives the read/write/close/error callbacks for the connection.
class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
public:
  using TcpConnectionPtr = std::shared_ptr<TcpConnection>;

  using ConnectionCallback = std::function<void(const TcpConnectionPtr&)>;
  using MessageCallback =
      std::function<void(const TcpConnectionPtr&, Buffer&, vexo::time::Timestamp)>;
  using CloseCallback = std::function<void(const TcpConnectionPtr&)>;
  using WriteCompleteCallback = std::function<void(const TcpConnectionPtr&)>;
  // Fired (once per crossing) when output_buffer_ grows from below the
  // configured high-water-mark threshold to at or above it. The second
  // argument is the current readable byte count of output_buffer_.
  using HighWaterMarkCallback =
      std::function<void(const TcpConnectionPtr&, std::size_t)>;

  TcpConnection(
      EventLoop* loop,
      const std::string& name,
      int sockfd,
      const InetAddress& local_addr,
      const InetAddress& peer_addr);

  ~TcpConnection();

  VEXO_DELETE_COPY_MOVE(TcpConnection);

  EventLoop* loop() const { return loop_; }
  const std::string& name() const { return name_; }

  // Diagnostic counters for upstream (proxy-side) TcpConnection lifetimes.
  // Bumped only when name starts with "proxy->". A request count that far
  // exceeds the ctor count is the only evidence that the upstream conn
  // pool is actually reusing sockets (vs. silently rebuilding every time).
  static uint64_t UpstreamCtorCount();
  static uint64_t UpstreamDtorCount();
  const InetAddress& local_address() const { return local_addr_; }
  const InetAddress& peer_address() const { return peer_addr_; }
  bool Connected() const { return state_ == TCPState::kConnected; }

  // Sends a message on the connection. The actual write may happen
  // immediately or later in the owning loop thread.
  // Returns true if accepted (sent or queued for the loop thread); false if
  // dropped because the connection is no longer in the Connected state.
  // Callers that need flow control should also wire set_high_water_mark_callback
  // and watch for the buffer-growth signal.
  bool Send(const std::string& message);
  bool Send(std::string_view message);
  bool Send(const void* data, std::size_t len);

  // Initiates a graceful shutdown of the write side.
  void Shutdown();

  void set_connection_callback(ConnectionCallback cb) {
    connection_callback_ = std::move(cb);
  }

  void set_message_callback(MessageCallback cb) {
    message_callback_ = std::move(cb);
  }

  void set_close_callback(CloseCallback cb) {
    close_callback_ = std::move(cb);
  }

  void set_write_complete_callback(WriteCompleteCallback cb) {
    write_complete_callback_ = std::move(cb);
  }

  // Configures the backpressure threshold and the callback fired when the
  // output buffer crosses it. Both should be set before the first Send().
  void set_high_water_mark_callback(HighWaterMarkCallback cb) {
    high_water_mark_callback_ = std::move(cb);
  }

  // Threshold in bytes. The callback is only meaningful when this value is
  // less than the maximum possible buffer growth.
  void set_high_water_mark(std::size_t bytes) { high_water_mark_ = bytes; }
  std::size_t high_water_mark() const { return high_water_mark_; }
  std::size_t output_buffer_readable_bytes() const {
    return output_buffer_.readable_bytes();
  }

  // Associates arbitrary upper-layer context with the connection.
  void set_context(std::any ctx) { context_ = std::move(ctx); }
  std::any& context() { return context_; }
  const std::any& context() const { return context_; }

  void set_tcp_no_delay(bool on);

  // Must be called before ConnectEstablished() so the channel is registered
  // with EPOLLET from the first epoll_ctl ADD call.
  void set_edge_triggered(bool et);

  void ConnectEstablished();
  void ConnectDestroyed();

private:
  enum class TCPState {
    kDisconnected,
    kConnecting,
    kConnected,
    kDisconnecting,
  };

  void set_state(TCPState state) { state_ = state; }

  void HandleRead(vexo::time::Timestamp receive_time);
  void HandleWrite();
  void HandleClose();
  void HandleError();

  void SendInLoop(const std::string& message);
  void SendInLoop(const void* data, std::size_t len);
  void ShutdownInLoop();

private:
  EventLoop* loop_;
  const std::string name_;
  TCPState state_;

  std::unique_ptr<Socket> socket_;
  std::unique_ptr<Channel> channel_;

  const InetAddress local_addr_;
  const InetAddress peer_addr_;

  Buffer input_buffer_;
  Buffer output_buffer_;

  ConnectionCallback connection_callback_;
  MessageCallback message_callback_;
  WriteCompleteCallback write_complete_callback_;
  HighWaterMarkCallback high_water_mark_callback_;
  CloseCallback close_callback_;

  // Default: effectively disabled (SIZE_MAX). The callback is only invoked
  // when the user explicitly lowers the threshold.
  std::size_t high_water_mark_{static_cast<std::size_t>(-1)};

  std::any context_;

};

}  // namespace vexo::net
