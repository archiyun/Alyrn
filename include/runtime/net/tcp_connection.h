#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/net/buffer.h"
#include "runtime/net/channel.h"
#include "runtime/net/inet_address.h"
#include "runtime/net/socket.h"
#include "runtime/time/timestamp.h"

#include <any>
#include <functional>
#include <memory>
#include <string>

namespace runtime::net {

class EventLoop;

// TcpConnection represents one established TCP connection.
//
// It owns the connected socket and its Channel, buffers inbound and outbound
// data, and drives the read/write/close/error callbacks for the connection.
class TcpConnection : public runtime::base::NonCopyable,
                      public std::enable_shared_from_this<TcpConnection> {
public:
  using TcpConnectionPtr = std::shared_ptr<TcpConnection>;

  using ConnectionCallback = std::function<void(const TcpConnectionPtr&)>;
  using MessageCallback =
      std::function<void(const TcpConnectionPtr&, Buffer&, runtime::time::Timestamp)>;
  using CloseCallback = std::function<void(const TcpConnectionPtr&)>;
  using WriteCompleteCallback = std::function<void(const TcpConnectionPtr&)>;

  TcpConnection(
      EventLoop* loop,
      const std::string& name,
      int sockfd,
      const InetAddress& local_addr,
      const InetAddress& peer_addr);

  ~TcpConnection();

  EventLoop* GetLoop() const { return loop_; }
  const std::string& Name() const { return name_; }
  const InetAddress& LocalAddress() const { return local_addr_; }
  const InetAddress& PeerAddress() const { return peer_addr_; }
  bool Connected() const { return state_ == StateE::kConnected; }

  // Sends a message on the connection. The actual write may happen
  // immediately or later in the owning loop thread.
  void Send(const std::string& message);

  // Initiates a graceful shutdown of the write side.
  void Shutdown();

  void SetConnectionCallback(const ConnectionCallback& cb) {
    connection_callback_ = cb;
  }

  void SetMessageCallback(const MessageCallback& cb) { message_callback_ = cb; }

  void SetCloseCallback(const CloseCallback& cb) { close_callback_ = cb; }

  void SetWriteCompleteCallback(const WriteCompleteCallback& cb) {
    write_complete_callback_ = cb;
  }

  // Associates arbitrary upper-layer context with the connection.
  void SetContext(std::any ctx) { context_ = std::move(ctx); }
  std::any& GetContext() { return context_; }
  const std::any& GetContext() const { return context_; }

  // Must be called before ConnectEstablished() so the channel is registered
  // with EPOLLET from the first epoll_ctl ADD call.
  void SetEdgeTriggered(bool et) { channel_->SetEdgeTriggered(et); }

  void ConnectEstablished();
  void ConnectDestroyed();

private:
  enum class StateE {
    kDisconnected,
    kConnecting,
    kConnected,
    kDisconnecting,
  };

  void SetState(StateE state) { state_ = state; }

  void HandleRead(runtime::time::Timestamp receive_time);
  void HandleWrite();
  void HandleClose();
  void HandleError();

  void SendInLoop(const std::string &message);
  void ShutdownInLoop();

private:
  EventLoop* loop_;
  const std::string name_;
  StateE state_;

  std::unique_ptr<Socket> socket_;
  std::unique_ptr<Channel> channel_;

  const InetAddress local_addr_;
  const InetAddress peer_addr_;

  Buffer input_buffer_;
  Buffer output_buffer_;

  ConnectionCallback connection_callback_;
  MessageCallback message_callback_;
  WriteCompleteCallback write_complete_callback_;
  CloseCallback close_callback_;

  std::any context_;
};

}  // namespace runtime::net
