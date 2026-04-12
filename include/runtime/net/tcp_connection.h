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

/**
 * - between Reactor and User connection brige.
 * 管理 一条建立好的TCP连接， 只负责读，写，关闭，错误处理
 */
class TcpConnection : public runtime::base::NonCopyable,
                      public std::enable_shared_from_this<TcpConnection> {
public:
  using TcpConnectionPtr = std::shared_ptr<TcpConnection>;

  using ConnectionCallback = std::function<void(const TcpConnectionPtr&)>;
  using MessageCallback = std::function<void(
      const TcpConnectionPtr&,
      Buffer&,
      runtime::time::Timestamp)>;
  using CloseCallback = std::function<void(const TcpConnectionPtr&)>;
  using WriteCompleteCallback = std::function<void(
    const TcpConnectionPtr &)>;

  TcpConnection(
      EventLoop *loop, 
      const std::string &name, 
      int sockfd,
      const InetAddress &local_addr, 
      const InetAddress &peer_addr);

  ~TcpConnection();

  EventLoop *GetLoop() const { return loop_; }
  const std::string &Name() const { return name_; }
  const InetAddress &LocalAddress() const { return local_addr_; }
  const InetAddress &PeerAddress() const { return peer_addr_; }
  bool Connected() const { return state_ == StateE::kConnected; }

  void Send(const std::string &message);
  // 主动关闭写端
  void Shutdown();

  void SetConnectionCallback(const ConnectionCallback &cb) {
    connection_callback_ = cb;
  }

  void SetMessageCallback(const MessageCallback &cb) { message_callback_ = cb; }

  void SetCloseCallback(const CloseCallback &cb) { close_callback_ = cb; }

  void SetWriteCompleteCallback(const WriteCompleteCallback &cb) {
    write_complete_callback_ = cb;
  }

  // http 模块新增
  void SetContext(std::any ctx) { context_ = std::move(ctx); }
  std::any &GetContext() { return context_; }
  const std::any &GetContext() const { return context_; }

  void ConnectEstablished();
  void ConnectDestroyed();

private:
    // 状态机
    enum class StateE {
    kDisconnected,
    kConnecting,
    kConnected,
    kDisconnecting
  };

  void SetState(StateE state) { state_ = state; }

  void HandleRead(runtime::time::Timestamp receive_time);
  void HandleWrite();
  void HandleClose();
  void HandleError();

  void SendInLoop(const std::string &message);
  void ShutdownInLoop();

private:
  EventLoop *loop_;          // 归属 sub_loop
  const std::string name_;   // for debug
  StateE state_;             // 连接生命周期状态机

  // socket  负责生命周期和socket option
  // channel 负责事件注册和分发
  std::unique_ptr<Socket> socket_;    // 管理 fd, socket 语义
  std::unique_ptr<Channel> channel_;  // 把 fd 接入 Reactor事件系统

  const InetAddress local_addr_;  // 本端 ip : port
  const InetAddress peer_addr_;   // 对端 ip : port

  Buffer input_buffer_;   // 从 socket 读出的数据暂存
  Buffer output_buffer_;  // 写到 sockets 但还没写完的数据

  // 用户设置相关回调
  ConnectionCallback connection_callback_;
  MessageCallback message_callback_;
  WriteCompleteCallback write_complete_callback_;
  // 内部使用, 通知 TcpServer 关闭连接
  CloseCallback close_callback_; 

  std::any context_;
};

} // namespace runtime::net
