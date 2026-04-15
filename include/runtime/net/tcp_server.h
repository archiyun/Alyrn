#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/net/acceptor.h"
#include "runtime/net/event_loop_thread_pool.h"
#include "runtime/net/inet_address.h"
#include "runtime/net/tcp_connection.h"

#include <map>
#include <memory>
#include <string>

namespace runtime::net {

class EventLoop;

// TcpServer manages a TCP listening socket and a set of active connections.
//
// It accepts new connections on the base loop, assigns them to I/O loops from
// EventLoopThreadPool, and manages the lifecycle of TcpConnection objects.
class TcpServer : public runtime::base::NonCopyable {
public:
  using TcpConnectionPtr = std::shared_ptr<TcpConnection>;
  using ConnectionCallback = TcpConnection::ConnectionCallback;
  using MessageCallback = TcpConnection::MessageCallback;
  using WriteCompleteCallback = TcpConnection::WriteCompleteCallback;
  using ThreadInitCallback = EventLoopThreadPool::ThreadInitCallback;

  TcpServer(EventLoop* loop,
            const InetAddress& listenaddr,
            const std::string& name);
  ~TcpServer();

  // A value of 0 keeps all I/O on the base loop. A positive value creates
  // that many worker loops.
  void SetThreadNum(int num_threads) { thread_num_ = num_threads; }

  // Switch the acceptor and all new connections to edge-triggered epoll mode.
  // Must be called before Start().
  void SetEdgeTriggered(bool et) { et_mode_ = et; }

  void SetThreadInitCallback(ThreadInitCallback&& cb) {
    thread_init_callback_ = std::forward<ThreadInitCallback>(cb);
  }
  void SetConnectionCallback(ConnectionCallback&& cb) {
    connection_callback_ = std::forward<ConnectionCallback>(cb);
  }

  void SetMessageCallback(MessageCallback&& cb) {
    message_callback_ = std::forward<MessageCallback>(cb);
  }

  void SetWriteCompleteCallback(WriteCompleteCallback&& cb) {
    write_complete_callback_ = std::forward<WriteCompleteCallback>(cb);
  }

  // Starts the thread pool and begins accepting connections.
  void Start();

private:
  void NewConnection(int sockfd, const InetAddress& peeraddr);
  void RemoveConnection(const TcpConnectionPtr& conn);
  void RemoveConnectionInLoop(const TcpConnectionPtr& conn);

private:
  using ConnectionMap = std::map<std::string, TcpConnectionPtr>;

  EventLoop* loop_;
  const std::string name_;

  std::unique_ptr<Acceptor> acceptor_;
  std::unique_ptr<EventLoopThreadPool> thread_pool_;
  int thread_num_;
  bool started_;
  bool et_mode_;
  int next_conn_id_;

  ConnectionCallback connection_callback_;
  MessageCallback message_callback_;
  WriteCompleteCallback write_complete_callback_;
  ThreadInitCallback thread_init_callback_;

  ConnectionMap connections_;
};

}  // namespace runtime::net
