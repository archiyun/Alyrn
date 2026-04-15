#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/net/channel.h"
#include "runtime/net/socket.h"

#include <functional>

namespace runtime::net {

class EventLoop;
class InetAddress;

// Acceptor owns the listening socket and accepts new inbound TCP connections.
//
// It is typically attached to the base EventLoop. When the listening fd
// becomes readable, Acceptor accepts one or more pending connections and
// forwards them through NewConnectionCallback.
class Acceptor : public runtime::base::NonCopyable {
public:
  using NewConnectionCallback =
      std::function<void(int sockfd, const InetAddress&)>;

  Acceptor(EventLoop* loop, const InetAddress& listen_addr, bool reuse_port);
  ~Acceptor();

  void SetNewConnectionCallback(const NewConnectionCallback& cb) {
    new_connection_callback_ = cb;
  }

  // Must be called before Listen() so accept_channel_ is registered with EPOLLET.
  void SetEdgeTriggered(bool et) { accept_channel_.SetEdgeTriggered(et); }

  bool Listening() const { return listening_; }
  void Listen();

private:
  void HandleRead(runtime::time::Timestamp receive_time);

  EventLoop* loop_;
  Socket accept_socket_;
  Channel accept_channel_;
  NewConnectionCallback new_connection_callback_;
  bool listening_;
};

}  // namespace runtime::net
