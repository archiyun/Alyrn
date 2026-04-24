#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"

namespace runtime::net {

class Connector : public runtime::base::NonCopyable,
                  public std::enable_shared_from_this<Connector> {
public:
  using NewConnectionCallback = std::function<void(int sockfd)>;

  Connector(EventLoop *loop, const InetAddress &server_addr);
  ~Connector();

  void SetConnectionCallback(NewConnectionCallback&& cb) { new_connection_cb_ = std::move(cb); }
  void Start();
  void Stop();

private:
  enum class State : int { kDisConnected = 0, kConnecting, kConnected };

  void StartInLoop();
  void Connect();
  void Connecting(int sockfd);
  void handlerWrite();
  void handleError();
  void Retry(int sockfd);
  int RemoveAndResetChannel();

private:
  EventLoop *loop_;
  InetAddress server_addr_;
  State state_{State::kDisConnected};
  std::unique_ptr<Channel> channel_;
  double retry_delay_sec_{0.5};
  NewConnectionCallback new_connection_cb_;
};

} // namespace runtime::net