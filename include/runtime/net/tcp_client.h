
#include "runtime/base/noncopyable.h"
#include "runtime/net/inet_address.h"
#include "runtime/net/tcp_connection.h"
#include <atomic>
#include <string>

namespace runtime::net {

class TcpClient : public runtime::base::NonCopyable {
public:
  using TcpConnectionPtr = TcpConnection::TcpConnectionPtr;

  using ConnectionCallback = TcpConnection::ConnectionCallback;
  using MessageCallback = TcpConnection::MessageCallback;
  using WriteCompleteCallback = TcpConnection::WriteCompleteCallback;

  TcpClient(EventLoop* loop, const InetAddress& server_addr, std::string name);
  ~TcpClient();

  // 发起非阻塞连接， 建立后触发连接回调。
  void Connect();
  void Disconnect();

  void SetConnectionCallback(ConnectionCallback&& cb) {
    connection_callback_ = std::move(cb);
  }
  void SetMessageCallback(MessageCallback&& cb) {
    message_callback_ = std::move(cb);
  }
  void SetWriteCompleteCallback(WriteCompleteCallback&& cb) {
    write_complete_callback_ = std::move(cb);
  }

private:
  void NewConnection(int sockfd);
  void RemoveConnection(const TcpConnection&conn);

  EventLoop* loop_;
  const InetAddress server_addr_;
  std::string name_;
  std::atomic<bool> connect_{false};
  std::unique_ptr<Connector> connector_; // 状态机
  TcpConnectionPtr connection_;

  ConnectionCallback connection_callback_;
  MessageCallback message_callback_;
  WriteCompleteCallback write_complete_callback_;
};
} // namespace runtime::net