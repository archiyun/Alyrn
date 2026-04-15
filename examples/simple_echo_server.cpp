#include "runtime/log/logger.h"
#include "runtime/net/buffer.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"
#include "runtime/net/tcp_server.h"
#include "runtime/time/timestamp.h"

#include <memory>
#include <string>

int main() {
  auto& logger = runtime::log::Logger::Instance();
  logger.Init("simple_echo_server.log", runtime::log::LogLevel::DEBUG);

  runtime::net::EventLoop main_loop;
  runtime::net::InetAddress listen_addr(8080, "127.0.0.1");

  runtime::net::TcpServer server(&main_loop, listen_addr, "EchoServer");

  // Start four I/O threads to demonstrate the one-loop-per-thread model.
  server.SetThreadNum(4);

  server.SetConnectionCallback(
      [](const std::shared_ptr<runtime::net::TcpConnection>& conn) {
        LOG_INFO() << "[connection] " << conn->Name()
                   << " local=" << conn->LocalAddress().ToIpPort()
                   << " peer=" << conn->PeerAddress().ToIpPort();
      });

  server.SetMessageCallback(
      [](const std::shared_ptr<runtime::net::TcpConnection>& conn,
         runtime::net::Buffer& buffer,
         runtime::time::Timestamp receive_time) {
        std::string message = buffer.RetrieveAllAsString();
        LOG_INFO() << "[message] " << conn->Name()
                   << " at " << receive_time.ToFormattedString()
                   << " => " << message;

        conn->Send(message);
      });

  server.SetWriteCompleteCallback(
      [](const std::shared_ptr<runtime::net::TcpConnection>& conn) {
        LOG_INFO() << "[write complete] " << conn->Name();
      });

  server.Start();

  LOG_INFO() << "echo server listen on 127.0.0.1:8080";
  main_loop.Loop();
  return 0;
}
