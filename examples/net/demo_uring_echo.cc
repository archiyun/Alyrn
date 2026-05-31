// demo_uring_echo.cc -- io_uring proactor 回显
//   PORT=9100 ./build-tests/examples/demo_uring_echo
//   echo "hello" | nc -q1 127.0.0.1 9100
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <string>

#include "runtime/log/logger.h"
#include "runtime/net/inet_address.h"
#include "runtime/net/net_utils.h"
#include "runtime/uring/uring_loop.h"
#include "runtime/uring/uring_server.h"

namespace {
runtime::uring::UringLoop* g_loop = nullptr;
void OnSignal(int) {
  if (g_loop) g_loop->Quit();
}
}  // namespace

int main() {
  const char* p = std::getenv("PORT");
  const std::uint16_t port = static_cast<std::uint16_t>(p ? std::atoi(p) : 9100);

  runtime::net::IgnoreSigPipe();
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);

  runtime::log::Logger::Instance().Init("uring_echo", runtime::log::LogLevel::INFO);

  runtime::uring::UringLoop loop;
  g_loop = &loop;

  runtime::uring::UringServer server(&loop, runtime::net::InetAddress(port), "Uring Echo");

  server.set_message_callback(
      [](const runtime::uring::UringConnection::UringConnPtr& conn, runtime::net::Buffer& buf,
         runtime::time::Timestamp) { conn->Send(buf.RetrieveAllAsString()); });

  server.Start();
  std::printf("UringEcho listening on port %u\n", port);
  std::fflush(stdout);
  loop.Loop();
  return 0;
}
