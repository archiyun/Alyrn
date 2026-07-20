// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Raw TCP echo server built directly on vexo_net.
//
// This deliberately has no HTTP parser, router, gateway, upstream, or timer
// path. It is the reactor-side baseline for the io_uring raw echo example.

#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>

#include "vexo/net/buffer.h"
#include "vexo/net/event_loop.h"
#include "vexo/net/inet_address.h"
#include "vexo/net/tcp_connection.h"
#include "vexo/net/tcp_server.h"

namespace {

int EnvInt(const char* key, int fallback) {
  const char* value = std::getenv(key);
  return value != nullptr ? std::atoi(value) : fallback;
}

void Echo(const std::shared_ptr<vexo::net::TcpConnection>& connection, vexo::net::Buffer& input,
          vexo::time::Timestamp) {
  const std::size_t bytes = input.readable_bytes();
  if (bytes == 0) {
    return;
  }
  connection->Send(input.Peek(), bytes);
  input.RetrieveAll();
}

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);

  const auto port = static_cast<std::uint16_t>(EnvInt("PORT", 19090));
  const int io_threads = EnvInt("IO_THREADS", 4);
  const bool edge_triggered = EnvInt("ET_MODE", 1) != 0;

  vexo::net::EventLoop loop;
  vexo::net::TcpServer server(&loop, vexo::net::InetAddress(port), "RawEchoReactor");
  server.set_thread_num(io_threads);
  server.set_edge_triggered(edge_triggered);
  server.set_message_callback(Echo);
  server.Start();

  std::printf("RawEchoReactor port=%u io_threads=%d et=%s\n", port, io_threads,
              edge_triggered ? "on" : "off");
  std::fflush(stdout);
  loop.Loop();
  return 0;
}
