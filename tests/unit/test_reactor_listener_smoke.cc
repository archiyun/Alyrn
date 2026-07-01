// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <expected>
#include <iostream>
#include <memory>
#include <optional>

#include "vexo/base/error.h"
#include "vexo/coro/spawn.h"
#include "vexo/coro/task.h"
#include "vexo/net/async_listener.h"
#include "vexo/net/event_loop.h"
#include "vexo/net/event_loop_scheduler.h"
#include "vexo/net/inet_address.h"
#include "vexo/net/reactor_listener.h"
#include "vexo/net/reactor_stream.h"

namespace {

using AcceptResult =
    vexo::base::Result<std::unique_ptr<typename vexo::net::ReactorListener::Stream>>;

static_assert(vexo::net::AsyncListener<vexo::net::ReactorListener>);

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

int ConnectNonBlocking(const vexo::net::InetAddress& address) {
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    return -1;
  }

  const sockaddr_in& addr = address.sock_addr();
  int rc = ::connect(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr));
  if (rc == 0 || errno == EINPROGRESS) {
    return fd;
  }

  ::close(fd);
  return -1;
}

vexo::coro::Task<void> AcceptOnce(vexo::net::ReactorListener* listener, vexo::net::EventLoop* loop,
                                  std::optional<AcceptResult>* out) {
  out->emplace(co_await listener->Accept());
  loop->Quit();
}

bool CheckPendingAccept() {
  vexo::net::EventLoop loop;
  vexo::net::ReactorListener listener(&loop, vexo::net::InetAddress(0));
  vexo::net::EventLoopScheduler scheduler(&loop);

  auto listen_addr = listener.LocalAddress();
  if (!listen_addr.has_value()) {
    std::cout << "FAIL: listener local address failed\n";
    return false;
  }

  std::optional<AcceptResult> result;
  int client_fd = -1;

  vexo::coro::Spawn(scheduler, AcceptOnce(&listener, &loop, &result)).Detach();
  loop.QueueInLoop([&] { client_fd = ConnectNonBlocking(*listen_addr); });

  loop.Loop();

  if (client_fd >= 0) {
    ::close(client_fd);
  }

  return Check(result.has_value(), "pending accept did not finish") &&
         Check(result->has_value(), "pending accept returned error") &&
         Check((*result)->get() != nullptr, "pending accept returned null stream");
}

bool CheckCloseCancelsPendingAccept() {
  vexo::net::EventLoop loop;
  vexo::net::ReactorListener listener(&loop, vexo::net::InetAddress(0));
  vexo::net::EventLoopScheduler scheduler(&loop);

  std::optional<AcceptResult> result;

  vexo::coro::Spawn(scheduler, AcceptOnce(&listener, &loop, &result)).Detach();
  loop.QueueInLoop([&] { vexo::coro::Spawn(scheduler, listener.Close()).Detach(); });

  loop.Loop();

  return Check(result.has_value(), "cancelled accept did not finish") &&
         Check(!result->has_value(), "cancelled accept unexpectedly succeeded") &&
         Check(result->error() == std::errc::operation_canceled,
               "cancelled accept did not return ECANCELED");
}

}  // namespace

int main() {
  if (!CheckPendingAccept()) return 1;
  if (!CheckCloseCancelsPendingAccept()) return 1;

  std::cout << "reactor listener smoke: PASS\n";
  return 0;
}
