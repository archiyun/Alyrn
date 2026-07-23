// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <expected>
#include <iostream>
#include <optional>

#include "coropact/base/error.h"
#include "coropact/coro/spawn.h"
#include "coropact/coro/task.h"
#include "coropact/io/async_listener.h"
#include "coropact/io/io_backend.h"
#include "coropact/net/event_loop.h"
#include "coropact/net/event_loop_scheduler.h"
#include "coropact/net/inet_address.h"
#include "coropact/net/reactor_connect.h"
#include "coropact/net/reactor_listener.h"
#include "coropact/net/reactor_stream.h"

namespace {

using AcceptResult = coropact::base::Result<typename coropact::net::ReactorListener::Stream>;

static_assert(coropact::io::AsyncListener<coropact::net::ReactorListener>);

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

int ConnectNonBlocking(const coropact::net::InetAddress& address) {
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

coropact::coro::Task<void> AcceptOnce(coropact::net::ReactorListener* listener, coropact::net::EventLoop* loop,
                                  std::optional<AcceptResult>* out) {
  out->emplace(co_await listener->Accept());
  loop->Quit();
}

bool CheckFactories() {
  auto null_listener = coropact::net::ReactorListener::Create(nullptr, coropact::net::InetAddress(0));
  if (!Check(!null_listener.has_value() && null_listener.error() == std::errc::invalid_argument,
             "listener factory accepted a null EventLoop")) {
    return false;
  }

  auto null_connector = coropact::net::ReactorConnector::Create(nullptr);
  if (!Check(!null_connector.has_value() && null_connector.error() == std::errc::invalid_argument,
             "connector factory accepted a null EventLoop")) {
    return false;
  }

  coropact::net::EventLoop loop;
  auto listener = coropact::net::ReactorListener::Create(&loop, coropact::net::InetAddress(0));
  if (!Check(listener.has_value(), "listener factory failed for a valid socket")) {
    if (!listener.has_value()) {
      std::cout << "factory error: " << listener.error().message() << '\n';
    }
    return false;
  }

  auto address = listener->LocalAddress();
  if (!Check(address.has_value(), "factory listener local address lookup failed")) {
    return false;
  }

  auto conflicting_listener = coropact::net::ReactorListener::Create(&loop, *address);
  return Check(!conflicting_listener.has_value() &&
                   conflicting_listener.error() == std::errc::address_in_use,
               "listener factory did not return bind errors");
}

bool CheckPendingAccept() {
  coropact::net::EventLoop loop;
  coropact::net::ReactorListener listener(&loop, coropact::net::InetAddress(0));
  coropact::net::EventLoopScheduler scheduler(&loop);

  auto listen_addr = listener.LocalAddress();
  if (!listen_addr.has_value()) {
    std::cout << "FAIL: listener local address failed\n";
    return false;
  }

  std::optional<AcceptResult> result;
  int client_fd = -1;

  coropact::coro::Spawn(scheduler, AcceptOnce(&listener, &loop, &result)).Detach();
  loop.QueueInLoop([&] { client_fd = ConnectNonBlocking(*listen_addr); });

  loop.Loop();

  if (client_fd >= 0) {
    ::close(client_fd);
  }

  return Check(result.has_value(), "pending accept did not finish") &&
         Check(result->has_value(), "pending accept returned error");
}

bool CheckCloseCancelsPendingAccept() {
  coropact::net::EventLoop loop;
  coropact::net::ReactorListener listener(&loop, coropact::net::InetAddress(0));
  coropact::net::EventLoopScheduler scheduler(&loop);

  std::optional<AcceptResult> result;

  coropact::coro::Spawn(scheduler, AcceptOnce(&listener, &loop, &result)).Detach();
  loop.QueueInLoop([&] { coropact::coro::Spawn(scheduler, listener.Close()).Detach(); });

  loop.Loop();

  return Check(result.has_value(), "cancelled accept did not finish") &&
         Check(!result->has_value(), "cancelled accept unexpectedly succeeded") &&
         Check(result->error() == std::errc::operation_canceled,
               "cancelled accept did not return ECANCELED");
}

bool CheckBackendBindingProfile() {
  auto binding = coropact::io::BindReactor();
  if (!Check(binding.has_value(), "reactor backend binding failed")) {
    return false;
  }

  if (!Check(binding->active_profile.ContainsAll(coropact::io::CapabilitySet::CoreGateway()),
             "reactor binding does not activate core gateway profile")) {
    return false;
  }

  if (!Check(binding->backend_capabilities.ContainsAll(binding->active_profile),
             "reactor backend capabilities do not cover active profile")) {
    return false;
  }

  if (!Check(binding->backend_capabilities.ContainsAll(coropact::io::CapabilitySet::TimedGateway()),
             "reactor backend should advertise timeout-capable gateway profile")) {
    return false;
  }

  coropact::io::CapabilitySet invalid_profile;
  invalid_profile.Enable(coropact::io::IoCapability::kReadinessPoll);
  auto invalid = coropact::io::BindReactor(invalid_profile);
  return Check(!invalid.has_value(),
               "reactor binding unexpectedly accepted implementation tags in active profile");
}

}  // namespace

int main() {
  if (!CheckFactories()) return 1;
  if (!CheckPendingAccept()) return 1;
  if (!CheckCloseCancelsPendingAccept()) return 1;
  if (!CheckBackendBindingProfile()) return 1;

  std::cout << "reactor listener smoke: PASS\n";
  return 0;
}
