// SPDX-License-Identifier: MIT
// Bounded cover of examples/uring/01_single_shot_echo.cc and
// 02_single_shot_connect.cc: one accept, one echo round-trip, then stop.

#include <array>
#include <csignal>
#include <cstddef>
#include <iostream>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include "alyrn/coro/spawn.h"
#include "alyrn/io/loop.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/result.h"
#include "alyrn/uring/connector.h"
#include "alyrn/uring/listener.h"
#include "alyrn/uring/loop.h"
#include "alyrn/uring/options.h"
#include "alyrn/uring/stream.h"

namespace {

constexpr std::string_view kMessage = "hello from connector";

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

bool IsEnvironmentSkip(alyrn::Error error) {
  return error == std::errc::operation_not_supported || error == std::errc::operation_not_permitted;
}

alyrn::coro::DetachedTask EchoOnce(alyrn::uring::Stream stream) {
  std::array<std::byte, 4096> buffer{};

  for (;;) {
    auto read = co_await stream.Read(buffer);
    if (!read.has_value() || *read == 0) {
      break;
    }

    auto payload = std::span<const std::byte>(buffer.data(), *read);
    auto written = co_await stream.Write(payload);
    if (!written.has_value()) {
      break;
    }
  }

  (void)co_await stream.Close();
}

alyrn::coro::DetachedTask AcceptOnce(alyrn::uring::Loop& loop, alyrn::uring::Listener& listener) {
  auto accepted = co_await listener.Accept();
  if (!accepted.has_value()) {
    loop.RequestStop();
    co_return;
  }

  alyrn::coro::SpawnDetach(loop, EchoOnce(std::move(*accepted)));
}

alyrn::coro::DetachedTask ConnectOnce(alyrn::uring::Loop& loop, std::uint16_t port, bool* echo_ok) {
  auto connector = alyrn::uring::Connector::Create(&loop);
  if (!connector.has_value()) {
    loop.RequestStop();
    co_return;
  }

  auto connected = co_await connector->Connect("127.0.0.1", port);
  if (!connected.has_value()) {
    loop.RequestStop();
    co_return;
  }

  auto stream = std::move(*connected);
  const auto payload = std::as_bytes(std::span<const char>(kMessage.data(), kMessage.size()));

  auto written = co_await stream.Write(payload);
  if (!written.has_value()) {
    (void)co_await stream.Close();
    loop.RequestStop();
    co_return;
  }

  std::array<std::byte, 1024> buffer{};
  std::string received;
  received.reserve(kMessage.size());

  while (received.size() < kMessage.size()) {
    auto read = co_await stream.Read(buffer);
    if (!read.has_value() || *read == 0) {
      (void)co_await stream.Close();
      loop.RequestStop();
      co_return;
    }

    received.append(reinterpret_cast<const char*>(buffer.data()), *read);
    if (received.size() > kMessage.size()) {
      (void)co_await stream.Close();
      loop.RequestStop();
      co_return;
    }
  }

  *echo_ok = received == kMessage;
  (void)co_await stream.Close();
  loop.RequestStop();
}

bool CheckEchoRoundTrip() {
  alyrn::uring::Loop loop;
  alyrn::uring::Options options;
  options.entries = 64;

  auto init = loop.Init(options);
  if (!init.has_value()) {
    if (IsEnvironmentSkip(init.error())) {
      std::cout << "SKIP: io_uring unavailable: " << init.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: Loop init failed: " << init.error().message() << '\n';
    return false;
  }

  auto listener = alyrn::uring::Listener::Create(&loop, alyrn::net::Endpoint::Loopback(0));
  if (!listener.has_value()) {
    std::cout << "FAIL: Listener::Create failed: " << listener.error().message() << '\n';
    return false;
  }

  auto local = listener->LocalAddress();
  if (!local.has_value()) {
    std::cout << "FAIL: LocalAddress failed: " << local.error().message() << '\n';
    return false;
  }

  bool echo_ok = false;
  alyrn::coro::SpawnDetach(loop, AcceptOnce(loop, *listener));
  alyrn::coro::SpawnDetach(loop, ConnectOnce(loop, local->ToPort(), &echo_ok));
  loop.Run();

  return Check(echo_ok, "echo round-trip did not match") &&
         Check(loop.State() == alyrn::io::LoopState::kStopped, "loop did not stop");
}

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);

  if (!CheckEchoRoundTrip()) {
    return 1;
  }

  std::cout << "luring single-shot echo smoke: PASS\n";
  return 0;
}
