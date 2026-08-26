// SPDX-License-Identifier: MIT
// Bounded cover of examples/luring/01_single_shot_echo.cc and
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

#include "coropact/result.h"
#include "coropact/coro/spawn.h"
#include "coropact/io/loop.h"
#include "coropact/luring/connector.h"
#include "coropact/luring/listener.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/options.h"
#include "coropact/luring/stream.h"
#include "coropact/net/endpoint.h"

namespace {

constexpr std::string_view kMessage = "hello from connector";

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

bool IsEnvironmentSkip(coropact::Error error) {
  return error == std::errc::operation_not_supported || error == std::errc::operation_not_permitted;
}

coropact::coro::DetachedTask EchoOnce(coropact::luring::Stream stream) {
  std::array<std::byte, 4096> buffer{};

  for (;;) {
    auto read = co_await stream.ReadSome(buffer);
    if (!read.has_value() || *read == 0) {
      break;
    }

    auto payload = std::span<const std::byte>(buffer.data(), *read);
    auto written = co_await stream.WriteAll(payload);
    if (!written.has_value()) {
      break;
    }
  }

  (void)co_await stream.Close();
}

coropact::coro::DetachedTask AcceptOnce(coropact::luring::Loop& loop,
                                        coropact::luring::Listener& listener) {
  auto accepted = co_await listener.Accept();
  if (!accepted.has_value()) {
    loop.RequestStop();
    co_return;
  }

  coropact::coro::SpawnDetach(loop, EchoOnce(std::move(*accepted)));
}

coropact::coro::DetachedTask ConnectOnce(coropact::luring::Loop& loop, std::uint16_t port,
                                         bool* echo_ok) {
  auto connector = coropact::luring::Connector::Create(&loop);
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

  auto written = co_await stream.WriteAll(payload);
  if (!written.has_value()) {
    (void)co_await stream.Close();
    loop.RequestStop();
    co_return;
  }

  std::array<std::byte, 1024> buffer{};
  std::string received;
  received.reserve(kMessage.size());

  while (received.size() < kMessage.size()) {
    auto read = co_await stream.ReadSome(buffer);
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
  coropact::luring::Loop loop;
  coropact::luring::Options options;
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

  auto listener = coropact::luring::Listener::Create(&loop, coropact::net::Endpoint::Loopback(0));
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
  coropact::coro::SpawnDetach(loop, AcceptOnce(loop, *listener));
  coropact::coro::SpawnDetach(loop, ConnectOnce(loop, local->ToPort(), &echo_ok));
  loop.Run();

  return Check(echo_ok, "echo round-trip did not match") &&
         Check(loop.State() == coropact::io::LoopState::kStopped, "loop did not stop");
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
