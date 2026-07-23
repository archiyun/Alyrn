// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <iostream>
#include <system_error>
#include <type_traits>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/luring/listener.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/options.h"
#include "coropact/luring/stream.h"
#include "coropact/net/inet_address.h"

namespace {

enum class LoopInitStatus {
  kReady,
  kSkip,
  kFail,
};

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

bool IsEnvironmentSkip(coropact::base::Error error) {
  return error == std::errc::operation_not_supported || error == std::errc::operation_not_permitted;
}

LoopInitStatus InitLoop(coropact::luring::LUringLoop& loop) {
  coropact::luring::LUringOptions options;
  options.entries = 16;
  options.submit_batch = 1;

  auto init = loop.Init(options);
  if (init.has_value()) {
    return LoopInitStatus::kReady;
  }
  if (IsEnvironmentSkip(init.error())) {
    std::cout << "SKIP: io_uring unavailable: " << init.error().message() << '\n';
    return LoopInitStatus::kSkip;
  }

  std::cout << "FAIL: LUringLoop init failed: " << init.error().message() << '\n';
  return LoopInitStatus::kFail;
}

bool TestStreamMove(coropact::luring::LUringLoop& loop) {
  int fds[2]{-1, -1};
  if (!Check(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) == 0,
             "LUringStream socketpair creation failed")) {
    return false;
  }

  {
    coropact::luring::LUringStream source(&loop, fds[0], coropact::net::InetAddress(0));
    coropact::luring::LUringStream moved(std::move(source));
    coropact::luring::LUringStream target(&loop, fds[1], coropact::net::InetAddress(0));
    target = std::move(moved);

    if (!Check(source.fd() == -1 && moved.fd() == -1 && target.fd() == fds[0],
               "LUringStream move did not transfer fd ownership")) {
      return false;
    }
  }

  return true;
}

bool TestListenerMove(coropact::luring::LUringLoop& loop) {
  auto source = coropact::luring::LUringListener::Create(&loop, coropact::net::InetAddress(0));
  if (!Check(source.has_value(), "LUringListener creation failed")) {
    return false;
  }
  auto source_address = source->LocalAddress();
  if (!Check(source_address.has_value(), "LUringListener local address lookup failed")) {
    return false;
  }

  coropact::luring::LUringListener moved(std::move(*source));
  auto moved_address = moved.LocalAddress();
  if (!Check(moved_address.has_value() && moved_address->ToPort() == source_address->ToPort(),
             "LUringListener move construction did not transfer the socket")) {
    return false;
  }

  auto target = coropact::luring::LUringListener::Create(&loop, coropact::net::InetAddress(0));
  if (!Check(target.has_value(), "LUringListener move target creation failed")) {
    return false;
  }
  *target = std::move(moved);

  auto target_address = target->LocalAddress();
  return Check(source->fd() == -1 && moved.fd() == -1 && target_address.has_value() &&
                   target_address->ToPort() == source_address->ToPort(),
               "LUringListener move assignment did not transfer the socket");
}

}  // namespace

int main() {
  static_assert(std::is_move_constructible_v<coropact::luring::LUringStream>);
  static_assert(std::is_move_assignable_v<coropact::luring::LUringStream>);
  static_assert(std::is_move_constructible_v<coropact::luring::LUringListener>);
  static_assert(std::is_move_assignable_v<coropact::luring::LUringListener>);

  coropact::luring::LUringLoop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return 0;
    case LoopInitStatus::kFail:
      return 1;
  }

  if (!TestStreamMove(loop) || !TestListenerMove(loop)) {
    return 1;
  }

  std::cout << "luring move smoke: PASS\n";
  return 0;
}
