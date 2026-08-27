// SPDX-License-Identifier: MIT

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <iostream>
#include <system_error>
#include <type_traits>
#include <utility>

#include "alyrn/result.h"
#include "alyrn/luring/listener.h"
#include "alyrn/luring/loop.h"
#include "alyrn/luring/detail/loop_access.h"
#include "alyrn/luring/options.h"
#include "alyrn/luring/stream.h"
#include "alyrn/net/endpoint.h"

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

bool IsEnvironmentSkip(alyrn::Error error) {
  return error == std::errc::operation_not_supported || error == std::errc::operation_not_permitted;
}

LoopInitStatus InitLoop(alyrn::luring::Loop& loop) {
  alyrn::luring::Options options;
  options.entries = 16;

  auto init = loop.Init(options);
  if (init.has_value()) {
    return LoopInitStatus::kReady;
  }
  if (IsEnvironmentSkip(init.error())) {
    std::cout << "SKIP: io_uring unavailable: " << init.error().message() << '\n';
    return LoopInitStatus::kSkip;
  }

  std::cout << "FAIL: Loop init failed: " << init.error().message() << '\n';
  return LoopInitStatus::kFail;
}

bool TestStreamMove(alyrn::luring::Loop& loop) {
  int fds[2]{-1, -1};
  if (!Check(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) == 0,
             "Stream socketpair creation failed")) {
    return false;
  }

  {
    alyrn::luring::Stream source(&loop, fds[0], alyrn::net::Endpoint(0));
    alyrn::luring::Stream moved(std::move(source));
    alyrn::luring::Stream target(&loop, fds[1], alyrn::net::Endpoint(0));
    target = std::move(moved);

    if (!Check(source.Fd() == -1 && moved.Fd() == -1 && target.Fd() == fds[0],
               "Stream move did not transfer fd ownership")) {
      return false;
    }
  }

  return true;
}

bool TestListenerMove(alyrn::luring::Loop& loop) {
  auto source = alyrn::luring::Listener::Create(&loop, alyrn::net::Endpoint(0));
  if (!Check(source.has_value(), "Listener creation failed")) {
    return false;
  }
  auto source_address = source->LocalAddress();
  if (!Check(source_address.has_value(), "Listener local address lookup failed")) {
    return false;
  }

  alyrn::luring::Listener moved(std::move(*source));
  auto moved_address = moved.LocalAddress();
  if (!Check(moved_address.has_value() && moved_address->ToPort() == source_address->ToPort(),
             "Listener move construction did not transfer the socket")) {
    return false;
  }

  auto target = alyrn::luring::Listener::Create(&loop, alyrn::net::Endpoint(0));
  if (!Check(target.has_value(), "Listener move target creation failed")) {
    return false;
  }
  *target = std::move(moved);

  auto target_address = target->LocalAddress();
  return Check(source->Fd() == -1 && moved.Fd() == -1 && target_address.has_value() &&
                   target_address->ToPort() == source_address->ToPort(),
               "Listener move assignment did not transfer the socket");
}

}  // namespace

int main() {
  static_assert(std::is_move_constructible_v<alyrn::luring::Stream>);
  static_assert(std::is_move_assignable_v<alyrn::luring::Stream>);
  static_assert(std::is_move_constructible_v<alyrn::luring::Listener>);
  static_assert(std::is_move_assignable_v<alyrn::luring::Listener>);

  alyrn::luring::Loop loop;
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
