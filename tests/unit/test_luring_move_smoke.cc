// SPDX-License-Identifier: MIT

#include <sys/socket.h>

#include <iostream>
#include <system_error>
#include <type_traits>
#include <utility>

#include "alyrn/result.h"
#include "alyrn/uring/listener.h"
#include "alyrn/uring/loop.h"
#include "alyrn/uring/options.h"
#include "alyrn/uring/stream.h"
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

LoopInitStatus InitLoop(alyrn::uring::Loop& loop) {
  alyrn::uring::Options options;
  options.entries = 16;

  auto init = loop.Init(options);
  if (init.HasValue()) {
    return LoopInitStatus::kReady;
  }
  if (IsEnvironmentSkip(init.Error())) {
    std::cout << "SKIP: io_uring unavailable: " << init.Error().message() << '\n';
    return LoopInitStatus::kSkip;
  }

  std::cout << "FAIL: Loop init failed: " << init.Error().message() << '\n';
  return LoopInitStatus::kFail;
}

bool TestStreamMove(alyrn::uring::Loop& loop) {
  int fds[2]{-1, -1};
  if (!Check(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) == 0,
             "Stream socketpair creation failed")) {
    return false;
  }

  {
    alyrn::uring::Stream source(&loop, fds[0], alyrn::net::Endpoint(0));
    alyrn::uring::Stream moved(std::move(source));
    alyrn::uring::Stream target(&loop, fds[1], alyrn::net::Endpoint(0));
    target = std::move(moved);

    if (!Check(source.Fd() == -1 && moved.Fd() == -1 && target.Fd() == fds[0],
               "Stream move did not transfer fd ownership")) {
      return false;
    }
  }

  return true;
}

bool TestListenerMove(alyrn::uring::Loop& loop) {
  auto source = alyrn::uring::Listener::Create(&loop, alyrn::net::Endpoint(0));
  if (!Check(source.HasValue(), "Listener creation failed")) {
    return false;
  }
  auto source_address = source->LocalAddress();
  if (!Check(source_address.HasValue(), "Listener local address lookup failed")) {
    return false;
  }

  alyrn::uring::Listener moved(std::move(*source));
  auto moved_address = moved.LocalAddress();
  if (!Check(moved_address.HasValue() && moved_address->ToPort() == source_address->ToPort(),
             "Listener move construction did not transfer the socket")) {
    return false;
  }

  auto target = alyrn::uring::Listener::Create(&loop, alyrn::net::Endpoint(0));
  if (!Check(target.HasValue(), "Listener move target creation failed")) {
    return false;
  }
  *target = std::move(moved);

  auto target_address = target->LocalAddress();
  return Check(source->Fd() == -1 && moved.Fd() == -1 && target_address.HasValue() &&
                   target_address->ToPort() == source_address->ToPort(),
               "Listener move assignment did not transfer the socket");
}

}  // namespace

int main() {
  static_assert(std::is_move_constructible_v<alyrn::uring::Stream>);
  static_assert(std::is_move_assignable_v<alyrn::uring::Stream>);
  static_assert(std::is_move_constructible_v<alyrn::uring::Listener>);
  static_assert(std::is_move_assignable_v<alyrn::uring::Listener>);

  alyrn::uring::Loop loop;
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
