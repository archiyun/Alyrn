// SPDX-License-Identifier: MIT
// io::BlockOn drives a root task on a caller-owned Loop for every backend.

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <span>
#include <string>
#include <string_view>
#include <system_error>

#include "alyrn/epoll.h"
#include "alyrn/io.h"
#include "alyrn/net.h"
#include "alyrn/spawn.h"

#if defined(ALYRN_ENABLE_URING)
#include "alyrn/uring.h"
#endif

namespace {

using alyrn::Result;
using alyrn::Task;

bool Check(bool condition, const char* message) {
  if (!condition) std::cerr << "FAIL: " << message << '\n';
  return condition;
}

Task<int> Answer() { co_return 42; }

Task<void> SetFlag(bool& flag) {
  flag = true;
  co_return;
}

template <class Listener>
Task<Result<void>> EchoOnce(Listener& listener) {
  auto accepted = co_await listener.Accept();
  if (!accepted) {
    co_return std::unexpected(accepted.Error());
  }
  std::array<std::byte, 64> buffer{};
  auto read = co_await accepted->Read(buffer);
  if (!read) {
    co_return std::unexpected(read.Error());
  }
  auto written = co_await accepted->Write(std::span<const std::byte>(buffer.data(), *read));
  if (!written) {
    co_return std::unexpected(written.Error());
  }
  (void)co_await accepted->Close();
  co_return Result<void>{};
}

// A client round trip against a listener on the same loop: the shape of a
// small tool whose main() is a single BlockOn call.
template <class Loop, class Listener, class Connector>
Task<Result<std::string>> PingPong(Loop& loop, Listener& listener, Connector& connector,
                                   std::uint16_t port) {
  auto server = alyrn::Spawn(loop, EchoOnce(listener));

  auto stream = co_await connector.Connect("127.0.0.1", port);
  if (!stream) {
    co_return std::unexpected(stream.Error());
  }
  constexpr std::string_view kPing = "ping";
  auto written = co_await stream->Write(std::as_bytes(std::span(kPing.data(), kPing.size())));
  if (!written) {
    co_return std::unexpected(written.Error());
  }
  std::array<std::byte, 64> buffer{};
  auto read = co_await stream->Read(buffer);
  if (!read) {
    co_return std::unexpected(read.Error());
  }
  (void)co_await stream->Close();

  auto served = co_await std::move(server);
  if (!served) {
    co_return std::unexpected(served.Error());
  }
  (void)co_await listener.Close();
  co_return std::string(reinterpret_cast<const char*>(buffer.data()), *read);
}

bool EpollReturnsValue() {
  alyrn::epoll::Loop loop;
  const int value = alyrn::io::BlockOn(loop, Answer());
  return Check(value == 42, "epoll BlockOn must return the task value") &&
         Check(loop.State() == alyrn::io::LoopState::kStopped, "epoll BlockOn must stop its loop");
}

bool EpollRunsVoidTask() {
  alyrn::epoll::Loop loop;
  bool ran = false;
  alyrn::io::BlockOn(loop, SetFlag(ran));
  return Check(ran, "epoll BlockOn must run a Task<void> to completion");
}

bool EpollDrivesIo() {
  alyrn::epoll::Loop loop;
  auto listener = alyrn::epoll::Listener::Create(&loop, alyrn::net::Endpoint::Loopback(0));
  if (!Check(listener.HasValue(), "epoll listener creation failed")) {
    return false;
  }
  auto address = listener->LocalAddress();
  auto connector = alyrn::epoll::Connector::Create(&loop);
  if (!Check(address.HasValue() && connector.HasValue(), "epoll client setup failed")) {
    return false;
  }
  auto echoed = alyrn::io::BlockOn(loop, PingPong(loop, *listener, *connector, address->ToPort()));
  return Check(echoed.HasValue() && *echoed == "ping", "epoll BlockOn must drive socket I/O");
}

#if defined(ALYRN_ENABLE_URING)

bool IsEnvironmentSkip(std::error_code error) {
  return error == std::errc::operation_not_supported || error == std::errc::operation_not_permitted;
}

// Returns false when io_uring is unavailable in this environment.
bool InitUring(alyrn::uring::Loop& loop, bool& ok) {
  alyrn::uring::Options options;
  options.entries = 64;
  auto initialized = loop.Init(options);
  if (initialized.HasValue()) {
    return true;
  }
  ok = IsEnvironmentSkip(initialized.Error()) ||
       Check(false, "uring loop initialization failed for BlockOn");
  if (ok) {
    std::cout << "SKIP: io_uring unavailable: " << initialized.Error().message() << '\n';
  }
  return false;
}

bool UringReturnsValue() {
  alyrn::uring::Loop loop;
  bool ok = true;
  if (!InitUring(loop, ok)) {
    return ok;
  }
  const int value = alyrn::io::BlockOn(loop, Answer());
  return Check(value == 42, "uring BlockOn must return the task value") &&
         Check(loop.State() == alyrn::io::LoopState::kStopped, "uring BlockOn must stop its loop");
}

bool UringDrivesIo() {
  alyrn::uring::Loop loop;
  bool ok = true;
  if (!InitUring(loop, ok)) {
    return ok;
  }
  auto listener = alyrn::uring::Listener::Create(&loop, alyrn::net::Endpoint::Loopback(0));
  if (!Check(listener.HasValue(), "uring listener creation failed")) {
    return false;
  }
  auto address = listener->LocalAddress();
  auto connector = alyrn::uring::Connector::Create(&loop);
  if (!Check(address.HasValue() && connector.HasValue(), "uring client setup failed")) {
    return false;
  }
  auto echoed = alyrn::io::BlockOn(loop, PingPong(loop, *listener, *connector, address->ToPort()));
  return Check(echoed.HasValue() && *echoed == "ping", "uring BlockOn must drive socket I/O");
}

#endif

}  // namespace

int main() {
  bool ok = EpollReturnsValue();
  ok = EpollRunsVoidTask() && ok;
  ok = EpollDrivesIo() && ok;
#if defined(ALYRN_ENABLE_URING)
  ok = UringReturnsValue() && ok;
  ok = UringDrivesIo() && ok;
#endif
  if (!ok) {
    return 1;
  }
  std::cout << "io BlockOn smoke: PASS\n";
  return 0;
}
