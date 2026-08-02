// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <iostream>
#include <optional>
#include <type_traits>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/coro/spawn.h"
#include "coropact/coro/task.h"
#include "coropact/net/endpoint.h"
#include "coropact/net/socket.h"
#include "coropact/reactor/channel.h"
#include "coropact/reactor/event_loop.h"
#include "coropact/reactor/reactor_listener.h"
#include "coropact/reactor/reactor_stream.h"

namespace {

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cerr << "[FAIL] " << message << '\n';
    return false;
  }
  return true;
}

struct ChannelReadContext {
  int fd;
  bool* called;
  coropact::reactor::EventLoop* loop;
};

void DrainChannelRead(void* raw, coropact::time::Timestamp) noexcept {
  auto& context = *static_cast<ChannelReadContext*>(raw);
  char byte = 0;
  ::read(context.fd, &byte, sizeof(byte));
  *context.called = true;
  context.loop->Quit();
}

bool MakeSocketPair(int fds[2]) {
  return ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) == 0;
}

bool TestChannelMove() {
  coropact::reactor::EventLoop loop;
  int first[2]{-1, -1};
  int second[2]{-1, -1};
  if (!Check(MakeSocketPair(first) && MakeSocketPair(second), "socketpair creation failed")) {
    for (const int fd : first) {
      if (fd >= 0) ::close(fd);
    }
    for (const int fd : second) {
      if (fd >= 0) ::close(fd);
    }
    return false;
  }

  bool read_called = false;
  coropact::reactor::Channel source(&loop, first[0]);
  source.SetEdgeTriggered(true);
  ChannelReadContext context{first[0], &read_called, &loop};
  source.SetReadCallback(DrainChannelRead, &context);

  coropact::reactor::Channel moved(std::move(source));
  if (!Check(source.Fd() == -1 && moved.Fd() == first[0],
             "Channel move construction should transfer the fd association") ||
      !Check(moved.IsEdgeTriggered(), "Channel move construction should transfer mode")) {
    ::close(first[0]);
    ::close(first[1]);
    ::close(second[0]);
    ::close(second[1]);
    return false;
  }

  coropact::reactor::Channel target(&loop, second[0]);
  target = std::move(moved);
  ::close(second[0]);

  target.EnableReading();
  ::write(first[1], "x", 1);
  loop.RunAfter(0.1, [&] { loop.Quit(); });
  loop.Loop();

  target.DisableAll();
  target.Remove();
  ::close(first[0]);
  ::close(first[1]);
  ::close(second[1]);
  return Check(moved.Fd() == -1 && target.Fd() == first[0] && read_called,
               "Channel move assignment should preserve callbacks and registration use");
}

bool TestSocketMove() {
  int first[2]{-1, -1};
  int second[2]{-1, -1};
  if (!Check(MakeSocketPair(first) && MakeSocketPair(second), "socketpair creation failed")) {
    for (const int fd : first) {
      if (fd >= 0) ::close(fd);
    }
    for (const int fd : second) {
      if (fd >= 0) ::close(fd);
    }
    return false;
  }

  coropact::net::Socket source(first[0]);
  coropact::net::Socket moved(std::move(source));
  coropact::net::Socket target(second[0]);
  const int replaced_fd = target.fd();
  target = std::move(moved);

  errno = 0;
  const bool replaced_closed = ::fcntl(replaced_fd, F_GETFD) == -1 && errno == EBADF;
  const bool transferred = source.fd() == -1 && moved.fd() == -1 && target.fd() == first[0];

  target.Close();
  ::close(first[1]);
  ::close(second[1]);
  return Check(transferred && replaced_closed,
               "Socket move operations should transfer ownership and close the old target fd");
}

using ReadResult = coropact::base::Result<std::size_t>;
using AcceptResult = coropact::base::Result<coropact::reactor::ReactorListener::Stream>;

static_assert(std::is_move_constructible_v<coropact::reactor::ReactorStream>);
static_assert(std::is_move_assignable_v<coropact::reactor::ReactorStream>);
static_assert(std::is_move_constructible_v<coropact::reactor::ReactorListener>);
static_assert(std::is_move_assignable_v<coropact::reactor::ReactorListener>);

coropact::coro::DetachedTask ReadOnce(coropact::reactor::ReactorStream* stream,
                                      coropact::reactor::EventLoop* loop,
                                      std::array<std::byte, 16>* buffer,
                                      std::optional<ReadResult>* result) {
  result->emplace(co_await stream->ReadSome(*buffer));
  loop->Quit();
}

coropact::coro::DetachedTask AcceptOnce(coropact::reactor::ReactorListener* listener,
                                        coropact::reactor::EventLoop* loop,
                                        std::optional<AcceptResult>* result) {
  result->emplace(co_await listener->Accept());
  loop->Quit();
}

bool TestReactorStreamMove() {
  int source_pair[2]{-1, -1};
  int target_pair[2]{-1, -1};
  if (!Check(MakeSocketPair(source_pair) && MakeSocketPair(target_pair),
             "ReactorStream socketpair creation failed")) {
    for (const int fd : source_pair) {
      if (fd >= 0) ::close(fd);
    }
    for (const int fd : target_pair) {
      if (fd >= 0) ::close(fd);
    }
    return false;
  }

  std::optional<ReadResult> constructed_result;
  std::array<std::byte, 16> constructed_buffer{};
  {
    coropact::reactor::EventLoop loop;
    coropact::reactor::ReactorStream source(&loop, source_pair[0]);
    coropact::reactor::ReactorStream moved(std::move(source));

    coropact::coro::SpawnDetach(loop,
                                ReadOnce(&moved, &loop, &constructed_buffer, &constructed_result));
    loop.DeferOnOwner([peer_fd = source_pair[1]] { ::write(peer_fd, "c", 1); });
    loop.RunAfter(0.2, [&] { loop.Quit(); });
    loop.Loop();
  }

  if (!Check(constructed_result.has_value() && constructed_result->has_value() &&
                 **constructed_result == 1,
             "ReactorStream move construction lost the read callback")) {
    ::close(source_pair[1]);
    ::close(target_pair[0]);
    ::close(target_pair[1]);
    return false;
  }

  std::optional<ReadResult> assigned_result;
  std::array<std::byte, 16> assigned_buffer{};
  {
    coropact::reactor::EventLoop loop;
    coropact::reactor::ReactorStream source(&loop, target_pair[0]);
    coropact::reactor::ReactorStream target(&loop, source_pair[1]);
    target = std::move(source);

    coropact::coro::SpawnDetach(loop,
                                ReadOnce(&target, &loop, &assigned_buffer, &assigned_result));
    loop.DeferOnOwner([peer_fd = target_pair[1]] { ::write(peer_fd, "a", 1); });
    loop.RunAfter(0.2, [&] { loop.Quit(); });
    loop.Loop();
  }
  ::close(target_pair[1]);

  return Check(
      assigned_result.has_value() && assigned_result->has_value() && **assigned_result == 1,
      "ReactorStream move assignment lost the read callback");
}

int ConnectNonBlocking(const coropact::net::Endpoint& address) {
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    return -1;
  }

  const int rc = ::connect(fd, address.SockAddr(), address.SockAddrLen());
  if (rc == 0 || errno == EINPROGRESS) {
    return fd;
  }

  ::close(fd);
  return -1;
}

bool TestReactorListenerMove() {
  coropact::reactor::EventLoop loop;
  coropact::reactor::ReactorListener source(&loop, coropact::net::Endpoint(0));
  auto source_address = source.LocalAddress();
  if (!Check(source_address.has_value(), "ReactorListener local address lookup failed")) {
    return false;
  }

  std::optional<AcceptResult> accepted;
  int client_fd = -1;
  {
    coropact::reactor::ReactorListener moved(std::move(source));
    auto moved_address = moved.LocalAddress();
    if (!Check(moved_address.has_value() && moved_address->ToPort() == source_address->ToPort(),
               "ReactorListener move construction did not transfer the socket")) {
      return false;
    }

    coropact::coro::SpawnDetach(loop, AcceptOnce(&moved, &loop, &accepted));
    loop.DeferOnOwner([&] { client_fd = ConnectNonBlocking(*moved_address); });
    loop.RunAfter(0.2, [&] { loop.Quit(); });
    loop.Loop();
  }

  if (client_fd >= 0) {
    ::close(client_fd);
  }

  if (!Check(accepted.has_value() && accepted->has_value(),
             "ReactorListener move construction lost the accept callback")) {
    return false;
  }

  coropact::reactor::ReactorListener assigned_source(&loop, coropact::net::Endpoint(0));
  auto assigned_source_address = assigned_source.LocalAddress();
  coropact::reactor::ReactorListener assigned_target(&loop, coropact::net::Endpoint(0));
  assigned_target = std::move(assigned_source);
  auto assigned_target_address = assigned_target.LocalAddress();
  return Check(assigned_source_address.has_value() && assigned_target_address.has_value() &&
                   assigned_target_address->ToPort() == assigned_source_address->ToPort(),
               "ReactorListener move assignment did not transfer the socket");
}

}  // namespace

int main() {
  const bool channel_ok = TestChannelMove();
  const bool socket_ok = TestSocketMove();
  const bool stream_ok = TestReactorStreamMove();
  const bool listener_ok = TestReactorListenerMove();
  if (channel_ok && socket_ok && stream_ok && listener_ok) {
    std::cout << "[PASS] net_move_smoke_test\n";
    return 0;
  }
  return 1;
}
