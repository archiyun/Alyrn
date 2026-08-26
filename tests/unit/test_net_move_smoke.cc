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

#include "coropact/result.h"
#include "coropact/coro/spawn.h"
#include "coropact/coro/task.h"
#include "coropact/net/endpoint.h"
#include "coropact/net/socket.h"
#include "coropact/reactor/detail/channel.h"
#include "coropact/reactor/listener.h"
#include "coropact/reactor/loop.h"
#include "coropact/reactor/stream.h"

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
  coropact::reactor::Loop* loop;
};

void DrainChannelRead(void* raw) noexcept {
  auto& context = *static_cast<ChannelReadContext*>(raw);
  char byte = 0;
  ::read(context.fd, &byte, sizeof(byte));
  *context.called = true;
  context.loop->RequestStop();
}

bool MakeSocketPair(int fds[2]) {
  return ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) == 0;
}

bool TestChannelMove() {
  coropact::reactor::Loop loop;
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
  coropact::reactor::detail::Channel source(&loop, first[0]);
  source.SetEdgeTriggered(true);
  ChannelReadContext context{first[0], &read_called, &loop};
  source.SetReadCallback(DrainChannelRead, &context);

  coropact::reactor::detail::Channel moved(std::move(source));
  if (!Check(source.Fd() == -1 && moved.Fd() == first[0],
             "Channel move construction should transfer the fd association") ||
      !Check(moved.IsEdgeTriggered(), "Channel move construction should transfer mode")) {
    ::close(first[0]);
    ::close(first[1]);
    ::close(second[0]);
    ::close(second[1]);
    return false;
  }

  coropact::reactor::detail::Channel target(&loop, second[0]);
  target = std::move(moved);
  ::close(second[0]);

  target.EnableReading();
  ::write(first[1], "x", 1);
  loop.RunAfter(coropact::time::Milliseconds(100), [&] { loop.RequestStop(); });
  loop.Run();

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

using ReadResult = coropact::Result<std::size_t>;
using AcceptResult = coropact::Result<coropact::reactor::Listener::StreamType>;

static_assert(std::is_move_constructible_v<coropact::reactor::Stream>);
static_assert(std::is_move_assignable_v<coropact::reactor::Stream>);
static_assert(std::is_move_constructible_v<coropact::reactor::Listener>);
static_assert(std::is_move_assignable_v<coropact::reactor::Listener>);

coropact::coro::DetachedTask ReadOnce(coropact::reactor::Stream* stream,
                                      coropact::reactor::Loop* loop,
                                      std::array<std::byte, 16>* buffer,
                                      std::optional<ReadResult>* result) {
  result->emplace(co_await stream->ReadSome(*buffer));
  loop->RequestStop();
}

coropact::coro::DetachedTask AcceptOnce(coropact::reactor::Listener* listener,
                                        coropact::reactor::Loop* loop,
                                        std::optional<AcceptResult>* result) {
  result->emplace(co_await listener->Accept());
  loop->RequestStop();
}

bool TestReactorStreamMove() {
  int source_pair[2]{-1, -1};
  int target_pair[2]{-1, -1};
  if (!Check(MakeSocketPair(source_pair) && MakeSocketPair(target_pair),
             "Stream socketpair creation failed")) {
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
    coropact::reactor::Loop loop;
    coropact::reactor::Stream source(&loop, source_pair[0]);
    coropact::reactor::Stream moved(std::move(source));

    coropact::coro::SpawnDetach(loop,
                                ReadOnce(&moved, &loop, &constructed_buffer, &constructed_result));
    loop.RunAfter(coropact::time::Duration::zero(),
                  [peer_fd = source_pair[1]] { ::write(peer_fd, "c", 1); });
    loop.RunAfter(coropact::time::Milliseconds(200), [&] { loop.RequestStop(); });
    loop.Run();
  }

  if (!Check(constructed_result.has_value() && constructed_result->has_value() &&
                 **constructed_result == 1,
             "Stream move construction lost the read callback")) {
    ::close(source_pair[1]);
    ::close(target_pair[0]);
    ::close(target_pair[1]);
    return false;
  }

  std::optional<ReadResult> assigned_result;
  std::array<std::byte, 16> assigned_buffer{};
  {
    coropact::reactor::Loop loop;
    coropact::reactor::Stream source(&loop, target_pair[0]);
    coropact::reactor::Stream target(&loop, source_pair[1]);
    target = std::move(source);

    coropact::coro::SpawnDetach(loop, ReadOnce(&target, &loop, &assigned_buffer, &assigned_result));
    loop.RunAfter(coropact::time::Duration::zero(),
                  [peer_fd = target_pair[1]] { ::write(peer_fd, "a", 1); });
    loop.RunAfter(coropact::time::Milliseconds(200), [&] { loop.RequestStop(); });
    loop.Run();
  }
  ::close(target_pair[1]);

  return Check(
      assigned_result.has_value() && assigned_result->has_value() && **assigned_result == 1,
      "Stream move assignment lost the read callback");
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
  coropact::reactor::Loop loop;
  coropact::reactor::Listener source(&loop, coropact::net::Endpoint(0));
  auto source_address = source.LocalAddress();
  if (!Check(source_address.has_value(), "Listener local address lookup failed")) {
    return false;
  }

  std::optional<AcceptResult> accepted;
  int client_fd = -1;
  {
    coropact::reactor::Listener moved(std::move(source));
    auto moved_address = moved.LocalAddress();
    if (!Check(moved_address.has_value() && moved_address->ToPort() == source_address->ToPort(),
               "Listener move construction did not transfer the socket")) {
      return false;
    }

    coropact::coro::SpawnDetach(loop, AcceptOnce(&moved, &loop, &accepted));
    loop.RunAfter(coropact::time::Duration::zero(),
                  [&] { client_fd = ConnectNonBlocking(*moved_address); });
    loop.RunAfter(coropact::time::Milliseconds(200), [&] { loop.RequestStop(); });
    loop.Run();
  }

  if (client_fd >= 0) {
    ::close(client_fd);
  }

  if (!Check(accepted.has_value() && accepted->has_value(),
             "Listener move construction lost the accept callback")) {
    return false;
  }

  coropact::reactor::Listener assigned_source(&loop, coropact::net::Endpoint(0));
  auto assigned_source_address = assigned_source.LocalAddress();
  coropact::reactor::Listener assigned_target(&loop, coropact::net::Endpoint(0));
  assigned_target = std::move(assigned_source);
  auto assigned_target_address = assigned_target.LocalAddress();
  return Check(assigned_source_address.has_value() && assigned_target_address.has_value() &&
                   assigned_target_address->ToPort() == assigned_source_address->ToPort(),
               "Listener move assignment did not transfer the socket");
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
