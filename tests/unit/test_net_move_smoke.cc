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

#include "alyrn/coro/spawn.h"
#include "alyrn/epoll/detail/channel.h"
#include "alyrn/epoll/listener.h"
#include "alyrn/epoll/loop.h"
#include "alyrn/epoll/stream.h"
#include "alyrn/net/detail/socket.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/result.h"

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
  alyrn::epoll::Loop* loop;
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
  alyrn::epoll::Loop loop;
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
  alyrn::epoll::detail::Channel source(&loop, first[0]);
  source.SetEdgeTriggered(true);
  ChannelReadContext context{first[0], &read_called, &loop};
  source.SetReadCallback(DrainChannelRead, &context);

  alyrn::epoll::detail::Channel moved(std::move(source));
  if (!Check(source.Fd() == -1 && moved.Fd() == first[0],
             "Channel move construction should transfer the fd association") ||
      !Check(moved.IsEdgeTriggered(), "Channel move construction should transfer mode")) {
    ::close(first[0]);
    ::close(first[1]);
    ::close(second[0]);
    ::close(second[1]);
    return false;
  }

  alyrn::epoll::detail::Channel target(&loop, second[0]);
  target = std::move(moved);
  ::close(second[0]);

  target.EnableReading();
  ::write(first[1], "x", 1);
  loop.RunAfter(alyrn::time::Milliseconds(100), [&] { loop.RequestStop(); });
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

  alyrn::net::Socket source(first[0]);
  alyrn::net::Socket moved(std::move(source));
  alyrn::net::Socket target(second[0]);
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

using ReadResult = alyrn::Result<std::size_t>;
using AcceptResult = alyrn::Result<alyrn::epoll::Listener::StreamType>;

static_assert(std::is_move_constructible_v<alyrn::epoll::Stream>);
static_assert(std::is_move_assignable_v<alyrn::epoll::Stream>);
static_assert(std::is_move_constructible_v<alyrn::epoll::Listener>);
static_assert(std::is_move_assignable_v<alyrn::epoll::Listener>);

alyrn::coro::DetachedTask ReadOnce(alyrn::epoll::Stream* stream, alyrn::epoll::Loop* loop,
                                   std::array<std::byte, 16>* buffer,
                                   std::optional<ReadResult>* result) {
  result->emplace(co_await stream->Read(*buffer));
  loop->RequestStop();
}

alyrn::coro::DetachedTask AcceptOnce(alyrn::epoll::Listener* listener, alyrn::epoll::Loop* loop,
                                     std::optional<AcceptResult>* result) {
  result->emplace(co_await listener->Accept());
  loop->RequestStop();
}

bool TestEpollStreamMove() {
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
    alyrn::epoll::Loop loop;
    alyrn::epoll::Stream source(&loop, source_pair[0]);
    alyrn::epoll::Stream moved(std::move(source));

    alyrn::coro::SpawnDetach(loop,
                             ReadOnce(&moved, &loop, &constructed_buffer, &constructed_result));
    loop.RunAfter(alyrn::time::Duration::zero(),
                  [peer_fd = source_pair[1]] { ::write(peer_fd, "c", 1); });
    loop.RunAfter(alyrn::time::Milliseconds(200), [&] { loop.RequestStop(); });
    loop.Run();
  }

  if (!Check(constructed_result.has_value() && constructed_result->HasValue() &&
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
    alyrn::epoll::Loop loop;
    alyrn::epoll::Stream source(&loop, target_pair[0]);
    alyrn::epoll::Stream target(&loop, source_pair[1]);
    target = std::move(source);

    alyrn::coro::SpawnDetach(loop, ReadOnce(&target, &loop, &assigned_buffer, &assigned_result));
    loop.RunAfter(alyrn::time::Duration::zero(),
                  [peer_fd = target_pair[1]] { ::write(peer_fd, "a", 1); });
    loop.RunAfter(alyrn::time::Milliseconds(200), [&] { loop.RequestStop(); });
    loop.Run();
  }
  ::close(target_pair[1]);

  return Check(
      assigned_result.has_value() && assigned_result->HasValue() && **assigned_result == 1,
      "Stream move assignment lost the read callback");
}

int ConnectNonBlocking(const alyrn::net::Endpoint& address) {
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

bool TestEpollListenerMove() {
  alyrn::epoll::Loop loop;
  alyrn::epoll::Listener source(&loop, alyrn::net::Endpoint(0));
  auto source_address = source.LocalAddress();
  if (!Check(source_address.HasValue(), "Listener local address lookup failed")) {
    return false;
  }

  std::optional<AcceptResult> accepted;
  int client_fd = -1;
  {
    alyrn::epoll::Listener moved(std::move(source));
    auto moved_address = moved.LocalAddress();
    if (!Check(moved_address.HasValue() && moved_address->ToPort() == source_address->ToPort(),
               "Listener move construction did not transfer the socket")) {
      return false;
    }

    alyrn::coro::SpawnDetach(loop, AcceptOnce(&moved, &loop, &accepted));
    loop.RunAfter(alyrn::time::Duration::zero(),
                  [&] { client_fd = ConnectNonBlocking(*moved_address); });
    loop.RunAfter(alyrn::time::Milliseconds(200), [&] { loop.RequestStop(); });
    loop.Run();
  }

  if (client_fd >= 0) {
    ::close(client_fd);
  }

  if (!Check(accepted.has_value() && accepted->HasValue(),
             "Listener move construction lost the accept callback")) {
    return false;
  }

  alyrn::epoll::Listener assigned_source(&loop, alyrn::net::Endpoint(0));
  auto assigned_source_address = assigned_source.LocalAddress();
  alyrn::epoll::Listener assigned_target(&loop, alyrn::net::Endpoint(0));
  assigned_target = std::move(assigned_source);
  auto assigned_target_address = assigned_target.LocalAddress();
  return Check(assigned_source_address.HasValue() && assigned_target_address.HasValue() &&
                   assigned_target_address->ToPort() == assigned_source_address->ToPort(),
               "Listener move assignment did not transfer the socket");
}

}  // namespace

int main() {
  const bool channel_ok = TestChannelMove();
  const bool socket_ok = TestSocketMove();
  const bool stream_ok = TestEpollStreamMove();
  const bool listener_ok = TestEpollListenerMove();
  if (channel_ok && socket_ok && stream_ok && listener_ok) {
    std::cout << "[PASS] net_move_smoke_test\n";
    return 0;
  }
  return 1;
}
