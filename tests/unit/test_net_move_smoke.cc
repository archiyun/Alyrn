// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <iostream>
#include <utility>

#include "vexo/net/channel.h"
#include "vexo/net/event_loop.h"
#include "vexo/net/socket.h"

namespace {

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cerr << "[FAIL] " << message << '\n';
    return false;
  }
  return true;
}

bool MakeSocketPair(int fds[2]) {
  return ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) == 0;
}

bool TestChannelMove() {
  vexo::net::EventLoop loop;
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
  vexo::net::Channel source(&loop, first[0]);
  source.set_edge_triggered(true);
  source.set_read_callback([&](vexo::time::Timestamp) {
    char byte = 0;
    ::read(first[0], &byte, sizeof(byte));
    read_called = true;
    loop.Quit();
  });

  vexo::net::Channel moved(std::move(source));
  if (!Check(source.fd() == -1 && moved.fd() == first[0],
             "Channel move construction should transfer the fd association") ||
      !Check(moved.IsEdgeTriggered(), "Channel move construction should transfer mode")) {
    ::close(first[0]);
    ::close(first[1]);
    ::close(second[0]);
    ::close(second[1]);
    return false;
  }

  vexo::net::Channel target(&loop, second[0]);
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
  return Check(moved.fd() == -1 && target.fd() == first[0] && read_called,
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

  vexo::net::Socket source(first[0]);
  vexo::net::Socket moved(std::move(source));
  vexo::net::Socket target(second[0]);
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

}  // namespace

int main() {
  const bool channel_ok = TestChannelMove();
  const bool socket_ok = TestSocketMove();
  if (channel_ok && socket_ok) {
    std::cout << "[PASS] net_move_smoke_test\n";
    return 0;
  }
  return 1;
}
