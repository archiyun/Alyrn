// SPDX-License-Identifier: MIT
// A server whose descriptors are exhausted (accept returns EMFILE) must keep
// serving its existing connections, recover once descriptors free up, and stop
// cleanly. The regression it guards: epoll retried a transient accept failure
// in a tight loop that never yielded, pinning a worker at 100% CPU and wedging
// the loop permanently; io_uring stopped accepting for good after one EMFILE.
//
// The server runs in a forked child with a lowered RLIMIT_NOFILE, so only the
// child's descriptors are exhausted. The parent drives clients with its own
// ample budget and enforces a wall-clock deadline: a wedged loop never stops,
// so the parent kills it and fails instead of hanging.

#include <netinet/in.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <expected>
#include <span>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "alyrn/coro/detached_task.h"
#include "alyrn/epoll/runtime.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/result.h"

#ifdef ALYRN_ENABLE_URING
#include "alyrn/uring/runtime.h"
#endif

namespace {

namespace cp = alyrn;
using namespace std::chrono_literals;

bool Check(bool condition, const char* message) {
  if (!condition) std::printf("FAIL: %s\n", message);
  return condition;
}

// The server's per-connection work: a plain echo.
template <class Stream>
cp::coro::DetachedTask Echo(Stream stream) {
  std::array<std::byte, 1024> buffer{};
  for (;;) {
    auto read = co_await stream.Read(buffer);
    if (!read.HasValue() || *read == 0) break;
    auto written = co_await stream.Write(std::span<const std::byte>(buffer.data(), *read));
    if (!written.HasValue()) break;
  }
  (void)co_await stream.Close();
}

std::uint16_t PickLoopbackPort() {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) return 0;
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(0);
  std::uint16_t port = 0;
  socklen_t length = sizeof(address);
  if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0 &&
      ::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length) == 0) {
    port = ntohs(address.sin_port);
  }
  (void)::close(fd);
  return port;
}

int ConnectLoopback(std::uint16_t port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) return -1;
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(port);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
    (void)::close(fd);
    return -1;
  }
  return fd;
}

void SetTimeouts(int fd, std::chrono::milliseconds timeout) {
  timeval tv{};
  tv.tv_sec = timeout.count() / 1000;
  tv.tv_usec = (timeout.count() % 1000) * 1000;
  (void)::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  (void)::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

// Sends payload and reads exactly its length back within the socket timeout.
bool EchoRoundTrip(int fd, std::string_view payload) {
  if (::send(fd, payload.data(), payload.size(), MSG_NOSIGNAL) !=
      static_cast<ssize_t>(payload.size())) {
    return false;
  }
  std::string got;
  got.reserve(payload.size());
  while (got.size() < payload.size()) {
    char chunk[256];
    const ssize_t n = ::recv(fd, chunk, sizeof(chunk), 0);
    if (n <= 0) return false;
    got.append(chunk, static_cast<std::size_t>(n));
  }
  return got == payload;
}

constexpr int kSkipExit = 42;

// Runs the echo server until SIGTERM, under a lowered descriptor limit. Never
// returns: the child process exits with 0 (clean stop), kSkipExit (backend
// unavailable), or 1 (start failure).
template <class Tag>
[[noreturn]] void RunChildServer(std::uint16_t port, rlim_t nofile) {
  rlimit limit{nofile, nofile};
  (void)::setrlimit(RLIMIT_NOFILE, &limit);

  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &set, nullptr);

  std::stop_source source;
  std::thread waiter([&set, &source] {
    int signal = 0;
    (void)sigwait(&set, &signal);
    source.request_stop();
  });
  waiter.detach();

  auto runtime = cp::Runtime::Builder<Tag>{cp::net::Endpoint::Loopback(port)}
                     .OnConnection([](auto stream) { return Echo(std::move(stream)); })
                     .Build();
  auto ran = runtime.Run(source.get_token());
  if (ran.HasValue()) {
    _exit(0);
  }
  const bool skip = ran.Error() == std::errc::operation_not_supported ||
                    ran.Error() == std::errc::operation_not_permitted;
  _exit(skip ? kSkipExit : 1);
}

// Forks the server, exhausts its descriptors from the parent, and checks that
// existing connections keep working, new ones recover, and the loop stops.
template <class Tag>
bool RunExhaustionTest(const char* name) {
  const std::uint16_t port = PickLoopbackPort();
  if (!Check(port != 0, "could not reserve a loopback port")) return false;

  // Small enough to exhaust from the parent, large enough to start the server
  // and accept a first connection.
  constexpr rlim_t kChildNofile = 128;
  const pid_t child = ::fork();
  if (!Check(child >= 0, "fork failed")) return false;
  if (child == 0) {
    RunChildServer<Tag>(port, kChildNofile);  // never returns
  }

  bool ok = true;
  auto reap = [&](int grace_ms) -> bool {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(grace_ms);
    while (std::chrono::steady_clock::now() < deadline) {
      int status = 0;
      const pid_t reaped = ::waitpid(child, &status, WNOHANG);
      if (reaped == child) {
        return Check(WIFEXITED(status) && WEXITSTATUS(status) == 0,
                     "server did not exit cleanly");
      }
      std::this_thread::sleep_for(10ms);
    }
    return false;  // still running
  };
  auto force_kill = [&] {
    ::kill(child, SIGKILL);
    int status = 0;
    (void)::waitpid(child, &status, 0);
  };

  // Wait for the server, connecting a keep-alive connection. A child that exits
  // early with kSkipExit means the backend is unavailable here.
  int keeper = -1;
  for (int attempt = 0; attempt < 500 && keeper < 0; ++attempt) {
    keeper = ConnectLoopback(port);
    if (keeper >= 0) break;
    int status = 0;
    if (::waitpid(child, &status, WNOHANG) == child) {
      if (WIFEXITED(status) && WEXITSTATUS(status) == kSkipExit) {
        std::printf("SKIP [%s]: backend unavailable\n", name);
        return true;
      }
      return Check(false, "server exited before accepting a connection");
    }
    std::this_thread::sleep_for(10ms);
  }
  if (!Check(keeper >= 0, "could not establish the keep-alive connection")) {
    force_kill();
    return false;
  }
  SetTimeouts(keeper, 3s);
  ok = Check(EchoRoundTrip(keeper, "ping"), "baseline echo failed") && ok;

  // Exhaust the child's descriptors: many connections it cannot accept.
  std::vector<int> hogs;
  hogs.reserve(512);
  for (int i = 0; i < 512; ++i) {
    const int fd = ConnectLoopback(port);
    if (fd >= 0) hogs.push_back(fd);
  }
  std::this_thread::sleep_for(300ms);  // let the server hit EMFILE

  // The heart of the regression: a starved or spinning loop cannot answer.
  ok = Check(EchoRoundTrip(keeper, "ping-under-pressure"),
             "existing connection was starved under descriptor exhaustion") &&
       ok;

  // Free the descriptors; the server must recover and accept again. A stopped
  // listener still completes the TCP handshake in the kernel, so a broken
  // server accepts the connection but never echoes; bound the whole retry by a
  // wall-clock deadline instead of the per-attempt socket timeout.
  for (const int fd : hogs) (void)::close(fd);
  hogs.clear();
  bool recovered = false;
  const auto recover_deadline = std::chrono::steady_clock::now() + 5s;
  while (!recovered && std::chrono::steady_clock::now() < recover_deadline) {
    const int fd = ConnectLoopback(port);
    if (fd >= 0) {
      SetTimeouts(fd, 500ms);
      recovered = EchoRoundTrip(fd, "recovered");
      (void)::close(fd);
    }
    if (!recovered) std::this_thread::sleep_for(20ms);
  }
  ok = Check(recovered, "server did not recover after descriptors were freed") && ok;

  (void)::close(keeper);

  // A wedged loop never processes the stop request; the deadline turns that
  // into a failure instead of a hang.
  ::kill(child, SIGTERM);
  if (!reap(10000)) {
    ok = Check(false, "server did not stop within the deadline (loop wedged)");
    force_kill();
    return false;
  }

  if (ok) std::printf("accept resource exhaustion [%s]: PASS\n", name);
  return ok;
}

}  // namespace

int main() {
  // A dead child must not raise SIGPIPE in the parent's send() calls.
  ::signal(SIGPIPE, SIG_IGN);

  bool ok = RunExhaustionTest<cp::runtime::Epoll>("epoll");
#ifdef ALYRN_ENABLE_URING
  ok = RunExhaustionTest<cp::runtime::Uring>("uring") && ok;
#endif
  return ok ? 0 : 1;
}
