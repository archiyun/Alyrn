// SPDX-License-Identifier: MIT

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <expected>
#include <iostream>
#include <system_error>
#include <thread>
#include <utility>

#include "alyrn/result.h"
#include "alyrn/uring/detail/server.h"
#include "alyrn/uring/stream.h"
#include "alyrn/net/endpoint.h"

namespace {

class UniqueFd {
public:
  UniqueFd() = default;
  explicit UniqueFd(int fd) noexcept : fd_(fd) {}
  UniqueFd(const UniqueFd&) = delete;
  UniqueFd& operator=(const UniqueFd&) = delete;

  UniqueFd(UniqueFd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
  UniqueFd& operator=(UniqueFd&& other) noexcept {
    if (this != &other) {
      Reset();
      fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
  }

  ~UniqueFd() { Reset(); }

  void Reset(int fd = -1) noexcept {
    if (fd_ >= 0) {
      ::close(fd_);
    }
    fd_ = fd;
  }

private:
  int fd_{-1};
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

alyrn::net::Endpoint LoopbackAddress(std::uint16_t port) {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(port);
  return alyrn::net::Endpoint(addr);
}

alyrn::Result<std::uint16_t> PickFreePort() {
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    return std::unexpected(alyrn::CurrentErrno());
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(0);

  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    auto error = alyrn::CurrentErrno();
    ::close(fd);
    return std::unexpected(error);
  }

  socklen_t len = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) < 0) {
    auto error = alyrn::CurrentErrno();
    ::close(fd);
    return std::unexpected(error);
  }

  ::close(fd);
  return ntohs(addr.sin_port);
}

alyrn::Result<int> ConnectClient(const alyrn::net::Endpoint& address) {
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    return std::unexpected(alyrn::CurrentErrno());
  }

  int r = ::connect(fd, address.SockAddr(), address.SockAddrLen());
  if (r < 0 && errno != EINPROGRESS) {
    auto error = alyrn::CurrentErrno();
    ::close(fd);
    return std::unexpected(error);
  }

  return fd;
}

alyrn::uring::detail::ServerOptions MakeOptions(std::size_t worker_num = 1) {
  alyrn::uring::detail::ServerOptions options;
  options.worker_group_options.worker_num = worker_num;
  options.worker_group_options.worker_options.loop_options.entries = 16;
  options.worker_group_options.worker_options.listen_options.reuse_port = true;
  return options;
}

bool CheckServerStartStop() {
  auto port = PickFreePort();
  if (!port.has_value()) {
    if (IsEnvironmentSkip(port.error())) {
      std::cout << "SKIP: TCP bind unavailable: " << port.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: PickFreePort failed: " << port.error().message() << '\n';
    return false;
  }

  alyrn::uring::detail::Server server(LoopbackAddress(*port), MakeOptions());

  auto started = server.Start();
  if (!started.has_value()) {
    if (IsEnvironmentSkip(started.error())) {
      std::cout << "SKIP: io_uring unavailable: " << started.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: Server::Start failed: " << started.error().message() << '\n';
    return false;
  }

  auto second_start = server.Start();
  bool ok = Check(server.Started(), "server should be started") &&
            Check(!second_start.has_value(), "second Start should fail") &&
            Check(second_start.error().value() == EALREADY, "second Start should return EALREADY");

  server.Stop();

  return ok && Check(!server.Started(), "server should stop");
}

bool CheckServerSessionHandler() {
  auto port = PickFreePort();
  if (!port.has_value()) {
    if (IsEnvironmentSkip(port.error())) {
      std::cout << "SKIP: TCP bind unavailable: " << port.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: PickFreePort failed: " << port.error().message() << '\n';
    return false;
  }

  const auto listen_addr = LoopbackAddress(*port);
  alyrn::uring::detail::Server server(listen_addr, MakeOptions());

  std::atomic_size_t session_count{0};
  std::atomic_bool invalid_stream{false};
  std::atomic_bool wrong_loop{false};
  server.SetSessionHandler([&](alyrn::uring::detail::WorkerContext& context,
                                 alyrn::uring::Stream stream) -> alyrn::coro::DetachedTask {
    if (!context.loop.IsInLoopThread()) {
      wrong_loop.store(true, std::memory_order_relaxed);
    }
    if (stream.Fd() < 0) {
      invalid_stream.store(true, std::memory_order_relaxed);
    }
    session_count.fetch_add(1, std::memory_order_relaxed);
    co_return;
  });

  auto started = server.Start();
  if (!started.has_value()) {
    if (IsEnvironmentSkip(started.error())) {
      std::cout << "SKIP: io_uring unavailable: " << started.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: Server::Start failed: " << started.error().message() << '\n';
    return false;
  }

  auto client_fd = ConnectClient(listen_addr);
  if (!client_fd.has_value()) {
    server.Stop();
    if (IsEnvironmentSkip(client_fd.error())) {
      std::cout << "SKIP: TCP connect unavailable: " << client_fd.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: client connect failed: " << client_fd.error().message() << '\n';
    return false;
  }
  UniqueFd client(*client_fd);

  for (int i = 0; i < 200 && session_count.load(std::memory_order_relaxed) == 0; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }

  bool ok = Check(session_count.load(std::memory_order_relaxed) == 1,
                  "session handler should run once") &&
            Check(!invalid_stream.load(std::memory_order_relaxed),
                  "session handler received an invalid stream") &&
            Check(!wrong_loop.load(std::memory_order_relaxed),
                  "session handler should run in the worker loop thread");

  server.Stop();

  return ok && Check(!server.Started(), "server should stop after session test");
}

bool CheckServerStopsActiveSession() {
  auto port = PickFreePort();
  if (!port.has_value()) {
    if (IsEnvironmentSkip(port.error())) {
      std::cout << "SKIP: TCP bind unavailable: " << port.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: PickFreePort failed: " << port.error().message() << '\n';
    return false;
  }

  const auto listen_addr = LoopbackAddress(*port);
  alyrn::uring::detail::Server server(listen_addr, MakeOptions());
  std::atomic_bool session_started{false};
  std::atomic_bool session_cancelled{false};

  server.SetSessionHandler(
      [&](alyrn::uring::detail::WorkerContext&, alyrn::uring::Stream stream)
          -> alyrn::coro::DetachedTask {
        session_started.store(true, std::memory_order_release);
        std::array<std::byte, 1> buffer{};
        auto result = co_await stream.ReadSome(buffer);
        if (!result.has_value() && result.error().value() == ECANCELED) {
          session_cancelled.store(true, std::memory_order_release);
        }
      });

  auto started = server.Start();
  if (!started.has_value()) {
    if (IsEnvironmentSkip(started.error())) {
      std::cout << "SKIP: io_uring unavailable: " << started.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: Server::Start failed: " << started.error().message() << '\n';
    return false;
  }

  auto client_fd = ConnectClient(listen_addr);
  if (!client_fd.has_value()) {
    server.Stop();
    if (IsEnvironmentSkip(client_fd.error())) {
      std::cout << "SKIP: TCP connect unavailable: " << client_fd.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: client connect failed: " << client_fd.error().message() << '\n';
    return false;
  }
  UniqueFd client(*client_fd);

  for (int i = 0; i < 200 && !session_started.load(std::memory_order_acquire); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }

  const bool accepted = Check(session_started.load(std::memory_order_acquire),
                              "active session should start before stop");
  server.Stop();

  return accepted &&
         Check(session_cancelled.load(std::memory_order_acquire),
               "active session should receive ECANCELED during stop") &&
         Check(!server.Started(), "server should stop after cancelling active session");
}

}  // namespace

int main() {
  if (!CheckServerStartStop()) return 1;
  if (!CheckServerSessionHandler()) return 1;
  if (!CheckServerStopsActiveSession()) return 1;

  std::cout << "luring server smoke: PASS\n";
  return 0;
}
