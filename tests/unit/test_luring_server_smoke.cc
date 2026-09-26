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

#include "../contracts/worker_exit.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/result.h"
#include "alyrn/uring/detail/server.h"
#include "alyrn/uring/stream.h"

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
  if (!port.HasValue()) {
    if (IsEnvironmentSkip(port.Error())) {
      std::cout << "SKIP: TCP bind unavailable: " << port.Error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: PickFreePort failed: " << port.Error().message() << '\n';
    return false;
  }

  alyrn::uring::detail::Server server(LoopbackAddress(*port), MakeOptions());

  auto started = server.Start();
  if (!started.HasValue()) {
    if (IsEnvironmentSkip(started.Error())) {
      std::cout << "SKIP: io_uring unavailable: " << started.Error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: Server::Start failed: " << started.Error().message() << '\n';
    return false;
  }

  auto second_start = server.Start();
  bool ok = Check(server.Started(), "server should be started") &&
            Check(!second_start.HasValue(), "second Start should fail") &&
            Check(second_start.Error().value() == EALREADY, "second Start should return EALREADY");

  server.Stop();

  return ok && Check(!server.Started(), "server should stop");
}

bool CheckServerSessionHandler() {
  auto port = PickFreePort();
  if (!port.HasValue()) {
    if (IsEnvironmentSkip(port.Error())) {
      std::cout << "SKIP: TCP bind unavailable: " << port.Error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: PickFreePort failed: " << port.Error().message() << '\n';
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
  if (!started.HasValue()) {
    if (IsEnvironmentSkip(started.Error())) {
      std::cout << "SKIP: io_uring unavailable: " << started.Error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: Server::Start failed: " << started.Error().message() << '\n';
    return false;
  }

  auto client_fd = ConnectClient(listen_addr);
  if (!client_fd.HasValue()) {
    server.Stop();
    if (IsEnvironmentSkip(client_fd.Error())) {
      std::cout << "SKIP: TCP connect unavailable: " << client_fd.Error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: client connect failed: " << client_fd.Error().message() << '\n';
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
  if (!port.HasValue()) {
    if (IsEnvironmentSkip(port.Error())) {
      std::cout << "SKIP: TCP bind unavailable: " << port.Error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: PickFreePort failed: " << port.Error().message() << '\n';
    return false;
  }

  const auto listen_addr = LoopbackAddress(*port);
  alyrn::uring::detail::Server server(listen_addr, MakeOptions());
  std::atomic_bool session_started{false};
  std::atomic_bool session_cancelled{false};

  server.SetSessionHandler([&](alyrn::uring::detail::WorkerContext&,
                               alyrn::uring::Stream stream) -> alyrn::coro::DetachedTask {
    session_started.store(true, std::memory_order_release);
    std::array<std::byte, 1> buffer{};
    auto result = co_await stream.Read(buffer);
    if (!result.HasValue() && result.Error().value() == ECANCELED) {
      session_cancelled.store(true, std::memory_order_release);
    }
  });

  auto started = server.Start();
  if (!started.HasValue()) {
    if (IsEnvironmentSkip(started.Error())) {
      std::cout << "SKIP: io_uring unavailable: " << started.Error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: Server::Start failed: " << started.Error().message() << '\n';
    return false;
  }

  auto client_fd = ConnectClient(listen_addr);
  if (!client_fd.HasValue()) {
    server.Stop();
    if (IsEnvironmentSkip(client_fd.Error())) {
      std::cout << "SKIP: TCP connect unavailable: " << client_fd.Error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: client connect failed: " << client_fd.Error().message() << '\n';
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

bool CheckServerExitFailure(int startup_failure) {
  std::atomic_size_t exited{0};
  bool fail_start = true;
  bool fail_exit = true;
  auto options = MakeOptions(2);
  options.worker_group_options.frame_resource_factory =
      [&](std::size_t index) -> std::pmr::memory_resource* {
    if (fail_start && startup_failure == 2 && index == 1) throw 42;
    return nullptr;
  };
  auto server = alyrn::uring::detail::Server(LoopbackAddress(0), options);
  server.SetThreadInitCallback([&](auto& context) {
    if (fail_start && startup_failure == 1 && context.index == 1) throw 42;
  });
  server.SetThreadExitCallback([&](auto& context) {
    ++exited;
    if (fail_exit) throw alyrn::test::contracts::ExitFailure{context.index};
  });
  bool caught = false;
  try {
    auto started = server.Start();
    if (!started.HasValue() && IsEnvironmentSkip(started.Error())) {
      std::cout << "SKIP: io_uring unavailable: " << started.Error().message() << '\n';
      return true;
    }
    if (startup_failure == 1) {
      if (!Check(!started.HasValue() && started.Error() == std::errc::bad_address,
                 "server must preserve the init failure"))
        return false;
    } else if (!Check(started.HasValue(), "server exit test must start")) {
      return false;
    }
  } catch (int value) {
    caught = value == 42;
  }
  if (!Check(caught == (startup_failure == 2), "server must preserve the factory exception")) {
    return false;
  }
  auto result = server.Stop();
  if (!Check(alyrn::test::contracts::IsExitFailure(result, 0),
             "server Stop must return the original worker exit exception") ||
      !Check(!server.Started() && exited == (startup_failure == 2 ? 1 : 2),
             "server must finish joining workers before returning an exit failure"))
    return false;
  auto repeated = server.Stop();
  if (!Check(!repeated.HasValue() && repeated.Error() == result.Error(),
             "server must retain the exit result after releasing workers"))
    return false;
  fail_start = false;
  fail_exit = false;
  if (!Check(server.Start().HasValue(), "server must restart after exit failure")) return false;
  return Check(server.Stop().HasValue(), "server restart must clear the previous exit failure");
}

}  // namespace

int main() {
  if (!CheckServerExitFailure(0)) return 1;
  if (!CheckServerExitFailure(1)) return 1;
  if (!CheckServerExitFailure(2)) return 1;
  if (!CheckServerStartStop()) return 1;
  if (!CheckServerSessionHandler()) return 1;
  if (!CheckServerStopsActiveSession()) return 1;

  std::cout << "luring server smoke: PASS\n";
  return 0;
}
