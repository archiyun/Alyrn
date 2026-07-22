// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <expected>
#include <iostream>
#include <system_error>
#include <thread>
#include <utility>

#include "vexo/base/error.h"
#include "vexo/luring/worker_group.h"
#include "vexo/net/inet_address.h"

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

bool IsEnvironmentSkip(vexo::base::Error error) {
  return error == std::errc::operation_not_supported || error == std::errc::operation_not_permitted;
}

vexo::net::InetAddress LoopbackAddress(std::uint16_t port) {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(port);
  return vexo::net::InetAddress(addr);
}

vexo::base::Result<std::uint16_t> PickFreePort() {
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    return std::unexpected(vexo::base::CurrentErrno());
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(0);

  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    auto error = vexo::base::CurrentErrno();
    ::close(fd);
    return std::unexpected(error);
  }

  socklen_t len = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) < 0) {
    auto error = vexo::base::CurrentErrno();
    ::close(fd);
    return std::unexpected(error);
  }

  ::close(fd);
  return ntohs(addr.sin_port);
}

vexo::base::Result<int> ConnectClient(const vexo::net::InetAddress& address) {
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    return std::unexpected(vexo::base::CurrentErrno());
  }

  const sockaddr_in& addr = address.sock_addr();
  int r = ::connect(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr));
  if (r < 0 && errno != EINPROGRESS) {
    auto error = vexo::base::CurrentErrno();
    ::close(fd);
    return std::unexpected(error);
  }

  return fd;
}

bool CheckWorkerGroupStartStop() {
  auto port = PickFreePort();
  if (!port.has_value()) {
    if (IsEnvironmentSkip(port.error())) {
      std::cout << "SKIP: TCP bind unavailable: " << port.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: PickFreePort failed: " << port.error().message() << '\n';
    return false;
  }

  vexo::luring::LUringWorkerGroupOptions options;
  options.worker_num = 2;
  options.worker_options.loop_options.entries = 16;
  options.worker_options.loop_options.submit_batch = 1;
  options.worker_options.listen_options.reuse_port = true;

  std::atomic_size_t init_count{0};
  std::atomic_bool bad_thread{false};
  std::atomic_bool invalid_index{false};
  std::atomic_bool invalid_listener{false};

  vexo::luring::LUringWorkerGroup group(LoopbackAddress(*port), options,
                                        [&](vexo::luring::LUringWorkerContext& context) {
                                          if (!context.loop.IsInLoopThread()) {
                                            bad_thread.store(true, std::memory_order_relaxed);
                                          }
                                          if (context.index >= 2) {
                                            invalid_index.store(true, std::memory_order_relaxed);
                                          }
                                          if (context.listener.fd() < 0) {
                                            invalid_listener.store(true, std::memory_order_relaxed);
                                          }
                                          init_count.fetch_add(1, std::memory_order_relaxed);
                                        });

  auto started = group.Start();
  if (!started.has_value()) {
    if (IsEnvironmentSkip(started.error())) {
      std::cout << "SKIP: io_uring unavailable: " << started.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: LUringWorkerGroup::Start failed: " << started.error().message() << '\n';
    return false;
  }

  bool ok = Check(group.started(), "group should be started") &&
            Check(group.size() == 2, "group size should be 2") &&
            Check(init_count.load(std::memory_order_relaxed) == 2,
                  "init callback should run for each worker") &&
            Check(!bad_thread.load(std::memory_order_relaxed),
                  "init callback should run in loop thread") &&
            Check(!invalid_index.load(std::memory_order_relaxed),
                  "init callback received an invalid worker index") &&
            Check(!invalid_listener.load(std::memory_order_relaxed),
                  "init callback received an invalid listener");

  group.Stop();

  return ok && Check(!group.started(), "group should stop") &&
         Check(group.size() == 0, "workers should be cleared after stop");
}

bool CheckWorkerGroupAcceptCallback() {
  auto port = PickFreePort();
  if (!port.has_value()) {
    if (IsEnvironmentSkip(port.error())) {
      std::cout << "SKIP: TCP bind unavailable: " << port.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: PickFreePort failed: " << port.error().message() << '\n';
    return false;
  }

  vexo::luring::LUringWorkerGroupOptions options;
  options.worker_num = 1;
  options.worker_options.loop_options.entries = 16;
  options.worker_options.loop_options.submit_batch = 1;
  options.worker_options.listen_options.reuse_port = true;

  std::atomic_size_t connection_count{0};
  std::atomic_bool invalid_stream{false};
  std::atomic_bool bad_thread{false};

  const auto listen_addr = LoopbackAddress(*port);
  vexo::luring::LUringWorkerGroup group(
      listen_addr, options, {},
      [&](vexo::luring::LUringWorkerContext& context,
          vexo::luring::LUringStream stream) -> vexo::coro::Task<void> {
        if (!context.loop.IsInLoopThread()) {
          bad_thread.store(true, std::memory_order_relaxed);
        }
        if (stream.fd() < 0) {
          invalid_stream.store(true, std::memory_order_relaxed);
        }
        connection_count.fetch_add(1, std::memory_order_relaxed);
        co_return;
      });

  auto started = group.Start();
  if (!started.has_value()) {
    if (IsEnvironmentSkip(started.error())) {
      std::cout << "SKIP: io_uring unavailable: " << started.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: LUringWorkerGroup::Start failed: " << started.error().message() << '\n';
    return false;
  }

  auto client_fd = ConnectClient(listen_addr);
  if (!client_fd.has_value()) {
    group.Stop();
    if (IsEnvironmentSkip(client_fd.error())) {
      std::cout << "SKIP: TCP connect unavailable: " << client_fd.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: client connect failed: " << client_fd.error().message() << '\n';
    return false;
  }
  UniqueFd client(*client_fd);

  for (int i = 0; i < 200 && connection_count.load(std::memory_order_relaxed) == 0; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }

  bool ok = Check(connection_count.load(std::memory_order_relaxed) == 1,
                  "connection callback should run once") &&
            Check(!bad_thread.load(std::memory_order_relaxed),
                  "connection callback should run in loop thread") &&
            Check(!invalid_stream.load(std::memory_order_relaxed),
                  "connection callback received an invalid stream");

  group.Stop();

  return ok && Check(!group.started(), "group should stop after accept callback test") &&
         Check(group.size() == 0, "workers should be cleared after accept callback test");
}

}  // namespace

int main() {
  if (!CheckWorkerGroupStartStop()) return 1;
  if (!CheckWorkerGroupAcceptCallback()) return 1;

  std::cout << "luring worker group smoke: PASS\n";
  return 0;
}
