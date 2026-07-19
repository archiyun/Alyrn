// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <expected>
#include <iostream>
#include <memory>
#include <optional>
#include <string_view>
#include <system_error>
#include <utility>

#include "vexo/base/error.h"
#include "vexo/coro/scheduler.h"
#include "vexo/coro/spawn.h"
#include "vexo/coro/task.h"
#include "vexo/luring/connector.h"
#include "vexo/luring/loop.h"
#include "vexo/luring/options.h"
#include "vexo/luring/stream.h"
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

  int fd() const noexcept { return fd_; }

  void Reset(int fd = -1) noexcept {
    if (fd_ >= 0) {
      ::close(fd_);
    }
    fd_ = fd;
  }

private:
  int fd_{-1};
};

struct ListenEndpoint {
  UniqueFd fd;
  std::uint16_t port{0};
};

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

bool IsEnvironmentSkip(vexo::base::Error error) {
  return error == std::errc::operation_not_supported || error == std::errc::operation_not_permitted;
}

LoopInitStatus InitLoop(vexo::luring::LUringLoop& loop) {
  vexo::luring::LUringOptions options;
  options.entries = 16;
  options.submit_batch = 1;

  auto init = loop.Init(options);
  if (init.has_value()) {
    return LoopInitStatus::kReady;
  }
  if (IsEnvironmentSkip(init.error())) {
    std::cout << "SKIP: io_uring unavailable: " << init.error().message() << '\n';
    return LoopInitStatus::kSkip;
  }

  std::cout << "FAIL: LUringLoop init failed: " << init.error().message() << '\n';
  return LoopInitStatus::kFail;
}

vexo::base::Result<ListenEndpoint> ListenLoopback() {
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    return std::unexpected(vexo::base::CurrentErrno());
  }

  auto fail = [fd](vexo::base::Error error) -> vexo::base::Result<ListenEndpoint> {
    ::close(fd);
    return std::unexpected(error);
  };

  int on = 1;
  if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0) {
    return fail(vexo::base::CurrentErrno());
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(0);

  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    return fail(vexo::base::CurrentErrno());
  }

  if (::listen(fd, SOMAXCONN) < 0) {
    return fail(vexo::base::CurrentErrno());
  }

  socklen_t len = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) < 0) {
    return fail(vexo::base::CurrentErrno());
  }

  return ListenEndpoint{.fd = UniqueFd(fd), .port = ntohs(addr.sin_port)};
}

vexo::coro::Task<void> ConnectOnce(
    vexo::luring::LUringConnector* connector, vexo::luring::LUringLoop* loop, std::string_view host,
    std::uint16_t port,
    std::optional<vexo::base::Result<std::unique_ptr<vexo::luring::LUringStream>>>* out,
    bool* resumed_with_scheduler) {
  auto connected = co_await connector->Connect(host, port);
  *resumed_with_scheduler = vexo::coro::Scheduler::Current() == loop;
  out->emplace(std::move(connected));
}

bool CheckConnectSuccess() {
  auto listener = ListenLoopback();
  if (!listener.has_value()) {
    if (IsEnvironmentSkip(listener.error())) {
      std::cout << "SKIP: TCP listen unavailable: " << listener.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: listen failed: " << listener.error().message() << '\n';
    return false;
  }

  vexo::luring::LUringLoop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  vexo::luring::LUringConnector connector(&loop);
  std::optional<vexo::base::Result<std::unique_ptr<vexo::luring::LUringStream>>> connected;
  bool resumed_with_scheduler = false;

  vexo::coro::Spawn(loop, ConnectOnce(&connector, &loop, "127.0.0.1", listener->port, &connected,
                                      &resumed_with_scheduler))
      .Detach();

  loop.RunReady();

  auto completions = loop.WaitCompletions();
  if (!completions.has_value()) {
    std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
    return false;
  }

  loop.RunReady();

  return Check(*completions >= 1, "connect did not produce a completion") &&
         Check(connected.has_value(), "connect coroutine did not resume") &&
         Check(connected->has_value(), "Connect returned an error") &&
         Check(connected->value() != nullptr, "Connect returned nullptr stream") &&
         Check(resumed_with_scheduler, "connect resumed without current scheduler");
}

bool CheckConnectRejectsInvalidHost() {
  vexo::luring::LUringLoop loop;
  vexo::luring::LUringConnector connector(&loop);

  std::optional<vexo::base::Result<std::unique_ptr<vexo::luring::LUringStream>>> connected;
  bool resumed_with_scheduler = false;

  vexo::coro::Spawn(
      loop, ConnectOnce(&connector, &loop, "not-an-ip", 80, &connected, &resumed_with_scheduler))
      .Detach();

  loop.RunReady();

  return Check(connected.has_value(), "invalid host connect did not finish immediately") &&
         Check(!connected->has_value(), "invalid host connect unexpectedly succeeded") &&
         Check(connected->error().value() == EINVAL, "invalid host should return EINVAL") &&
         Check(resumed_with_scheduler, "invalid host connect resumed without current scheduler");
}

}  // namespace

int main() {
  if (!CheckConnectSuccess()) return 1;
  if (!CheckConnectRejectsInvalidHost()) return 1;

  std::cout << "luring connector smoke: PASS\n";
  return 0;
}
