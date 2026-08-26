// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <expected>
#include <iostream>
#include <optional>
#include <string_view>
#include <system_error>
#include <utility>

#include "coropact/result.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/spawn.h"
#include "coropact/coro/task.h"
#include "coropact/luring/connector.h"
#include "coropact/luring/detail/loop_access.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/options.h"
#include "coropact/luring/stream.h"
#include "coropact/net/endpoint.h"

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

bool IsEnvironmentSkip(coropact::Error error) {
  return error == std::errc::operation_not_supported || error == std::errc::operation_not_permitted;
}

LoopInitStatus InitLoop(coropact::luring::Loop& loop) {
  coropact::luring::Options options;
  options.entries = 16;

  auto init = loop.Init(options);
  if (init.has_value()) {
    return LoopInitStatus::kReady;
  }
  if (IsEnvironmentSkip(init.error())) {
    std::cout << "SKIP: io_uring unavailable: " << init.error().message() << '\n';
    return LoopInitStatus::kSkip;
  }

  std::cout << "FAIL: Loop init failed: " << init.error().message() << '\n';
  return LoopInitStatus::kFail;
}

coropact::Result<ListenEndpoint> ListenLoopback() {
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    return std::unexpected(coropact::CurrentErrno());
  }

  auto fail = [fd](coropact::Error error) -> coropact::Result<ListenEndpoint> {
    ::close(fd);
    return std::unexpected(error);
  };

  int on = 1;
  if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0) {
    return fail(coropact::CurrentErrno());
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(0);

  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    return fail(coropact::CurrentErrno());
  }

  if (::listen(fd, SOMAXCONN) < 0) {
    return fail(coropact::CurrentErrno());
  }

  socklen_t len = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) < 0) {
    return fail(coropact::CurrentErrno());
  }

  return ListenEndpoint{.fd = UniqueFd(fd), .port = ntohs(addr.sin_port)};
}

coropact::coro::DetachedTask ConnectOnce(
    coropact::luring::Connector* connector, coropact::luring::Loop* loop,
    std::string_view host, std::uint16_t port,
    std::optional<coropact::Result<coropact::luring::Stream>>* out,
    bool* resumed_with_scheduler) {
  auto connected = co_await connector->Connect(host, port);
  *resumed_with_scheduler = coropact::coro::Scheduler::TryCurrent() == loop;
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

  coropact::luring::Loop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  coropact::luring::Connector connector(&loop);
  std::optional<coropact::Result<coropact::luring::Stream>> connected;
  bool resumed_with_scheduler = false;

  coropact::coro::SpawnDetach(loop, ConnectOnce(&connector, &loop, "127.0.0.1", listener->port,
                                                &connected, &resumed_with_scheduler));

  coropact::luring::detail::LoopAccess::RunReady(loop);

  auto completions = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
  if (!completions.has_value()) {
    std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
    return false;
  }

  coropact::luring::detail::LoopAccess::RunReady(loop);

  return Check(*completions >= 1, "connect did not produce a completion") &&
         Check(connected.has_value(), "connect coroutine did not resume") &&
         Check(connected->has_value(), "Connect returned an error") &&
         Check(connected->value().Fd() >= 0, "Connect returned an invalid stream") &&
         Check(resumed_with_scheduler, "connect resumed without current scheduler");
}

bool CheckConnectRejectsInvalidHost() {
  coropact::luring::Loop loop;
  coropact::luring::Connector connector(&loop);

  std::optional<coropact::Result<coropact::luring::Stream>> connected;
  bool resumed_with_scheduler = false;

  coropact::coro::SpawnDetach(
      loop, ConnectOnce(&connector, &loop, "not-an-ip", 80, &connected, &resumed_with_scheduler));

  coropact::luring::detail::LoopAccess::RunReady(loop);

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
