// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
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

#include "alyrn/result.h"
#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/coro/task.h"
#include "alyrn/luring/connector.h"
#include "alyrn/luring/detail/loop_access.h"
#include "alyrn/luring/loop.h"
#include "alyrn/luring/options.h"
#include "alyrn/luring/stream.h"
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

bool ReadSocketOption(int fd, int level, int option, int* value) {
  socklen_t length = sizeof(*value);
  return ::getsockopt(fd, level, option, value, &length) == 0;
}

bool IsEnvironmentSkip(alyrn::Error error) {
  return error == std::errc::operation_not_supported || error == std::errc::operation_not_permitted;
}

LoopInitStatus InitLoop(alyrn::luring::Loop& loop) {
  alyrn::luring::Options options;
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

alyrn::Result<ListenEndpoint> ListenLoopback() {
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    return std::unexpected(alyrn::CurrentErrno());
  }

  auto fail = [fd](alyrn::Error error) -> alyrn::Result<ListenEndpoint> {
    ::close(fd);
    return std::unexpected(error);
  };

  int on = 1;
  if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0) {
    return fail(alyrn::CurrentErrno());
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(0);

  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    return fail(alyrn::CurrentErrno());
  }

  if (::listen(fd, SOMAXCONN) < 0) {
    return fail(alyrn::CurrentErrno());
  }

  socklen_t len = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) < 0) {
    return fail(alyrn::CurrentErrno());
  }

  return ListenEndpoint{.fd = UniqueFd(fd), .port = ntohs(addr.sin_port)};
}

alyrn::coro::DetachedTask ConnectOnce(
    alyrn::luring::Connector* connector, alyrn::luring::Loop* loop,
    std::string_view host, std::uint16_t port,
    std::optional<alyrn::Result<alyrn::luring::Stream>>* out,
    bool* resumed_with_scheduler) {
  auto connected = co_await connector->Connect(host, port);
  *resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == loop;
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

  alyrn::luring::Loop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  alyrn::luring::ConnectorOptions connector_options;
  connector_options.tcp_options.no_delay = true;
  connector_options.tcp_options.keep_alive = true;
  connector_options.tcp_options.read_buffer = 64 * 1024;
  connector_options.tcp_options.write_buffer = 64 * 1024;
  alyrn::luring::Connector connector(&loop, connector_options);
  std::optional<alyrn::Result<alyrn::luring::Stream>> connected;
  bool resumed_with_scheduler = false;

  alyrn::coro::SpawnDetach(loop, ConnectOnce(&connector, &loop, "127.0.0.1", listener->port,
                                                &connected, &resumed_with_scheduler));

  alyrn::luring::detail::LoopAccess::RunReady(loop);

  auto completions = alyrn::luring::detail::LoopAccess::WaitCompletions(loop);
  if (!completions.has_value()) {
    std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
    return false;
  }

  alyrn::luring::detail::LoopAccess::RunReady(loop);

  if (!Check(*completions >= 1, "connect did not produce a completion") ||
      !Check(connected.has_value(), "connect coroutine did not resume") ||
      !Check(connected->has_value(), "Connect returned an error") ||
      !Check(connected->value().Fd() >= 0, "Connect returned an invalid stream") ||
      !Check(resumed_with_scheduler, "connect resumed without current scheduler")) {
    return false;
  }

  const int fd = connected->value().Fd();
  int no_delay = 0;
  int keep_alive = 0;
  int read_buffer = 0;
  int write_buffer = 0;
  return Check(ReadSocketOption(fd, IPPROTO_TCP, TCP_NODELAY, &no_delay),
               "could not read TCP_NODELAY") &&
         Check(no_delay == 1, "Connector did not apply TCP_NODELAY") &&
         Check(ReadSocketOption(fd, SOL_SOCKET, SO_KEEPALIVE, &keep_alive),
               "could not read SO_KEEPALIVE") &&
         Check(keep_alive == 1, "Connector did not apply SO_KEEPALIVE") &&
         Check(ReadSocketOption(fd, SOL_SOCKET, SO_RCVBUF, &read_buffer),
               "could not read SO_RCVBUF") &&
         Check(read_buffer > 0, "Connector produced an invalid receive buffer") &&
         Check(ReadSocketOption(fd, SOL_SOCKET, SO_SNDBUF, &write_buffer),
               "could not read SO_SNDBUF") &&
         Check(write_buffer > 0, "Connector produced an invalid send buffer");
}

bool CheckConnectRejectsInvalidHost() {
  alyrn::luring::Loop loop;
  alyrn::luring::Connector connector(&loop);

  std::optional<alyrn::Result<alyrn::luring::Stream>> connected;
  bool resumed_with_scheduler = false;

  alyrn::coro::SpawnDetach(
      loop, ConnectOnce(&connector, &loop, "not-an-ip", 80, &connected, &resumed_with_scheduler));

  alyrn::luring::detail::LoopAccess::RunReady(loop);

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
