// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <expected>
#include <iostream>
#include <optional>
#include <system_error>
#include <utility>

#include "alyrn/result.h"
#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/coro/task.h"
#include "alyrn/luring/detail/loop_access.h"
#include "alyrn/luring/listener.h"
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

alyrn::net::Endpoint LoopbackAddress(std::uint16_t port) {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(port);
  return alyrn::net::Endpoint(addr);
}

int GetSocketOption(int fd, int level, int option) {
  int value = 0;
  auto length = static_cast<socklen_t>(sizeof(value));
  if (::getsockopt(fd, level, option, &value, &length) < 0) {
    return -1;
  }
  return value;
}

alyrn::coro::DetachedTask AcceptOnce(
    alyrn::luring::Listener* listener, alyrn::luring::Loop* loop,
    std::optional<alyrn::Result<alyrn::luring::Stream>>* out,
    bool* resumed_with_scheduler, int* resume_count = nullptr) {
  auto result = co_await listener->Accept();
  if (resume_count != nullptr) {
    ++*resume_count;
  }
  *resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == loop;
  out->emplace(std::move(result));
}

alyrn::coro::DetachedTask AcceptThenAccept(
    alyrn::luring::Listener* listener, alyrn::luring::Loop* loop,
    std::optional<alyrn::Result<alyrn::luring::Stream>>* first,
    std::optional<alyrn::Result<alyrn::luring::Stream>>* second,
    bool* resumed_with_scheduler) {
  first->emplace(co_await listener->Accept());
  second->emplace(co_await listener->Accept());
  *resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == loop;
}

alyrn::coro::DetachedTask CloseOnce(alyrn::luring::Listener* listener,
                                       std::optional<alyrn::Result<void>>* out) {
  auto result = co_await listener->Close();
  out->emplace(std::move(result));
}

bool CheckAccept() {
  alyrn::luring::Loop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  alyrn::luring::ListenOptions options;
  options.tcp_options.no_delay = true;
  options.tcp_options.keep_alive = true;
  auto listener = alyrn::luring::Listener::Create(&loop, LoopbackAddress(0), options);
  if (!listener.has_value()) {
    std::cout << "FAIL: Listener::Create failed: " << listener.error().message() << '\n';
    return false;
  }

  auto local = listener->LocalAddress();
  if (!local.has_value()) {
    std::cout << "FAIL: LocalAddress failed: " << local.error().message() << '\n';
    return false;
  }

  auto client_fd = ConnectClient(*local);
  if (!client_fd.has_value()) {
    std::cout << "FAIL: client connect failed: " << client_fd.error().message() << '\n';
    return false;
  }
  UniqueFd client(*client_fd);

  std::optional<alyrn::Result<alyrn::luring::Stream>> accepted;
  bool resumed_with_scheduler = false;

  alyrn::coro::SpawnDetach(loop,
                              AcceptOnce(&*listener, &loop, &accepted, &resumed_with_scheduler));

  alyrn::luring::detail::LoopAccess::RunReady(loop);

  auto completions = alyrn::luring::detail::LoopAccess::WaitCompletions(loop);
  if (!completions.has_value()) {
    std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
    return false;
  }

  alyrn::luring::detail::LoopAccess::RunReady(loop);

  bool ok = Check(*completions >= 1, "accept did not produce a completion") &&
            Check(accepted.has_value(), "accept coroutine did not resume") &&
            Check(accepted->has_value(), "Accept returned an error") &&
            Check(accepted->value().Fd() >= 0, "Accept returned an invalid stream") &&
            Check(resumed_with_scheduler, "accept resumed without current scheduler");
  if (ok) {
    const int accepted_fd = accepted->value().Fd();
    ok = Check(GetSocketOption(accepted_fd, IPPROTO_TCP, TCP_NODELAY) == 1,
               "Listener did not apply TCP_NODELAY to accepted socket") &&
         Check(GetSocketOption(accepted_fd, SOL_SOCKET, SO_KEEPALIVE) == 1,
               "Listener did not apply SO_KEEPALIVE to accepted socket");
  }
  return ok;
}

bool CheckAcceptReleasesReservationBeforeContinuation() {
  alyrn::luring::Loop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  auto listener = alyrn::luring::Listener::Create(&loop, LoopbackAddress(0));
  if (!listener.has_value()) {
    std::cout << "FAIL: Listener::Create failed: " << listener.error().message() << '\n';
    return false;
  }

  auto local = listener->LocalAddress();
  if (!local.has_value()) {
    std::cout << "FAIL: LocalAddress failed: " << local.error().message() << '\n';
    return false;
  }

  auto first_client_fd = ConnectClient(*local);
  if (!first_client_fd.has_value()) {
    std::cout << "FAIL: first client connect failed: " << first_client_fd.error().message() << '\n';
    return false;
  }
  UniqueFd first_client(*first_client_fd);

  auto second_client_fd = ConnectClient(*local);
  if (!second_client_fd.has_value()) {
    std::cout << "FAIL: second client connect failed: " << second_client_fd.error().message()
              << '\n';
    return false;
  }
  UniqueFd second_client(*second_client_fd);

  std::optional<alyrn::Result<alyrn::luring::Stream>> first;
  std::optional<alyrn::Result<alyrn::luring::Stream>> second;
  bool resumed_with_scheduler = false;
  alyrn::coro::SpawnDetach(
      loop, AcceptThenAccept(&*listener, &loop, &first, &second, &resumed_with_scheduler));
  alyrn::luring::detail::LoopAccess::RunReady(loop);

  for (int i = 0; i < 8 && !second.has_value(); ++i) {
    auto completions = alyrn::luring::detail::LoopAccess::WaitCompletions(loop);
    if (!completions.has_value()) {
      std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
      return false;
    }
    alyrn::luring::detail::LoopAccess::RunReady(loop);
  }

  return Check(first.has_value(), "first accept did not finish") &&
         Check(first->has_value(), "first accept returned an error") &&
         Check(first->value().Fd() >= 0, "first accept returned an invalid stream") &&
         Check(second.has_value(), "follow-up accept did not finish") &&
         Check(second->has_value(),
               "single-shot accept left its listener reservation active during continuation") &&
         Check(second->value().Fd() >= 0, "follow-up accept returned an invalid stream") &&
         Check(resumed_with_scheduler,
               "single-shot follow-up accept resumed without current scheduler");
}

bool CheckCloseCancelsPendingAccept() {
  alyrn::luring::Loop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  auto listener = alyrn::luring::Listener::Create(&loop, LoopbackAddress(0));
  if (!listener.has_value()) {
    std::cout << "FAIL: Listener::Create failed: " << listener.error().message() << '\n';
    return false;
  }

  std::optional<alyrn::Result<alyrn::luring::Stream>> accepted;
  bool resumed_with_scheduler = false;
  alyrn::coro::SpawnDetach(loop,
                              AcceptOnce(&*listener, &loop, &accepted, &resumed_with_scheduler));

  alyrn::luring::detail::LoopAccess::RunReady(loop);

  std::optional<alyrn::Result<void>> close_result;
  alyrn::coro::SpawnDetach(loop, CloseOnce(&*listener, &close_result));

  alyrn::luring::detail::LoopAccess::RunReady(loop);

  if (!Check(!close_result.has_value(), "Close with pending accept should suspend")) {
    return false;
  }

  for (int i = 0; i < 4 && (!close_result.has_value() || !accepted.has_value()); ++i) {
    auto completions = alyrn::luring::detail::LoopAccess::WaitCompletions(loop);
    if (!completions.has_value()) {
      std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
      return false;
    }
    alyrn::luring::detail::LoopAccess::RunReady(loop);
  }

  return Check(close_result.has_value(), "close coroutine did not resume") &&
         Check(close_result->has_value(), "Close with pending accept returned an error") &&
         Check(accepted.has_value(), "pending accept was not cleaned up") &&
         Check(!accepted->has_value(), "pending accept should be cancelled") &&
         Check(accepted->error().value() == ECANCELED, "pending accept should return ECANCELED") &&
         Check(resumed_with_scheduler, "pending accept resumed without current scheduler");
}


}  // namespace

int main() {
  if (!CheckAccept()) return 1;
  if (!CheckAcceptReleasesReservationBeforeContinuation()) return 1;
  if (!CheckCloseCancelsPendingAccept()) return 1;

  std::cout << "luring listener smoke: PASS\n";
  return 0;
}
