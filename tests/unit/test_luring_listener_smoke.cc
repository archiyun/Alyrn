// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <expected>
#include <iostream>
#include <optional>
#include <system_error>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/spawn.h"
#include "coropact/coro/task.h"
#include "coropact/luring/listener.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/detail/loop_access.h"
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

bool IsEnvironmentSkip(coropact::base::Error error) {
  return error == std::errc::operation_not_supported || error == std::errc::operation_not_permitted;
}

LoopInitStatus InitLoop(coropact::luring::LUringLoop& loop) {
  coropact::luring::LUringOptions options;
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

coropact::base::Result<int> ConnectClient(const coropact::net::Endpoint& address) {
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }

  int r = ::connect(fd, address.SockAddr(), address.SockAddrLen());
  if (r < 0 && errno != EINPROGRESS) {
    auto error = coropact::base::CurrentErrno();
    ::close(fd);
    return std::unexpected(error);
  }

  return fd;
}

coropact::net::Endpoint LoopbackAddress(std::uint16_t port) {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(port);
  return coropact::net::Endpoint(addr);
}

coropact::coro::DetachedTask AcceptOnce(
    coropact::luring::LUringListener* listener, coropact::luring::LUringLoop* loop,
    std::optional<coropact::base::Result<coropact::luring::LUringStream>>* out,
    bool* resumed_with_scheduler, int* resume_count = nullptr) {
  auto result = co_await listener->Accept();
  if (resume_count != nullptr) {
    ++*resume_count;
  }
  *resumed_with_scheduler = coropact::coro::Scheduler::Current() == loop;
  out->emplace(std::move(result));
}

coropact::coro::DetachedTask CloseOnce(coropact::luring::LUringListener* listener,
                                       std::optional<coropact::base::Result<void>>* out) {
  auto result = co_await listener->Close();
  out->emplace(std::move(result));
}

bool CheckAccept() {
  coropact::luring::LUringLoop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  auto listener = coropact::luring::LUringListener::Create(&loop, LoopbackAddress(0));
  if (!listener.has_value()) {
    std::cout << "FAIL: LUringListener::Create failed: " << listener.error().message() << '\n';
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

  std::optional<coropact::base::Result<coropact::luring::LUringStream>> accepted;
  bool resumed_with_scheduler = false;

  coropact::coro::SpawnDetach(loop,
                              AcceptOnce(&*listener, &loop, &accepted, &resumed_with_scheduler));

  coropact::luring::detail::LoopAccess::RunReady(loop);

  auto completions = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
  if (!completions.has_value()) {
    std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
    return false;
  }

  coropact::luring::detail::LoopAccess::RunReady(loop);

  return Check(*completions >= 1, "accept did not produce a completion") &&
         Check(accepted.has_value(), "accept coroutine did not resume") &&
         Check(accepted->has_value(), "Accept returned an error") &&
         Check(accepted->value().Fd() >= 0, "Accept returned an invalid stream") &&
         Check(resumed_with_scheduler, "accept resumed without current scheduler");
}

bool CheckCloseCancelsPendingAccept() {
  coropact::luring::LUringLoop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  auto listener = coropact::luring::LUringListener::Create(&loop, LoopbackAddress(0));
  if (!listener.has_value()) {
    std::cout << "FAIL: LUringListener::Create failed: " << listener.error().message() << '\n';
    return false;
  }

  std::optional<coropact::base::Result<coropact::luring::LUringStream>> accepted;
  bool resumed_with_scheduler = false;
  coropact::coro::SpawnDetach(loop,
                              AcceptOnce(&*listener, &loop, &accepted, &resumed_with_scheduler));

  coropact::luring::detail::LoopAccess::RunReady(loop);

  std::optional<coropact::base::Result<void>> close_result;
  coropact::coro::SpawnDetach(loop, CloseOnce(&*listener, &close_result));

  coropact::luring::detail::LoopAccess::RunReady(loop);

  if (!Check(!close_result.has_value(), "Close with pending accept should suspend")) {
    return false;
  }

  for (int i = 0; i < 4 && (!close_result.has_value() || !accepted.has_value()); ++i) {
    auto completions = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
    if (!completions.has_value()) {
      std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
      return false;
    }
    coropact::luring::detail::LoopAccess::RunReady(loop);
  }

  return Check(close_result.has_value(), "close coroutine did not resume") &&
         Check(close_result->has_value(), "Close with pending accept returned an error") &&
         Check(accepted.has_value(), "pending accept was not cleaned up") &&
         Check(!accepted->has_value(), "pending accept should be cancelled") &&
         Check(accepted->error().value() == ECANCELED, "pending accept should return ECANCELED") &&
         Check(resumed_with_scheduler, "pending accept resumed without current scheduler");
}

#if defined(COROPACT_ENABLE_TEST_HOOKS)
bool CheckAcceptSubmitFailureRollsBack() {
  coropact::luring::LUringLoop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  auto listener = coropact::luring::LUringListener::Create(&loop, LoopbackAddress(0));
  if (!listener.has_value()) {
    std::cout << "FAIL: LUringListener::Create failed: " << listener.error().message() << '\n';
    return false;
  }

  std::optional<coropact::base::Result<coropact::luring::LUringStream>> accept_result;
  bool accept_with_scheduler = false;
  int accept_resume_count = 0;
  loop.FailNextSubmissionsForTesting(1, EIO);
  coropact::coro::SpawnDetach(
      loop, AcceptOnce(&*listener, &loop, &accept_result, &accept_with_scheduler,
                       &accept_resume_count));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  if (!Check(accept_result.has_value(), "failed accept coroutine did not finish") ||
      !Check(!accept_result->has_value(), "failed accept unexpectedly succeeded") ||
      !Check(accept_result->error().value() == EIO, "failed accept returned wrong error") ||
      !Check(accept_resume_count == 1, "failed accept resumed more than once") ||
      !Check(accept_with_scheduler, "failed accept resumed without current scheduler")) {
    return false;
  }

  // A failed submission must undo pending_accepts_. Otherwise Close() would
  // submit a cancel and remain suspended for an operation that never entered
  // the ring.
  std::optional<coropact::base::Result<void>> close_result;
  coropact::coro::SpawnDetach(loop, CloseOnce(&*listener, &close_result));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  return Check(close_result.has_value(), "Close after failed accept did not finish") &&
         Check(close_result->has_value(), "Close after failed accept returned an error");
}
#endif

}  // namespace

int main() {
  if (!CheckAccept()) return 1;
  if (!CheckCloseCancelsPendingAccept()) return 1;
#if defined(COROPACT_ENABLE_TEST_HOOKS)
  if (!CheckAcceptSubmitFailureRollsBack()) return 1;
#endif

  std::cout << "luring listener smoke: PASS\n";
  return 0;
}
