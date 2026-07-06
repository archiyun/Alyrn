// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <expected>
#include <iostream>
#include <optional>
#include <span>
#include <string_view>
#include <system_error>
#include <utility>

#include "vexo/base/error.h"
#include "vexo/coro/scheduler.h"
#include "vexo/coro/spawn.h"
#include "vexo/coro/task.h"
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
  int Release() noexcept { return std::exchange(fd_, -1); }

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

bool CreateSocketPair(UniqueFd& lhs, UniqueFd& rhs) {
  int fds[2] = {-1, -1};
  if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) < 0) {
    std::cout << "FAIL: socketpair failed: " << errno << '\n';
    return false;
  }

  lhs.Reset(fds[0]);
  rhs.Reset(fds[1]);
  return true;
}

vexo::net::InetAddress EmptyPeerAddress() {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  return vexo::net::InetAddress(addr);
}

bool WriteFd(int fd, std::string_view bytes) {
  while (!bytes.empty()) {
    ssize_t n = ::write(fd, bytes.data(), bytes.size());
    if (n > 0) {
      bytes.remove_prefix(static_cast<std::size_t>(n));
      continue;
    }
    if (n < 0 && errno == EINTR) {
      continue;
    }
    std::cout << "FAIL: write failed: " << errno << '\n';
    return false;
  }
  return true;
}

vexo::coro::Task<void> ReadOnce(vexo::luring::LUringStream* stream,
                                vexo::luring::LUringLoop* loop,
                                std::span<std::byte> buffer,
                                std::optional<vexo::base::Result<std::size_t>>* out,
                                bool* resumed_with_scheduler) {
  auto result = co_await stream->ReadSome(buffer);
  *resumed_with_scheduler = vexo::coro::Scheduler::Current() == loop;
  out->emplace(std::move(result));
}

vexo::coro::Task<void> WriteOnce(vexo::luring::LUringStream* stream,
                                 vexo::luring::LUringLoop* loop,
                                 std::span<const std::byte> buffer,
                                 std::optional<vexo::base::Result<std::size_t>>* out,
                                 bool* resumed_with_scheduler) {
  auto result = co_await stream->WriteSome(buffer);
  *resumed_with_scheduler = vexo::coro::Scheduler::Current() == loop;
  out->emplace(std::move(result));
}

vexo::coro::Task<void> CloseOnce(vexo::luring::LUringStream* stream,
                                 std::optional<vexo::base::Result<void>>* out) {
  auto result = co_await stream->Close();
  out->emplace(std::move(result));
}

bool CheckReadSome() {
  vexo::luring::LUringLoop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  UniqueFd local;
  UniqueFd peer;
  if (!CreateSocketPair(local, peer)) return false;

  vexo::luring::LUringStream stream(&loop, local.Release(), EmptyPeerAddress());

  constexpr std::string_view kPayload = "hello";
  if (!WriteFd(peer.fd(), kPayload)) return false;

  std::array<std::byte, 16> buffer{};
  std::optional<vexo::base::Result<std::size_t>> result;
  bool resumed_with_scheduler = false;

  vexo::coro::Spawn(loop, ReadOnce(&stream, &loop, buffer, &result, &resumed_with_scheduler))
      .Detach();

  loop.RunReady();

  auto completions = loop.WaitCompletions();
  if (!completions.has_value()) {
    std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
    return false;
  }

  loop.RunReady();

  std::string_view actual(reinterpret_cast<const char*>(buffer.data()), kPayload.size());

  return Check(*completions >= 1, "read did not produce a completion") &&
         Check(result.has_value(), "read coroutine did not resume") &&
         Check(result->has_value(), "ReadSome returned an error") &&
         Check(**result == kPayload.size(), "ReadSome returned wrong byte count") &&
         Check(actual == kPayload, "ReadSome payload mismatch") &&
         Check(resumed_with_scheduler, "read resumed without current scheduler");
}

bool CheckWriteSome() {
  vexo::luring::LUringLoop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  UniqueFd local;
  UniqueFd peer;
  if (!CreateSocketPair(local, peer)) return false;

  vexo::luring::LUringStream stream(&loop, local.Release(), EmptyPeerAddress());

  constexpr std::string_view kPayload = "pong";
  auto bytes = std::as_bytes(std::span<const char>(kPayload.data(), kPayload.size()));

  std::optional<vexo::base::Result<std::size_t>> result;
  bool resumed_with_scheduler = false;

  vexo::coro::Spawn(loop, WriteOnce(&stream, &loop, bytes, &result, &resumed_with_scheduler))
      .Detach();

  loop.RunReady();

  auto completions = loop.WaitCompletions();
  if (!completions.has_value()) {
    std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
    return false;
  }

  loop.RunReady();

  std::array<char, 16> read_buffer{};
  ssize_t n = ::read(peer.fd(), read_buffer.data(), read_buffer.size());
  if (n < 0) {
    std::cout << "FAIL: peer read failed: " << errno << '\n';
    return false;
  }

  std::string_view actual(read_buffer.data(), static_cast<std::size_t>(n));

  return Check(*completions >= 1, "write did not produce a completion") &&
         Check(result.has_value(), "write coroutine did not resume") &&
         Check(result->has_value(), "WriteSome returned an error") &&
         Check(**result == kPayload.size(), "WriteSome returned wrong byte count") &&
         Check(actual == kPayload, "WriteSome payload mismatch") &&
         Check(resumed_with_scheduler, "write resumed without current scheduler");
}

bool CheckCloseWithoutPending() {
  vexo::luring::LUringLoop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  UniqueFd local;
  UniqueFd peer;
  if (!CreateSocketPair(local, peer)) return false;

  vexo::luring::LUringStream stream(&loop, local.Release(), EmptyPeerAddress());

  std::optional<vexo::base::Result<void>> result;
  vexo::coro::Spawn(loop, CloseOnce(&stream, &result)).Detach();

  loop.RunReady();

  return Check(result.has_value(), "close coroutine did not run") &&
         Check(result->has_value(), "Close without pending op returned an error");
}

bool CheckCloseCancelsPendingRead() {
  vexo::luring::LUringLoop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  UniqueFd local;
  UniqueFd peer;
  if (!CreateSocketPair(local, peer)) return false;

  vexo::luring::LUringStream stream(&loop, local.Release(), EmptyPeerAddress());

  std::array<std::byte, 8> buffer{};
  std::optional<vexo::base::Result<std::size_t>> read_result;
  bool read_resumed_with_scheduler = false;

  vexo::coro::Spawn(loop,
                    ReadOnce(&stream, &loop, buffer, &read_result, &read_resumed_with_scheduler))
      .Detach();

  loop.RunReady();

  std::optional<vexo::base::Result<void>> close_result;
  vexo::coro::Spawn(loop, CloseOnce(&stream, &close_result)).Detach();

  loop.RunReady();

  if (!Check(!close_result.has_value(), "Close with pending read should suspend")) {
    return false;
  }

  for (int i = 0; i < 4 && (!close_result.has_value() || !read_result.has_value()); ++i) {
    auto completions = loop.WaitCompletions();
    if (!completions.has_value()) {
      std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
      return false;
    }
    loop.RunReady();
  }

  return Check(close_result.has_value(), "busy close coroutine did not run") &&
         Check(close_result->has_value(), "Close with pending read returned an error") &&
         Check(read_result.has_value(), "pending read was not cleaned up") &&
         Check(!read_result->has_value(), "pending read should be cancelled") &&
         Check(read_result->error().value() == ECANCELED, "pending read should return ECANCELED") &&
         Check(read_resumed_with_scheduler, "pending read resumed without current scheduler");
}

}  // namespace

int main() {
  if (!CheckReadSome()) return 1;
  if (!CheckWriteSome()) return 1;
  if (!CheckCloseWithoutPending()) return 1;
  if (!CheckCloseCancelsPendingRead()) return 1;

  std::cout << "luring stream smoke: PASS\n";
  return 0;
}
