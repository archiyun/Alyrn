// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <expected>
#include <iostream>
#include <optional>
#include <span>
#include <string_view>
#include <system_error>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/spawn.h"
#include "coropact/coro/task.h"
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

coropact::net::Endpoint EmptyPeerAddress() {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  return coropact::net::Endpoint(addr);
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

coropact::coro::DetachedTask ReadOnce(coropact::luring::LUringStream* stream,
                                      coropact::luring::LUringLoop* loop,
                                      std::span<std::byte> buffer,
                                      std::optional<coropact::base::Result<std::size_t>>* out,
                                      bool* resumed_with_scheduler, int* resume_count = nullptr) {
  auto result = co_await stream->ReadSome(buffer);
  if (resume_count != nullptr) {
    ++*resume_count;
  }
  *resumed_with_scheduler = coropact::coro::Scheduler::Current() == loop;
  out->emplace(std::move(result));
}

coropact::coro::DetachedTask ReadForOnce(coropact::luring::LUringStream* stream,
                                         coropact::luring::LUringLoop* loop,
                                         std::span<std::byte> buffer,
                                         std::chrono::milliseconds timeout,
                                         std::optional<coropact::base::Result<std::size_t>>* out,
                                         bool* resumed_with_scheduler, int* resume_count) {
  auto result = co_await stream->ReadSomeFor(buffer, timeout);
  ++*resume_count;
  *resumed_with_scheduler = coropact::coro::Scheduler::Current() == loop;
  out->emplace(std::move(result));
}

coropact::coro::DetachedTask WriteOnce(coropact::luring::LUringStream* stream,
                                       coropact::luring::LUringLoop* loop,
                                       std::span<const std::byte> buffer,
                                       std::optional<coropact::base::Result<std::size_t>>* out,
                                       bool* resumed_with_scheduler,
                                       int* resume_count = nullptr) {
  auto result = co_await stream->WriteSome(buffer);
  if (resume_count != nullptr) {
    ++*resume_count;
  }
  *resumed_with_scheduler = coropact::coro::Scheduler::Current() == loop;
  out->emplace(std::move(result));
}

coropact::coro::DetachedTask CloseOnce(coropact::luring::LUringStream* stream,
                                       std::optional<coropact::base::Result<void>>* out) {
  auto result = co_await stream->Close();
  out->emplace(std::move(result));
}

bool CheckReadSome() {
  coropact::luring::LUringLoop loop;
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

  coropact::luring::LUringStream stream(&loop, local.Release(), EmptyPeerAddress());

  constexpr std::string_view kPayload = "hello";
  if (!WriteFd(peer.fd(), kPayload)) return false;

  std::array<std::byte, 16> buffer{};
  std::optional<coropact::base::Result<std::size_t>> result;
  bool resumed_with_scheduler = false;

  coropact::coro::SpawnDetach(loop,
                              ReadOnce(&stream, &loop, buffer, &result, &resumed_with_scheduler));

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

bool CheckTimedReadSuccessResumesOnce() {
  coropact::luring::LUringLoop loop;
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

  coropact::luring::LUringStream stream(&loop, local.Release(), EmptyPeerAddress());
  constexpr std::string_view kPayload = "timed";
  if (!WriteFd(peer.fd(), kPayload)) return false;

  std::array<std::byte, 16> buffer{};
  std::optional<coropact::base::Result<std::size_t>> result;
  bool resumed_with_scheduler = false;
  int resume_count = 0;
  coropact::coro::SpawnDetach(loop, ReadForOnce(&stream, &loop, buffer, std::chrono::seconds(1),
                                                &result, &resumed_with_scheduler, &resume_count));
  loop.RunReady();

  for (int i = 0; i < 4 && !result.has_value(); ++i) {
    auto completions = loop.WaitCompletions();
    if (!completions.has_value()) {
      std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
      return false;
    }
    loop.RunReady();
  }

  std::string_view actual(reinterpret_cast<const char*>(buffer.data()), kPayload.size());
  return Check(result.has_value(), "timed read success coroutine did not resume") &&
         Check(result->has_value(), "timed read success returned an error") &&
         Check(**result == kPayload.size(), "timed read success returned wrong byte count") &&
         Check(actual == kPayload, "timed read success payload mismatch") &&
         Check(resume_count == 1, "timed read success resumed more than once") &&
         Check(resumed_with_scheduler, "timed read success resumed without current scheduler");
}

bool CheckTimedReadTimeoutResumesOnce() {
  coropact::luring::LUringLoop loop;
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

  coropact::luring::LUringStream stream(&loop, local.Release(), EmptyPeerAddress());
  std::array<std::byte, 16> buffer{};
  std::optional<coropact::base::Result<std::size_t>> result;
  bool resumed_with_scheduler = false;
  int resume_count = 0;
  coropact::coro::SpawnDetach(
      loop, ReadForOnce(&stream, &loop, buffer, std::chrono::milliseconds(1), &result,
                        &resumed_with_scheduler, &resume_count));
  loop.RunReady();

  for (int i = 0; i < 4 && !result.has_value(); ++i) {
    auto completions = loop.WaitCompletions();
    if (!completions.has_value()) {
      std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
      return false;
    }
    loop.RunReady();
  }

  return Check(result.has_value(), "timed read timeout coroutine did not resume") &&
         Check(!result->has_value(), "timed read timeout unexpectedly succeeded") &&
         Check(result->error().value() == ETIMEDOUT, "timed read did not return ETIMEDOUT") &&
         Check(resume_count == 1, "timed read timeout resumed more than once") &&
         Check(resumed_with_scheduler, "timed read timeout resumed without current scheduler");
}

#if defined(COROPACT_ENABLE_TEST_HOOKS)
bool CheckReadSubmitFailureRollsBack() {
  coropact::luring::LUringLoop loop;
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

  coropact::luring::LUringStream stream(&loop, local.Release(), EmptyPeerAddress());
  std::array<std::byte, 16> buffer{};
  std::optional<coropact::base::Result<std::size_t>> failed_result;
  bool failed_with_scheduler = false;
  int failed_resume_count = 0;

  loop.FailNextSubmissionsForTesting(1, EIO);
  coropact::coro::SpawnDetach(
      loop, ReadOnce(&stream, &loop, buffer, &failed_result, &failed_with_scheduler,
                     &failed_resume_count));
  loop.RunReady();

  if (!Check(failed_result.has_value(), "failed read coroutine did not finish") ||
      !Check(!failed_result->has_value(), "failed read unexpectedly succeeded") ||
      !Check(failed_result->error().value() == EIO, "failed read returned wrong error") ||
      !Check(failed_resume_count == 1, "failed read resumed more than once") ||
      !Check(failed_with_scheduler, "failed read resumed without current scheduler")) {
    return false;
  }

  // A failed submission must release pending_read_. Otherwise this second
  // read would report EBUSY rather than submitting normally.
  constexpr std::string_view kPayload = "retry";
  if (!WriteFd(peer.fd(), kPayload)) return false;

  std::optional<coropact::base::Result<std::size_t>> retried_result;
  bool retried_with_scheduler = false;
  int retried_resume_count = 0;
  coropact::coro::SpawnDetach(
      loop, ReadOnce(&stream, &loop, buffer, &retried_result, &retried_with_scheduler,
                     &retried_resume_count));
  loop.RunReady();

  auto completions = loop.WaitCompletions();
  if (!completions.has_value()) {
    std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
    return false;
  }
  loop.RunReady();

  return Check(retried_result.has_value(), "retried read coroutine did not finish") &&
         Check(retried_result->has_value(), "retried read returned an error") &&
         Check(**retried_result == kPayload.size(), "retried read returned wrong byte count") &&
         Check(retried_resume_count == 1, "retried read resumed more than once") &&
         Check(retried_with_scheduler, "retried read resumed without current scheduler");
}

bool CheckWriteSubmitFailureRollsBack() {
  coropact::luring::LUringLoop loop;
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

  coropact::luring::LUringStream stream(&loop, local.Release(), EmptyPeerAddress());
  constexpr std::string_view kPayload = "retry";
  auto bytes = std::as_bytes(std::span<const char>(kPayload.data(), kPayload.size()));

  std::optional<coropact::base::Result<std::size_t>> failed_result;
  bool failed_with_scheduler = false;
  int failed_resume_count = 0;
  loop.FailNextSubmissionsForTesting(1, EIO);
  coropact::coro::SpawnDetach(
      loop, WriteOnce(&stream, &loop, bytes, &failed_result, &failed_with_scheduler,
                      &failed_resume_count));
  loop.RunReady();

  if (!Check(failed_result.has_value(), "failed write coroutine did not finish") ||
      !Check(!failed_result->has_value(), "failed write unexpectedly succeeded") ||
      !Check(failed_result->error().value() == EIO, "failed write returned wrong error") ||
      !Check(failed_resume_count == 1, "failed write resumed more than once") ||
      !Check(failed_with_scheduler, "failed write resumed without current scheduler")) {
    return false;
  }

  std::optional<coropact::base::Result<std::size_t>> retried_result;
  bool retried_with_scheduler = false;
  int retried_resume_count = 0;
  coropact::coro::SpawnDetach(
      loop, WriteOnce(&stream, &loop, bytes, &retried_result, &retried_with_scheduler,
                      &retried_resume_count));
  loop.RunReady();

  auto completions = loop.WaitCompletions();
  if (!completions.has_value()) {
    std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
    return false;
  }
  loop.RunReady();

  std::array<char, 16> read_buffer{};
  const ssize_t received = ::read(peer.fd(), read_buffer.data(), read_buffer.size());
  if (received < 0) {
    std::cout << "FAIL: peer read failed: " << errno << '\n';
    return false;
  }

  return Check(retried_result.has_value(), "retried write coroutine did not finish") &&
         Check(retried_result->has_value(), "retried write returned an error") &&
         Check(**retried_result == kPayload.size(), "retried write returned wrong byte count") &&
         Check(retried_resume_count == 1, "retried write resumed more than once") &&
         Check(retried_with_scheduler, "retried write resumed without current scheduler") &&
         Check(std::string_view(read_buffer.data(), static_cast<std::size_t>(received)) == kPayload,
               "retried write payload mismatch");
}

bool CheckTimedReadTimeoutSubmitFailureResumesOnce() {
  coropact::luring::LUringLoop loop;
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

  coropact::luring::LUringStream stream(&loop, local.Release(), EmptyPeerAddress());
  constexpr std::string_view kPayload = "fallback";
  if (!WriteFd(peer.fd(), kPayload)) return false;

  std::array<std::byte, 16> buffer{};
  std::optional<coropact::base::Result<std::size_t>> result;
  bool resumed_with_scheduler = false;
  int resume_count = 0;

  // ReadSomeFor first submits the linked recv, then the linked timeout. Let
  // the recv reach the kernel and fail only the optional timeout SQE.
  loop.FailSubmissionAfterForTesting(1, EIO);
  coropact::coro::SpawnDetach(
      loop, ReadForOnce(&stream, &loop, buffer, std::chrono::seconds(1), &result,
                         &resumed_with_scheduler, &resume_count));
  loop.RunReady();

  for (int i = 0; i < 4 && !result.has_value(); ++i) {
    auto completions = loop.WaitCompletions();
    if (!completions.has_value()) {
      std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
      return false;
    }
    loop.RunReady();
  }

  std::string_view actual(reinterpret_cast<const char*>(buffer.data()), kPayload.size());
  return Check(result.has_value(), "timed read fallback coroutine did not resume") &&
         Check(result->has_value(), "timed read fallback returned an error") &&
         Check(**result == kPayload.size(), "timed read fallback returned wrong byte count") &&
         Check(actual == kPayload, "timed read fallback payload mismatch") &&
         Check(resume_count == 1, "timed read fallback resumed more than once") &&
         Check(resumed_with_scheduler, "timed read fallback resumed without current scheduler");
}
#endif

bool CheckWriteSome() {
  coropact::luring::LUringLoop loop;
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

  coropact::luring::LUringStream stream(&loop, local.Release(), EmptyPeerAddress());

  constexpr std::string_view kPayload = "pong";
  auto bytes = std::as_bytes(std::span<const char>(kPayload.data(), kPayload.size()));

  std::optional<coropact::base::Result<std::size_t>> result;
  bool resumed_with_scheduler = false;

  coropact::coro::SpawnDetach(loop,
                              WriteOnce(&stream, &loop, bytes, &result, &resumed_with_scheduler));

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
  coropact::luring::LUringLoop loop;
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

  coropact::luring::LUringStream stream(&loop, local.Release(), EmptyPeerAddress());

  std::optional<coropact::base::Result<void>> result;
  coropact::coro::SpawnDetach(loop, CloseOnce(&stream, &result));

  loop.RunReady();

  return Check(result.has_value(), "close coroutine did not run") &&
         Check(result->has_value(), "Close without pending op returned an error");
}

bool CheckCloseCancelsPendingRead() {
  coropact::luring::LUringLoop loop;
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

  coropact::luring::LUringStream stream(&loop, local.Release(), EmptyPeerAddress());

  std::array<std::byte, 8> buffer{};
  std::optional<coropact::base::Result<std::size_t>> read_result;
  bool read_resumed_with_scheduler = false;
  int read_resume_count = 0;

  coropact::coro::SpawnDetach(loop, ReadOnce(&stream, &loop, buffer, &read_result,
                                             &read_resumed_with_scheduler, &read_resume_count));

  loop.RunReady();

  std::optional<coropact::base::Result<void>> close_result;
  coropact::coro::SpawnDetach(loop, CloseOnce(&stream, &close_result));

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
         Check(read_resume_count == 1, "pending read cancellation resumed more than once") &&
         Check(read_resumed_with_scheduler, "pending read resumed without current scheduler");
}

bool CheckReadCompletionCancelRaceResumesOnce() {
  coropact::luring::LUringLoop loop;
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

  coropact::luring::LUringStream stream(&loop, local.Release(), EmptyPeerAddress());
  std::array<std::byte, 8> buffer{};
  std::optional<coropact::base::Result<std::size_t>> read_result;
  bool read_resumed_with_scheduler = false;
  int read_resume_count = 0;

  coropact::coro::SpawnDetach(loop, ReadOnce(&stream, &loop, buffer, &read_result,
                                             &read_resumed_with_scheduler, &read_resume_count));
  loop.RunReady();

  constexpr std::string_view kPayload = "race";
  if (!WriteFd(peer.fd(), kPayload)) return false;

  std::optional<coropact::base::Result<void>> close_result;
  coropact::coro::SpawnDetach(loop, CloseOnce(&stream, &close_result));
  loop.RunReady();

  for (int i = 0; i < 6 && (!close_result.has_value() || !read_result.has_value()); ++i) {
    auto completions = loop.WaitCompletions();
    if (!completions.has_value()) {
      std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
      return false;
    }
    loop.RunReady();
  }

  if (!Check(close_result.has_value(), "race close coroutine did not finish") ||
      !Check(close_result->has_value(), "race close returned an error") ||
      !Check(read_result.has_value(), "race read coroutine did not finish") ||
      !Check(read_resume_count == 1, "CQE-cancel race resumed more than once") ||
      !Check(read_resumed_with_scheduler, "CQE-cancel race resumed without scheduler")) {
    return false;
  }

  if (read_result->has_value()) {
    return Check(**read_result == kPayload.size(), "race read returned wrong byte count");
  }
  return Check(read_result->error().value() == ECANCELED,
               "race read failed with an unexpected error");
}

}  // namespace

int main() {
  if (!CheckReadSome()) return 1;
  if (!CheckTimedReadSuccessResumesOnce()) return 1;
  if (!CheckTimedReadTimeoutResumesOnce()) return 1;
#if defined(COROPACT_ENABLE_TEST_HOOKS)
  if (!CheckReadSubmitFailureRollsBack()) return 1;
  if (!CheckWriteSubmitFailureRollsBack()) return 1;
  if (!CheckTimedReadTimeoutSubmitFailureResumesOnce()) return 1;
#endif
  if (!CheckWriteSome()) return 1;
  if (!CheckCloseWithoutPending()) return 1;
  if (!CheckCloseCancelsPendingRead()) return 1;
  if (!CheckReadCompletionCancelRaceResumesOnce()) return 1;

  std::cout << "luring stream smoke: PASS\n";
  return 0;
}
