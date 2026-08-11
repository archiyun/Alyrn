// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <expected>
#include <iostream>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/spawn.h"
#include "coropact/coro/task.h"
#include "coropact/io/async_stream.h"
#include "coropact/io/read_into.h"
#include "coropact/luring/detail/loop_access.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/options.h"
#include "coropact/luring/stream.h"
#include "coropact/net/endpoint.h"

namespace {

using OwnedReadOutcome = coropact::io::ReadIntoOutcome;

static_assert(coropact::io::AsyncReadIntoStream<coropact::luring::LUringStream>);
static_assert(coropact::io::AsyncTimedStream<coropact::luring::LUringStream>);

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
  *resumed_with_scheduler = coropact::coro::Scheduler::TryCurrent() == loop;
  out->emplace(std::move(result));
}

coropact::coro::DetachedTask ReadIntoOnce(coropact::luring::LUringStream* stream,
                                          coropact::luring::LUringLoop* loop,
                                          coropact::net::Buffer buffer,
                                          std::optional<OwnedReadOutcome>* out,
                                          bool* resumed_with_scheduler,
                                          int* resume_count = nullptr) {
  OwnedReadOutcome outcome = co_await stream->ReadInto(std::move(buffer), 32);
  if (resume_count != nullptr) {
    ++*resume_count;
  }
  *resumed_with_scheduler = coropact::coro::Scheduler::TryCurrent() == loop;
  out->emplace(std::move(outcome));
}

coropact::coro::DetachedTask ReadIntoWithReserveOnce(coropact::luring::LUringStream* stream,
                                                     coropact::luring::LUringLoop* loop,
                                                     coropact::net::Buffer buffer,
                                                     std::size_t reserve,
                                                     std::optional<OwnedReadOutcome>* out,
                                                     bool* resumed_with_scheduler) {
  OwnedReadOutcome outcome = co_await stream->ReadInto(std::move(buffer), reserve);
  *resumed_with_scheduler = coropact::coro::Scheduler::TryCurrent() == loop;
  out->emplace(std::move(outcome));
}

coropact::coro::DetachedTask ReadForOnce(coropact::luring::LUringStream* stream,
                                         coropact::luring::LUringLoop* loop,
                                         std::span<std::byte> buffer,
                                         std::chrono::milliseconds timeout,
                                         std::optional<coropact::base::Result<std::size_t>>* out,
                                         bool* resumed_with_scheduler, int* resume_count) {
  auto result = co_await stream->ReadSomeFor(buffer, timeout);
  ++*resume_count;
  *resumed_with_scheduler = coropact::coro::Scheduler::TryCurrent() == loop;
  out->emplace(std::move(result));
}

coropact::coro::DetachedTask TimedReadThenRead(
    coropact::luring::LUringStream* stream, coropact::luring::LUringLoop* loop,
    std::span<std::byte> timed_buffer, std::span<std::byte> next_buffer,
    std::optional<coropact::base::Result<std::size_t>>* timed_result,
    std::optional<coropact::base::Result<std::size_t>>* next_result, bool* resumed_with_scheduler) {
  timed_result->emplace(co_await stream->ReadSomeFor(timed_buffer, std::chrono::seconds(1)));
  next_result->emplace(co_await stream->ReadSome(next_buffer));
  *resumed_with_scheduler = coropact::coro::Scheduler::TryCurrent() == loop;
}

coropact::coro::DetachedTask ReadThenRead(
    coropact::luring::LUringStream* stream, coropact::luring::LUringLoop* loop,
    std::span<std::byte> first_buffer, std::span<std::byte> second_buffer,
    std::optional<coropact::base::Result<std::size_t>>* first_result,
    std::optional<coropact::base::Result<std::size_t>>* second_result,
    bool* resumed_with_scheduler) {
  first_result->emplace(co_await stream->ReadSome(first_buffer));
  second_result->emplace(co_await stream->ReadSome(second_buffer));
  *resumed_with_scheduler = coropact::coro::Scheduler::TryCurrent() == loop;
}

coropact::coro::DetachedTask WriteOnce(coropact::luring::LUringStream* stream,
                                       coropact::luring::LUringLoop* loop,
                                       std::span<const std::byte> buffer,
                                       std::optional<coropact::base::Result<void>>* out,
                                       bool* resumed_with_scheduler, int* resume_count = nullptr) {
  auto result = co_await stream->WriteAll(buffer);
  if (resume_count != nullptr) {
    ++*resume_count;
  }
  *resumed_with_scheduler = coropact::coro::Scheduler::TryCurrent() == loop;
  out->emplace(std::move(result));
}

coropact::coro::DetachedTask CloseOnce(coropact::luring::LUringStream* stream,
                                       std::optional<coropact::base::Result<void>>* out) {
  auto result = co_await stream->Close();
  out->emplace(std::move(result));
}

coropact::coro::DetachedTask ShutdownThenReadAndWrite(
    coropact::luring::LUringStream* stream, coropact::luring::LUringLoop* loop,
    std::span<const std::byte> write_buffer, std::span<std::byte> read_buffer,
    std::optional<coropact::base::Result<void>>* first_shutdown,
    std::optional<coropact::base::Result<void>>* second_shutdown,
    std::optional<coropact::base::Result<void>>* write_result,
    std::optional<coropact::base::Result<std::size_t>>* read_result, bool* resumed_with_scheduler) {
  first_shutdown->emplace(co_await stream->Shutdown());
  second_shutdown->emplace(co_await stream->Shutdown());
  write_result->emplace(co_await stream->WriteAll(write_buffer));
  read_result->emplace(co_await stream->ReadSome(read_buffer));
  *resumed_with_scheduler = coropact::coro::Scheduler::TryCurrent() == loop;
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

  coropact::luring::detail::LoopAccess::RunReady(loop);

  auto completions = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
  if (!completions.has_value()) {
    std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
    return false;
  }

  coropact::luring::detail::LoopAccess::RunReady(loop);

  std::string_view actual(reinterpret_cast<const char*>(buffer.data()), kPayload.size());

  return Check(*completions >= 1, "read did not produce a completion") &&
         Check(result.has_value(), "read coroutine did not resume") &&
         Check(result->has_value(), "ReadSome returned an error") &&
         Check(**result == kPayload.size(), "ReadSome returned wrong byte count") &&
         Check(actual == kPayload, "ReadSome payload mismatch") &&
         Check(resumed_with_scheduler, "read resumed without current scheduler");
}

bool CheckReadReleasesSlotBeforeContinuation() {
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
  constexpr std::string_view kFirstPayload = "first";
  constexpr std::string_view kSecondPayload = "second";
  std::string payload{kFirstPayload};
  payload.append(kSecondPayload);
  if (!WriteFd(peer.fd(), payload)) return false;

  std::array<std::byte, kFirstPayload.size()> first_buffer{};
  std::array<std::byte, kSecondPayload.size()> second_buffer{};
  std::optional<coropact::base::Result<std::size_t>> first_result;
  std::optional<coropact::base::Result<std::size_t>> second_result;
  bool resumed_with_scheduler = false;

  coropact::coro::SpawnDetach(
      loop, ReadThenRead(&stream, &loop, first_buffer, second_buffer, &first_result, &second_result,
                         &resumed_with_scheduler));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  for (int i = 0; i < 8 && !second_result.has_value(); ++i) {
    auto completions = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
    if (!completions.has_value()) {
      std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
      return false;
    }
    coropact::luring::detail::LoopAccess::RunReady(loop);
  }

  const std::string_view first_actual(reinterpret_cast<const char*>(first_buffer.data()),
                                      kFirstPayload.size());
  const std::string_view second_actual(reinterpret_cast<const char*>(second_buffer.data()),
                                       kSecondPayload.size());
  return Check(first_result.has_value(), "first read did not finish before follow-up read") &&
         Check(first_result->has_value(), "first read returned an error") &&
         Check(**first_result == kFirstPayload.size(), "first read returned wrong byte count") &&
         Check(first_actual == kFirstPayload, "first read payload mismatch") &&
         Check(second_result.has_value(), "follow-up read did not finish") &&
         Check(second_result->has_value(),
               "single-shot read left the stream read slot reserved during continuation") &&
         Check(**second_result == kSecondPayload.size(), "follow-up read returned wrong byte count") &&
         Check(second_actual == kSecondPayload, "follow-up read payload mismatch") &&
         Check(resumed_with_scheduler,
               "single-shot follow-up read resumed without current scheduler");
}

bool CheckOwnedReadIntoReturnsBuffer() {
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
  constexpr std::string_view kPayload = "owned-read";
  if (!WriteFd(peer.fd(), kPayload)) return false;

  std::optional<OwnedReadOutcome> outcome;
  bool resumed_with_scheduler = false;
  coropact::coro::SpawnDetach(loop, ReadIntoOnce(&stream, &loop, coropact::net::Buffer(4), &outcome,
                                                 &resumed_with_scheduler));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  auto completions = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
  if (!completions.has_value()) {
    std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
    return false;
  }
  coropact::luring::detail::LoopAccess::RunReady(loop);

  if (!Check(*completions >= 1, "owned read did not produce a completion") ||
      !Check(outcome.has_value(), "owned read coroutine did not resume") ||
      !Check(outcome->result.has_value(), "owned read returned an error") ||
      !Check(*outcome->result == kPayload.size(), "owned read byte count mismatch") ||
      !Check(resumed_with_scheduler, "owned read resumed without current scheduler")) {
    return false;
  }

  std::string actual;
  for (const iovec& iov : outcome->buffer.ReadableIov(8)) {
    actual.append(static_cast<const char*>(iov.iov_base), iov.iov_len);
  }
  auto reusable = outcome->buffer.PrepareWrite(8, 1);
  const bool reusable_after_resume = !reusable.empty();
  outcome->buffer.AbortWrite();
  return Check(actual == kPayload, "owned read payload mismatch") &&
         Check(reusable_after_resume, "owned read returned a buffer with a live reservation");
}

bool CheckOwnedReadIntoSpansBufferBlocks() {
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
  constexpr std::string_view kPrefix = "abc";
  constexpr std::string_view kPayload = "12345678";

  coropact::net::Buffer buffer(4);
  buffer.Append(kPrefix);
  if (!WriteFd(peer.fd(), kPayload)) return false;

  std::optional<OwnedReadOutcome> outcome;
  bool resumed_with_scheduler = false;
  coropact::coro::SpawnDetach(
      loop, ReadIntoWithReserveOnce(&stream, &loop, std::move(buffer), kPayload.size(), &outcome,
                                    &resumed_with_scheduler));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  auto completions = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
  if (!completions.has_value()) {
    std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
    return false;
  }
  coropact::luring::detail::LoopAccess::RunReady(loop);

  if (!Check(*completions >= 1, "vectored owned read did not produce a completion") ||
      !Check(outcome.has_value(), "vectored owned read coroutine did not resume") ||
      !Check(outcome->result.has_value(), "vectored owned read returned an error") ||
      !Check(*outcome->result == kPayload.size(), "vectored owned read byte count mismatch") ||
      !Check(resumed_with_scheduler, "vectored owned read resumed without current scheduler")) {
    return false;
  }

  std::string actual;
  for (const iovec& iov : outcome->buffer.ReadableIov(8)) {
    actual.append(static_cast<const char*>(iov.iov_base), iov.iov_len);
  }
  std::string expected(kPrefix);
  expected.append(kPayload);
  return Check(actual == expected, "vectored owned read payload mismatch");
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
  coropact::luring::detail::LoopAccess::RunReady(loop);

  for (int i = 0; i < 4 && !result.has_value(); ++i) {
    auto completions = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
    if (!completions.has_value()) {
      std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
      return false;
    }
    coropact::luring::detail::LoopAccess::RunReady(loop);
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
  coropact::luring::detail::LoopAccess::RunReady(loop);

  for (int i = 0; i < 4 && !result.has_value(); ++i) {
    auto completions = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
    if (!completions.has_value()) {
      std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
      return false;
    }
    coropact::luring::detail::LoopAccess::RunReady(loop);
  }

  return Check(result.has_value(), "timed read timeout coroutine did not resume") &&
         Check(!result->has_value(), "timed read timeout unexpectedly succeeded") &&
         Check(result->error().value() == ETIMEDOUT, "timed read did not return ETIMEDOUT") &&
         Check(resume_count == 1, "timed read timeout resumed more than once") &&
         Check(resumed_with_scheduler, "timed read timeout resumed without current scheduler");
}

bool CheckTimedReadReleasesSlotBeforeContinuation() {
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
  constexpr std::string_view kTimedPayload = "timed";
  constexpr std::string_view kNextPayload = "next";
  std::string payload{kTimedPayload};
  payload.append(kNextPayload);
  if (!WriteFd(peer.fd(), payload)) return false;

  std::array<std::byte, kTimedPayload.size()> timed_buffer{};
  std::array<std::byte, kNextPayload.size()> next_buffer{};
  std::optional<coropact::base::Result<std::size_t>> timed_result;
  std::optional<coropact::base::Result<std::size_t>> next_result;
  bool resumed_with_scheduler = false;

  coropact::coro::SpawnDetach(
      loop, TimedReadThenRead(&stream, &loop, timed_buffer, next_buffer, &timed_result,
                              &next_result, &resumed_with_scheduler));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  for (int i = 0; i < 8 && !next_result.has_value(); ++i) {
    auto completions = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
    if (!completions.has_value()) {
      std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
      return false;
    }
    coropact::luring::detail::LoopAccess::RunReady(loop);
  }

  const std::string_view timed_actual(reinterpret_cast<const char*>(timed_buffer.data()),
                                      kTimedPayload.size());
  const std::string_view next_actual(reinterpret_cast<const char*>(next_buffer.data()),
                                     kNextPayload.size());
  return Check(timed_result.has_value(), "timed read did not finish before follow-up read") &&
         Check(timed_result->has_value(), "timed read returned an error before follow-up read") &&
         Check(**timed_result == kTimedPayload.size(), "timed read returned wrong byte count") &&
         Check(timed_actual == kTimedPayload, "timed read payload mismatch") &&
         Check(next_result.has_value(), "follow-up read did not finish") &&
         Check(next_result->has_value(),
               "timed read left the stream read slot reserved during continuation") &&
         Check(**next_result == kNextPayload.size(), "follow-up read returned wrong byte count") &&
         Check(next_actual == kNextPayload, "follow-up read payload mismatch") &&
         Check(resumed_with_scheduler,
               "follow-up read continuation resumed without current scheduler");
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
  coropact::coro::SpawnDetach(loop, ReadOnce(&stream, &loop, buffer, &failed_result,
                                             &failed_with_scheduler, &failed_resume_count));
  coropact::luring::detail::LoopAccess::RunReady(loop);

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
  coropact::coro::SpawnDetach(loop, ReadOnce(&stream, &loop, buffer, &retried_result,
                                             &retried_with_scheduler, &retried_resume_count));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  auto completions = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
  if (!completions.has_value()) {
    std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
    return false;
  }
  coropact::luring::detail::LoopAccess::RunReady(loop);

  return Check(retried_result.has_value(), "retried read coroutine did not finish") &&
         Check(retried_result->has_value(), "retried read returned an error") &&
         Check(**retried_result == kPayload.size(), "retried read returned wrong byte count") &&
         Check(retried_resume_count == 1, "retried read resumed more than once") &&
         Check(retried_with_scheduler, "retried read resumed without current scheduler");
}

bool CheckOwnedReadSubmitFailureReturnsBuffer() {
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
  std::optional<OwnedReadOutcome> failed_outcome;
  bool failed_with_scheduler = false;
  int failed_resume_count = 0;

  loop.FailNextSubmissionsForTesting(1, EIO);
  coropact::coro::SpawnDetach(
      loop, ReadIntoOnce(&stream, &loop, coropact::net::Buffer(8), &failed_outcome,
                         &failed_with_scheduler, &failed_resume_count));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  if (!Check(failed_outcome.has_value(), "failed owned read coroutine did not finish") ||
      !Check(!failed_outcome->result.has_value(), "failed owned read unexpectedly succeeded") ||
      !Check(failed_outcome->result.error().value() == EIO,
             "failed owned read returned wrong error") ||
      !Check(failed_resume_count == 1, "failed owned read resumed more than once") ||
      !Check(failed_with_scheduler, "failed owned read resumed without current scheduler")) {
    return false;
  }

  auto reusable = failed_outcome->buffer.PrepareWrite(8, 1);
  const bool buffer_released_after_failure = !reusable.empty();
  failed_outcome->buffer.AbortWrite();
  if (!Check(buffer_released_after_failure,
             "failed owned read returned a buffer with a live reservation")) {
    return false;
  }

  // Submission rollback must release pending_read_. A later owned read must
  // submit instead of being rejected as EBUSY.
  constexpr std::string_view kPayload = "retry";
  if (!WriteFd(peer.fd(), kPayload)) return false;

  std::optional<OwnedReadOutcome> retried_outcome;
  bool retried_with_scheduler = false;
  int retried_resume_count = 0;
  coropact::coro::SpawnDetach(
      loop, ReadIntoOnce(&stream, &loop, coropact::net::Buffer(8), &retried_outcome,
                         &retried_with_scheduler, &retried_resume_count));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  auto completions = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
  if (!completions.has_value()) {
    std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
    return false;
  }
  coropact::luring::detail::LoopAccess::RunReady(loop);

  return Check(retried_outcome.has_value(), "retried owned read did not finish") &&
         Check(retried_outcome->result.has_value(), "retried owned read returned an error") &&
         Check(*retried_outcome->result == kPayload.size(),
               "retried owned read returned wrong byte count") &&
         Check(retried_resume_count == 1, "retried owned read resumed more than once") &&
         Check(retried_with_scheduler, "retried owned read resumed without current scheduler");
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

  std::optional<coropact::base::Result<void>> failed_result;
  bool failed_with_scheduler = false;
  int failed_resume_count = 0;
  loop.FailNextSubmissionsForTesting(1, EIO);
  coropact::coro::SpawnDetach(loop, WriteOnce(&stream, &loop, bytes, &failed_result,
                                              &failed_with_scheduler, &failed_resume_count));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  if (!Check(failed_result.has_value(), "failed write coroutine did not finish") ||
      !Check(!failed_result->has_value(), "failed write unexpectedly succeeded") ||
      !Check(failed_result->error().value() == EIO, "failed write returned wrong error") ||
      !Check(failed_resume_count == 1, "failed write resumed more than once") ||
      !Check(failed_with_scheduler, "failed write resumed without current scheduler")) {
    return false;
  }

  std::optional<coropact::base::Result<void>> retried_result;
  bool retried_with_scheduler = false;
  int retried_resume_count = 0;
  coropact::coro::SpawnDetach(loop, WriteOnce(&stream, &loop, bytes, &retried_result,
                                              &retried_with_scheduler, &retried_resume_count));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  auto completions = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
  if (!completions.has_value()) {
    std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
    return false;
  }
  coropact::luring::detail::LoopAccess::RunReady(loop);

  std::array<char, 16> read_buffer{};
  const ssize_t received = ::read(peer.fd(), read_buffer.data(), read_buffer.size());
  if (received < 0) {
    std::cout << "FAIL: peer read failed: " << errno << '\n';
    return false;
  }

  return Check(retried_result.has_value(), "retried write coroutine did not finish") &&
         Check(retried_result->has_value(), "retried write returned an error") &&
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
  coropact::coro::SpawnDetach(loop, ReadForOnce(&stream, &loop, buffer, std::chrono::seconds(1),
                                                &result, &resumed_with_scheduler, &resume_count));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  for (int i = 0; i < 4 && !result.has_value(); ++i) {
    auto completions = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
    if (!completions.has_value()) {
      std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
      return false;
    }
    coropact::luring::detail::LoopAccess::RunReady(loop);
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

bool CheckWriteAll() {
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

  std::optional<coropact::base::Result<void>> result;
  bool resumed_with_scheduler = false;

  coropact::coro::SpawnDetach(loop,
                              WriteOnce(&stream, &loop, bytes, &result, &resumed_with_scheduler));

  coropact::luring::detail::LoopAccess::RunReady(loop);

  auto completions = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
  if (!completions.has_value()) {
    std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
    return false;
  }

  coropact::luring::detail::LoopAccess::RunReady(loop);

  std::array<char, 16> read_buffer{};
  ssize_t n = ::read(peer.fd(), read_buffer.data(), read_buffer.size());
  if (n < 0) {
    std::cout << "FAIL: peer read failed: " << errno << '\n';
    return false;
  }

  std::string_view actual(read_buffer.data(), static_cast<std::size_t>(n));

  return Check(*completions >= 1, "write did not produce a completion") &&
         Check(result.has_value(), "write coroutine did not resume") &&
         Check(result->has_value(), "WriteAll returned an error") &&
         Check(actual == kPayload, "WriteAll payload mismatch") &&
         Check(resumed_with_scheduler, "write resumed without current scheduler");
}

bool CheckShutdownKeepsReadOpen() {
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

  constexpr std::string_view kReply = "reply";
  if (!WriteFd(peer.fd(), kReply)) return false;

  coropact::luring::LUringStream stream(&loop, local.Release(), EmptyPeerAddress());
  constexpr std::string_view kWrite = "must-not-send";
  auto write_buffer = std::as_bytes(std::span<const char>(kWrite.data(), kWrite.size()));
  std::array<std::byte, 16> read_buffer{};
  std::optional<coropact::base::Result<void>> first_shutdown;
  std::optional<coropact::base::Result<void>> second_shutdown;
  std::optional<coropact::base::Result<void>> write_result;
  std::optional<coropact::base::Result<std::size_t>> read_result;
  bool resumed_with_scheduler = false;

  coropact::coro::SpawnDetach(
      loop, ShutdownThenReadAndWrite(&stream, &loop, write_buffer, read_buffer, &first_shutdown,
                                     &second_shutdown, &write_result, &read_result,
                                     &resumed_with_scheduler));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  auto completions = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
  if (!completions.has_value()) {
    std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
    return false;
  }
  coropact::luring::detail::LoopAccess::RunReady(loop);

  std::array<char, 1> peer_buffer{};
  const ssize_t peer_read = ::read(peer.fd(), peer_buffer.data(), peer_buffer.size());
  const std::string_view actual(reinterpret_cast<const char*>(read_buffer.data()), kReply.size());

  return Check(*completions >= 1, "read after Shutdown did not complete") &&
         Check(first_shutdown.has_value() && first_shutdown->has_value(),
               "first Shutdown failed") &&
         Check(second_shutdown.has_value() && second_shutdown->has_value(),
               "second Shutdown was not idempotent") &&
         Check(write_result.has_value() && !write_result->has_value(),
               "WriteAll after Shutdown unexpectedly succeeded") &&
         Check(write_result->error() == std::errc::broken_pipe,
               "WriteAll after Shutdown did not return EPIPE") &&
         Check(
             read_result.has_value() && read_result->has_value() && **read_result == kReply.size(),
             "ReadSome after Shutdown did not remain usable") &&
         Check(actual == kReply, "ReadSome after Shutdown returned wrong payload") &&
         Check(peer_read == 0, "peer did not observe Shutdown EOF") &&
         Check(resumed_with_scheduler, "read after Shutdown resumed without current scheduler");
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

  coropact::luring::detail::LoopAccess::RunReady(loop);

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

  coropact::luring::detail::LoopAccess::RunReady(loop);

  std::optional<coropact::base::Result<void>> close_result;
  coropact::coro::SpawnDetach(loop, CloseOnce(&stream, &close_result));

  coropact::luring::detail::LoopAccess::RunReady(loop);

  if (!Check(!close_result.has_value(), "Close with pending read should suspend")) {
    return false;
  }

  for (int i = 0; i < 4 && (!close_result.has_value() || !read_result.has_value()); ++i) {
    auto completions = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
    if (!completions.has_value()) {
      std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
      return false;
    }
    coropact::luring::detail::LoopAccess::RunReady(loop);
  }

  return Check(close_result.has_value(), "busy close coroutine did not run") &&
         Check(close_result->has_value(), "Close with pending read returned an error") &&
         Check(read_result.has_value(), "pending read was not cleaned up") &&
         Check(!read_result->has_value(), "pending read should be cancelled") &&
         Check(read_result->error().value() == ECANCELED, "pending read should return ECANCELED") &&
         Check(read_resume_count == 1, "pending read cancellation resumed more than once") &&
         Check(read_resumed_with_scheduler, "pending read resumed without current scheduler");
}

#if defined(COROPACT_ENABLE_TEST_HOOKS)
bool CheckCloseSubmitFailureAbortsPreparation() {
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
  coropact::luring::detail::LoopAccess::RunReady(loop);

  // The read SQE already exists. Fail only Close's local cancel submission;
  // the stream must return to Open rather than strand the pending read in a
  // provisional Closing state.
  loop.FailNextSubmissionsForTesting(1, EIO);
  std::optional<coropact::base::Result<void>> failed_close;
  coropact::coro::SpawnDetach(loop, CloseOnce(&stream, &failed_close));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  if (!Check(failed_close.has_value(), "failed close coroutine did not run") ||
      !Check(!failed_close->has_value(), "injected close submission unexpectedly succeeded") ||
      !Check(failed_close->error().value() == EIO,
             "injected close submission returned the wrong error")) {
    return false;
  }

  constexpr std::string_view kPayload = "retry";
  if (!WriteFd(peer.fd(), kPayload)) return false;

  for (int i = 0; i < 4 && !read_result.has_value(); ++i) {
    auto completions = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
    if (!completions.has_value()) {
      std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
      return false;
    }
    coropact::luring::detail::LoopAccess::RunReady(loop);
  }

  if (!Check(read_result.has_value(), "read did not survive failed close preparation") ||
      !Check(read_result->has_value(), "read after failed close preparation returned an error") ||
      !Check(**read_result == kPayload.size(),
             "read after failed close returned wrong byte count") ||
      !Check(read_resume_count == 1, "read after failed close resumed more than once") ||
      !Check(read_resumed_with_scheduler, "read after failed close resumed without scheduler")) {
    return false;
  }

  std::optional<coropact::base::Result<void>> retry_close;
  coropact::coro::SpawnDetach(loop, CloseOnce(&stream, &retry_close));
  coropact::luring::detail::LoopAccess::RunReady(loop);
  return Check(retry_close.has_value(), "retry Close did not finish") &&
         Check(retry_close->has_value(), "retry Close returned an error");
}
#endif

bool CheckCloseReturnsOwnedReadBuffer() {
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
  std::optional<OwnedReadOutcome> read_outcome;
  bool resumed_with_scheduler = false;
  int resume_count = 0;
  coropact::coro::SpawnDetach(loop,
                              ReadIntoOnce(&stream, &loop, coropact::net::Buffer(8), &read_outcome,
                                           &resumed_with_scheduler, &resume_count));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  std::optional<coropact::base::Result<void>> close_result;
  coropact::coro::SpawnDetach(loop, CloseOnce(&stream, &close_result));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  for (int i = 0; i < 4 && (!close_result.has_value() || !read_outcome.has_value()); ++i) {
    auto completions = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
    if (!completions.has_value()) {
      std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
      return false;
    }
    coropact::luring::detail::LoopAccess::RunReady(loop);
  }

  if (!Check(close_result.has_value(), "owned read close coroutine did not finish") ||
      !Check(close_result->has_value(), "owned read close returned an error") ||
      !Check(read_outcome.has_value(), "owned cancelled read did not resume") ||
      !Check(!read_outcome->result.has_value(), "owned cancelled read unexpectedly succeeded") ||
      !Check(read_outcome->result.error().value() == ECANCELED,
             "owned cancelled read did not return ECANCELED") ||
      !Check(resume_count == 1, "owned cancelled read resumed more than once") ||
      !Check(resumed_with_scheduler, "owned cancelled read resumed without current scheduler")) {
    return false;
  }

  auto reusable = read_outcome->buffer.PrepareWrite(8, 1);
  const bool reusable_after_cancel = !reusable.empty();
  read_outcome->buffer.AbortWrite();
  return Check(reusable_after_cancel,
               "owned cancelled read returned a buffer with a live reservation");
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
  coropact::luring::detail::LoopAccess::RunReady(loop);

  constexpr std::string_view kPayload = "race";
  if (!WriteFd(peer.fd(), kPayload)) return false;

  std::optional<coropact::base::Result<void>> close_result;
  coropact::coro::SpawnDetach(loop, CloseOnce(&stream, &close_result));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  for (int i = 0; i < 6 && (!close_result.has_value() || !read_result.has_value()); ++i) {
    auto completions = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
    if (!completions.has_value()) {
      std::cout << "FAIL: WaitCompletions failed: " << completions.error().message() << '\n';
      return false;
    }
    coropact::luring::detail::LoopAccess::RunReady(loop);
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
  if (!CheckReadReleasesSlotBeforeContinuation()) return 1;
  if (!CheckOwnedReadIntoReturnsBuffer()) return 1;
  if (!CheckOwnedReadIntoSpansBufferBlocks()) return 1;
  if (!CheckTimedReadSuccessResumesOnce()) return 1;
  if (!CheckTimedReadTimeoutResumesOnce()) return 1;
  if (!CheckTimedReadReleasesSlotBeforeContinuation()) return 1;
#if defined(COROPACT_ENABLE_TEST_HOOKS)
  if (!CheckReadSubmitFailureRollsBack()) return 1;
  if (!CheckOwnedReadSubmitFailureReturnsBuffer()) return 1;
  if (!CheckWriteSubmitFailureRollsBack()) return 1;
  if (!CheckTimedReadTimeoutSubmitFailureResumesOnce()) return 1;
  if (!CheckCloseSubmitFailureAbortsPreparation()) return 1;
#endif
  if (!CheckWriteAll()) return 1;
  if (!CheckShutdownKeepsReadOpen()) return 1;
  if (!CheckCloseWithoutPending()) return 1;
  if (!CheckCloseCancelsPendingRead()) return 1;
  if (!CheckCloseReturnsOwnedReadBuffer()) return 1;
  if (!CheckReadCompletionCancelRaceResumesOnce()) return 1;

  std::cout << "luring stream smoke: PASS\n";
  return 0;
}
