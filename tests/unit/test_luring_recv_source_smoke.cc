// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/coro/detached_task.h"
#include "coropact/coro/spawn.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/options.h"
#include "coropact/luring/recv_source.h"

namespace {

using coropact::base::Error;
using coropact::base::Result;
using coropact::coro::DetachedTask;
using coropact::luring::LUringLoop;
using coropact::luring::LUringOptions;
using coropact::luring::LUringRecvSource;
using coropact::luring::LUringRecvSourceOptions;

class UniqueFd final {
public:
  explicit UniqueFd(int fd = -1) noexcept : fd_(fd) {}

  UniqueFd(const UniqueFd&) = delete;
  UniqueFd& operator=(const UniqueFd&) = delete;

  UniqueFd(UniqueFd&& other) noexcept
      : fd_(std::exchange(other.fd_, -1)) {}

  UniqueFd& operator=(UniqueFd&& other) noexcept {
    if (this != &other) {
      Reset();
      fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
  }

  ~UniqueFd() { Reset(); }

  [[nodiscard]]
  int Get() const noexcept { return fd_; }

  void Reset() noexcept {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

private:
  int fd_{-1};
};

enum class LoopInitStatus {
  kReady,
  kSkip,
  kFail,
};

struct Observation {
  bool done{false};
  bool eof{false};
  bool stopped{false};
  std::string payload;
  std::optional<Error> error;
};

struct StopObservation {
  bool done{false};
  bool succeeded{false};
  std::optional<Error> error;
};

struct IncrementalObservation {
  bool done{false};
  bool stopped{false};
  std::string first;
  std::string second;
  std::uint32_t first_buffer{0};
  std::uint32_t second_buffer{0};
  std::uintptr_t first_address{0};
  std::uintptr_t second_address{0};
  std::optional<Error> error;
};

bool IsEnvironmentSkip(Error error) {
  return error == std::errc::operation_not_supported ||
         error == std::errc::operation_not_permitted ||
         error == std::errc::permission_denied ||
         error == std::errc::function_not_supported ||
         error.value() == EINVAL;
}

LoopInitStatus InitLoop(LUringLoop& loop) {
  LUringOptions options;
  options.entries = 32;
  options.submit_batch = 1;

  auto initialized = loop.Init(options);
  if (initialized.has_value()) {
    return LoopInitStatus::kReady;
  }
  if (IsEnvironmentSkip(initialized.error())) {
    std::cout << "SKIP: io_uring unavailable: "
              << initialized.error().message() << '\n';
    return LoopInitStatus::kSkip;
  }

  std::cout << "FAIL: loop init failed: "
            << initialized.error().message() << '\n';
  return LoopInitStatus::kFail;
}

template <typename Predicate>
bool PumpUntil(LUringLoop& loop, Predicate&& predicate, int max_iterations = 64) {
  for (int i = 0; i < max_iterations && !predicate(); ++i) {
    auto completed = loop.WaitCompletions();
    if (!completed.has_value()) {
      std::cout << "FAIL: waiting for CQE failed: "
                << completed.error().message() << '\n';
      return false;
    }
    loop.RunReady();
  }
  return predicate();
}

Result<std::pair<UniqueFd, UniqueFd>> MakeSocketPair() {
  int fds[2] = {-1, -1};
  if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }
  return std::make_pair(UniqueFd(fds[0]), UniqueFd(fds[1]));
}

DetachedTask ReceiveOne(LUringRecvSource* source, Observation* observation) {
  auto received = co_await source->Next();
  if (!received.has_value()) {
    observation->error = received.error();
    observation->done = true;
    co_return;
  }

  if (!received->has_value()) {
    observation->eof = true;
  } else {
    const auto bytes = (*received)->buffer.Bytes();
    observation->payload.assign(
        reinterpret_cast<const char*>(bytes.data()), bytes.size());
    // Stop must not complete while this lease is still alive. Release it
    // before awaiting Stop so the source can unregister its buffer ring.
    received->reset();
  }

  auto stopped = co_await source->Stop();
  if (!stopped.has_value()) {
    observation->error = stopped.error();
  } else {
    observation->stopped = true;
  }
  observation->done = true;
}

DetachedTask WaitForNext(
    LUringRecvSource* source,
    Observation* observation) {
  auto received = co_await source->Next();
  if (!received.has_value()) {
    observation->error = received.error();
  } else if (!received->has_value()) {
    observation->eof = true;
  } else {
    received->reset();
  }
  observation->done = true;
}

DetachedTask StopSource(
    LUringRecvSource* source,
    StopObservation* observation) {
  auto stopped = co_await source->Stop();
  if (!stopped.has_value()) {
    observation->error = stopped.error();
  } else {
    observation->succeeded = true;
  }
  observation->done = true;
}

DetachedTask ReceiveIncremental(
    LUringRecvSource* source,
    int sender,
    IncrementalObservation* observation) {
  auto first = co_await source->Next();
  if (!first.has_value() || !first->has_value()) {
    observation->error = first.has_value()
                             ? coropact::base::MakeErrno(EPROTO)
                             : first.error();
    (void)co_await source->Stop();
    observation->done = true;
    co_return;
  }
  const auto first_bytes = (*first)->buffer.Bytes();
  observation->first.assign(
      reinterpret_cast<const char*>(first_bytes.data()), first_bytes.size());
  observation->first_buffer = (*first)->buffer.BufferId();
  observation->first_address =
      reinterpret_cast<std::uintptr_t>(first_bytes.data());

  constexpr std::string_view kSecond = "defgh";
  const auto sent = ::send(sender, kSecond.data(), kSecond.size(), MSG_NOSIGNAL);
  if (sent != static_cast<ssize_t>(kSecond.size())) {
    observation->error = coropact::base::CurrentErrno();
    (void)co_await source->Stop();
    observation->done = true;
    co_return;
  }

  auto second = co_await source->Next();
  if (!second.has_value() || !second->has_value()) {
    observation->error = second.has_value()
                              ? coropact::base::MakeErrno(EPROTO)
                              : second.error();
    (void)co_await source->Stop();
    observation->done = true;
    co_return;
  }
  const auto second_bytes = (*second)->buffer.Bytes();
  observation->second.assign(
      reinterpret_cast<const char*>(second_bytes.data()), second_bytes.size());
  observation->second_buffer = (*second)->buffer.BufferId();
  observation->second_address =
      reinterpret_cast<std::uintptr_t>(second_bytes.data());
  // Keep both leases alive while the second segment is delivered. The ring
  // must not recycle the incrementally consumed buffer until the terminal
  // CQE and both consumer leases have been observed.
  const bool contiguous = observation->second_address ==
                           observation->first_address + observation->first.size();
  first->reset();
  second->reset();
  if (!contiguous) {
    observation->error = coropact::base::MakeErrno(EPROTO);
    (void)co_await source->Stop();
    observation->done = true;
    co_return;
  }

  auto stopped = co_await source->Stop();
  if (!stopped.has_value()) {
    observation->error = stopped.error();
  } else {
    observation->stopped = true;
  }
  observation->done = true;
}

bool CheckRecvAndLease() {
  LUringLoop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  auto pair = MakeSocketPair();
  if (!pair.has_value()) {
    std::cout << "FAIL: socketpair failed: " << pair.error().message() << '\n';
    return false;
  }
  auto receiver = std::move(pair->first);
  auto sender = std::move(pair->second);

  LUringRecvSourceOptions options;
  options.source.pending_depth = 1;
  options.source.event_capacity = 4;
  options.source.buffer_capacity = 4;
  options.buffer_size = 256;

  auto source_result = LUringRecvSource::Create(&loop, receiver.Get(), options);
  if (!source_result.has_value()) {
    if (IsEnvironmentSkip(source_result.error())) {
      std::cout << "SKIP: provided buffer ring unavailable: "
                << source_result.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: RecvSource creation failed: "
              << source_result.error().message() << '\n';
    return false;
  }
  auto source = std::move(*source_result);

  Observation observation;
  coropact::coro::SpawnDetach(loop, ReceiveOne(&source, &observation));
  loop.RunReady();

  constexpr std::string_view kPayload = "provided-buffer-recv";
  const auto sent = ::send(sender.Get(), kPayload.data(), kPayload.size(), MSG_NOSIGNAL);
  if (sent != static_cast<ssize_t>(kPayload.size())) {
    std::cout << "FAIL: send failed: " << coropact::base::CurrentErrno().message() << '\n';
    return false;
  }

  if (!PumpUntil(loop, [&] { return observation.done || observation.error.has_value(); })) {
    return false;
  }
  if (observation.error.has_value()) {
    std::cout << "FAIL: recv source failed: "
              << observation.error->message() << '\n';
    return false;
  }

  return observation.payload == kPayload && observation.stopped && !observation.eof;
}

bool CheckEof() {
  LUringLoop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  auto pair = MakeSocketPair();
  if (!pair.has_value()) {
    std::cout << "FAIL: socketpair failed: " << pair.error().message() << '\n';
    return false;
  }
  auto receiver = std::move(pair->first);
  auto sender = std::move(pair->second);

  auto source_result = LUringRecvSource::Create(&loop, receiver.Get());
  if (!source_result.has_value()) {
    if (IsEnvironmentSkip(source_result.error())) {
      std::cout << "SKIP: provided buffer ring unavailable: "
                << source_result.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: RecvSource creation failed: "
              << source_result.error().message() << '\n';
    return false;
  }
  auto source = std::move(*source_result);

  Observation observation;
  coropact::coro::SpawnDetach(loop, ReceiveOne(&source, &observation));
  loop.RunReady();
  sender.Reset();

  if (!PumpUntil(loop, [&] { return observation.done || observation.error.has_value(); })) {
    return false;
  }
  if (observation.error.has_value()) {
    std::cout << "FAIL: EOF recv source failed: "
              << observation.error->message() << '\n';
    return false;
  }
  return observation.eof && observation.stopped;
}

bool CheckIncrementalBufferConsumption() {
  LUringLoop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  auto pair = MakeSocketPair();
  if (!pair.has_value()) {
    std::cout << "FAIL: socketpair failed: " << pair.error().message() << '\n';
    return false;
  }
  auto receiver = std::move(pair->first);
  auto sender = std::move(pair->second);

  LUringRecvSourceOptions options;
  options.source.pending_depth = 1;
  options.source.event_capacity = 2;
  options.source.buffer_capacity = 2;
  options.buffer_size = 8;
  options.incremental_buffer_consumption = true;

  auto source_result = LUringRecvSource::Create(&loop, receiver.Get(), options);
  if (!source_result.has_value()) {
    if (IsEnvironmentSkip(source_result.error())) {
      std::cout << "SKIP: incremental provided buffer ring unavailable: "
                << source_result.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: incremental RecvSource creation failed: "
              << source_result.error().message() << '\n';
    return false;
  }
  auto source = std::move(*source_result);

  IncrementalObservation observation;
  coropact::coro::SpawnDetach(
      loop, ReceiveIncremental(&source, sender.Get(), &observation));
  loop.RunReady();

  constexpr std::string_view kFirst = "abc";
  const auto sent = ::send(sender.Get(), kFirst.data(), kFirst.size(), MSG_NOSIGNAL);
  if (sent != static_cast<ssize_t>(kFirst.size())) {
    std::cout << "FAIL: incremental first send failed: "
              << coropact::base::CurrentErrno().message() << '\n';
    return false;
  }

  if (!PumpUntil(loop, [&] { return observation.done || observation.error.has_value(); })) {
    return false;
  }
  if (observation.error.has_value()) {
    std::cout << "FAIL: incremental recv source failed: "
              << observation.error->message() << '\n';
    return false;
  }

  return observation.first == kFirst && observation.second == "defgh" &&
         observation.first_buffer == observation.second_buffer &&
         observation.second_address ==
             observation.first_address + observation.first.size() &&
         observation.stopped;
}

#if defined(COROPACT_ENABLE_TEST_HOOKS)

bool CheckInitialSubmitFailure() {
  LUringLoop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  auto pair = MakeSocketPair();
  if (!pair.has_value()) {
    std::cout << "FAIL: socketpair failed: " << pair.error().message() << '\n';
    return false;
  }
  auto receiver = std::move(pair->first);
  auto sender = std::move(pair->second);

  auto source_result = LUringRecvSource::Create(&loop, receiver.Get());
  if (!source_result.has_value()) {
    if (IsEnvironmentSkip(source_result.error())) {
      std::cout << "SKIP: provided buffer ring unavailable: "
                << source_result.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: RecvSource creation failed: "
              << source_result.error().message() << '\n';
    return false;
  }
  auto source = std::move(*source_result);

  loop.FailNextSubmissionsForTesting(1, EIO);
  Observation observation;
  coropact::coro::SpawnDetach(
      loop, WaitForNext(&source, &observation));
  loop.RunReady();

  if (!PumpUntil(loop, [&] { return observation.done; })) {
    return false;
  }
  return observation.error.has_value() && observation.error->value() == EIO;
}

bool CheckCancelSubmitFailure() {
  LUringLoop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  auto pair = MakeSocketPair();
  if (!pair.has_value()) {
    std::cout << "FAIL: socketpair failed: " << pair.error().message() << '\n';
    return false;
  }
  auto receiver = std::move(pair->first);
  auto sender = std::move(pair->second);

  auto source_result = LUringRecvSource::Create(&loop, receiver.Get());
  if (!source_result.has_value()) {
    if (IsEnvironmentSkip(source_result.error())) {
      std::cout << "SKIP: provided buffer ring unavailable: "
                << source_result.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: RecvSource creation failed: "
              << source_result.error().message() << '\n';
    return false;
  }
  auto source = std::move(*source_result);

  Observation next_observation;
  coropact::coro::SpawnDetach(
      loop, WaitForNext(&source, &next_observation));
  loop.RunReady();

  loop.FailNextSubmissionsForTesting(1, EIO);
  StopObservation first_stop;
  coropact::coro::SpawnDetach(
      loop, StopSource(&source, &first_stop));
  loop.RunReady();

  if (!PumpUntil(loop, [&] { return first_stop.done; })) {
    return false;
  }
  if (!first_stop.error.has_value() || first_stop.error->value() != EIO) {
    std::cout << "FAIL: recv cancel submit failure was not propagated\n";
    return false;
  }

  StopObservation retry_stop;
  coropact::coro::SpawnDetach(
      loop, StopSource(&source, &retry_stop));
  loop.RunReady();

  if (!PumpUntil(loop, [&] {
        return retry_stop.done && next_observation.done;
      })) {
    return false;
  }
  return retry_stop.succeeded && next_observation.eof &&
         !next_observation.error.has_value();
}

#endif

}  // namespace

int main() {
  if (!CheckRecvAndLease()) {
    return 1;
  }
  if (!CheckEof()) {
    return 1;
  }
  if (!CheckIncrementalBufferConsumption()) {
    return 1;
  }
#if defined(COROPACT_ENABLE_TEST_HOOKS)
  if (!CheckInitialSubmitFailure()) {
    return 1;
  }
  if (!CheckCancelSubmitFailure()) {
    return 1;
  }
#endif
  std::cout << "luring recv source/BufferLease smoke: PASS\n";
  return 0;
}
