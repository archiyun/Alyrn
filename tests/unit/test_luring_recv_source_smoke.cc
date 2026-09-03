// SPDX-License-Identifier: MIT

#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <expected>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include "alyrn/coro/detached_task.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/uring/detail/loop_access.h"
#include "alyrn/uring/loop.h"
#include "alyrn/uring/options.h"
#include "alyrn/uring/recv_source.h"
#include "alyrn/uring/timer.h"
#include "alyrn/result.h"

namespace {

using alyrn::Error;
using alyrn::Result;
using alyrn::coro::DetachedTask;
using alyrn::uring::Loop;
using alyrn::uring::Options;
using alyrn::uring::RecvSource;
using alyrn::uring::RecvSourceOptions;

class UniqueFd final {
public:
  explicit UniqueFd(int fd = -1) noexcept : fd_(fd) {}

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

  int Get() const noexcept {
    return fd_;
  }

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

struct PauseResumeObservation {
  bool first_received{false};
  bool low_water_consumed{false};
  bool resumed_received{false};
  bool stopped{false};
  bool done{false};
  std::string first;
  std::string queued;
  std::string resumed;
  std::optional<Error> error;
};

struct HeldLeaseObservation {
  bool first_received{false};
  bool done{false};
  bool stopped{false};
  std::string first;
  std::string second;
  std::optional<Error> error;
};

bool IsEnvironmentSkip(Error error) {
  return error == std::errc::operation_not_supported ||
         error == std::errc::operation_not_permitted || error == std::errc::permission_denied ||
         error == std::errc::function_not_supported || error.value() == EINVAL;
}

LoopInitStatus InitLoop(Loop& loop, std::size_t shared_buffer_capacity = 64,
                        std::size_t shared_buffer_size = 16 * 1024) {
  Options options;
  options.entries = 32;

  options.shared_buffer_capacity = shared_buffer_capacity;
  options.shared_buffer_size = shared_buffer_size;
  auto initialized = loop.Init(options);
  if (initialized.has_value()) {
    return LoopInitStatus::kReady;
  }
  if (IsEnvironmentSkip(initialized.Error())) {
    std::cout << "SKIP: io_uring unavailable: " << initialized.Error().message() << '\n';
    return LoopInitStatus::kSkip;
  }

  std::cout << "FAIL: loop init failed: " << initialized.Error().message() << '\n';
  return LoopInitStatus::kFail;
}

template <typename Predicate>
bool PumpUntil(Loop& loop, Predicate&& predicate, int max_iterations = 64) {
  for (int i = 0; i < max_iterations && !predicate(); ++i) {
    auto completed = alyrn::uring::detail::LoopAccess::WaitCompletions(loop);
    if (!completed.has_value()) {
      std::cout << "FAIL: waiting for CQE failed: " << completed.Error().message() << '\n';
      return false;
    }
    alyrn::uring::detail::LoopAccess::RunReady(loop);
  }
  return predicate();
}

Result<std::pair<UniqueFd, UniqueFd>> MakeSocketPair() {
  int fds[2] = {-1, -1};
  if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) < 0) {
    return std::unexpected(alyrn::CurrentErrno());
  }
  return std::make_pair(UniqueFd(fds[0]), UniqueFd(fds[1]));
}

DetachedTask ReceiveOne(RecvSource* source, Observation* observation) {
  auto received = co_await source->Next();
  if (!received.has_value()) {
    observation->error = received.Error();
    observation->done = true;
    co_return;
  }

  if (!received->has_value()) {
    observation->eof = true;
  } else {
    const auto bytes = (*received)->buffer.Bytes();
    observation->payload.assign(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    // Stop must not complete while this lease is still alive. Release it
    // before awaiting Stop so the shared pool can reuse the slot.
    received->reset();
  }

  auto stopped = co_await source->Stop();
  if (!stopped.has_value()) {
    observation->error = stopped.Error();
  } else {
    observation->stopped = true;
  }
  observation->done = true;
}

/* Only the test-hook scenarios drive these two directly. */

DetachedTask ReceivePauseThenResume(RecvSource* source, Loop* loop,
                                    PauseResumeObservation* observation) {
  const auto take = [observation](RecvSource::NextResult received,
                                  std::string* target) -> bool {
    if (!received.has_value()) {
      observation->error = received.Error();
      return false;
    }
    if (!received->has_value()) {
      observation->error = alyrn::Errno(ECONNRESET);
      return false;
    }
    const auto bytes = (*received)->buffer.Bytes();
    target->assign(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    received->reset();
    return true;
  };

  auto first = co_await source->Next();
  if (!take(std::move(first), &observation->first)) {
    observation->done = true;
    co_return;
  }
  observation->first_received = true;

  // The second datagram is sent while this coroutine sleeps. With a one
  // element event queue it reaches the high-water mark and pauses the native
  // multishot request before this coroutine consumes it.
  auto delay = co_await alyrn::uring::SleepFor(*loop, std::chrono::milliseconds(50));
  if (!delay.has_value()) {
    observation->error = delay.Error();
    observation->done = true;
    co_return;
  }

  auto queued = co_await source->Next();
  if (!take(std::move(queued), &observation->queued)) {
    observation->done = true;
    co_return;
  }
  observation->low_water_consumed = true;

  // This cannot be fulfilled by the queue above: the test sends its payload
  // only after low_water_consumed is visible. A result proves that the
  // cancel target reached terminal and a fresh multishot recv was submitted.
  auto resumed = co_await source->Next();
  if (!take(std::move(resumed), &observation->resumed)) {
    observation->done = true;
    co_return;
  }
  observation->resumed_received = true;

  auto stopped = co_await source->Stop();
  if (!stopped.has_value()) {
    observation->error = stopped.Error();
  } else {
    observation->stopped = true;
  }
  observation->done = true;
}

DetachedTask ReceiveWithFirstLeaseHeld(RecvSource* source,
                                       HeldLeaseObservation* observation) {
  auto first = co_await source->Next();
  if (!first.has_value() || !first->has_value()) {
    observation->error = first.has_value() ? alyrn::Errno(ECONNRESET) : first.Error();
    observation->done = true;
    co_return;
  }
  observation->first_received = true;

  auto second = co_await source->Next();
  if (!second.has_value() || !second->has_value()) {
    observation->error = second.has_value() ? alyrn::Errno(ECONNRESET) : second.Error();
    observation->done = true;
    co_return;
  }

  const auto first_bytes = (*first)->buffer.Bytes();
  const auto second_bytes = (*second)->buffer.Bytes();
  observation->first.assign(reinterpret_cast<const char*>(first_bytes.data()), first_bytes.size());
  observation->second.assign(reinterpret_cast<const char*>(second_bytes.data()),
                             second_bytes.size());
  first->reset();
  second->reset();

  auto stopped = co_await source->Stop();
  if (!stopped.has_value()) {
    observation->error = stopped.Error();
  } else {
    observation->stopped = true;
  }
  observation->done = true;
}

bool CheckRecvAndLease() {
  Loop loop;
  switch (InitLoop(loop, 64, 256)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  auto pair = MakeSocketPair();
  if (!pair.has_value()) {
    std::cout << "FAIL: socketpair failed: " << pair.Error().message() << '\n';
    return false;
  }
  auto receiver = std::move(pair->first);
  auto sender = std::move(pair->second);

  RecvSourceOptions options;
  options.source.pending_depth = 1;
  options.source.event_capacity = 4;
  options.source.buffer_capacity = 4;
  options.buffer_size = 256;

  auto source_result = RecvSource::Create(&loop, receiver.Get(), options);
  if (!source_result.has_value()) {
    if (IsEnvironmentSkip(source_result.Error())) {
      std::cout << "SKIP: provided buffer ring unavailable: " << source_result.Error().message()
                << '\n';
      return true;
    }
    std::cout << "FAIL: RecvSource creation failed: " << source_result.Error().message() << '\n';
    return false;
  }
  auto source = std::move(*source_result);

  Observation observation;
  alyrn::coro::SpawnDetach(loop, ReceiveOne(&source, &observation));
  alyrn::uring::detail::LoopAccess::RunReady(loop);

  constexpr std::string_view kPayload = "provided-buffer-recv";
  const auto sent = ::send(sender.Get(), kPayload.data(), kPayload.size(), MSG_NOSIGNAL);
  if (sent != static_cast<ssize_t>(kPayload.size())) {
    std::cout << "FAIL: send failed: " << alyrn::CurrentErrno().message() << '\n';
    return false;
  }

  if (!PumpUntil(loop, [&] { return observation.done || observation.error.has_value(); })) {
    return false;
  }
  if (observation.error.has_value()) {
    std::cout << "FAIL: recv source failed: " << observation.error->message() << '\n';
    return false;
  }

  return observation.payload == kPayload && observation.stopped && !observation.eof;
}

bool CheckHeldLeases() {
  Loop loop;
  switch (InitLoop(loop, 4, 256)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  auto pair = MakeSocketPair();
  if (!pair.has_value()) {
    std::cout << "FAIL: held-lease socketpair failed: " << pair.Error().message() << '\n';
    return false;
  }
  auto receiver = std::move(pair->first);
  auto sender = std::move(pair->second);

  RecvSourceOptions options;
  options.source.event_capacity = 2;
  options.source.buffer_capacity = 2;
  options.buffer_size = 256;
  auto source_result = RecvSource::Create(&loop, receiver.Get(), options);
  if (!source_result.has_value()) {
    if (IsEnvironmentSkip(source_result.Error())) {
      std::cout << "SKIP: held-lease provided buffer ring unavailable: "
                << source_result.Error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: held-lease source creation failed: " << source_result.Error().message()
              << '\n';
    return false;
  }
  auto source = std::move(*source_result);

  HeldLeaseObservation observation;
  alyrn::coro::SpawnDetach(loop, ReceiveWithFirstLeaseHeld(&source, &observation));
  alyrn::uring::detail::LoopAccess::RunReady(loop);

  constexpr std::string_view kFirst = "first-segment";
  constexpr std::string_view kSecond = "second-segment";
  if (::send(sender.Get(), kFirst.data(), kFirst.size(), MSG_NOSIGNAL) !=
      static_cast<ssize_t>(kFirst.size())) {
    std::cout << "FAIL: held-lease first send failed: " << alyrn::CurrentErrno().message()
              << '\n';
    return false;
  }
  if (!PumpUntil(loop,
                 [&] { return observation.error.has_value() || observation.first_received; })) {
    return false;
  }
  if (observation.error.has_value()) {
    std::cout << "FAIL: held-lease first recv failed: " << observation.error->message() << '\n';
    return false;
  }

  if (::send(sender.Get(), kSecond.data(), kSecond.size(), MSG_NOSIGNAL) !=
      static_cast<ssize_t>(kSecond.size())) {
    std::cout << "FAIL: held-lease second send failed: " << alyrn::CurrentErrno().message()
              << '\n';
    return false;
  }
  if (!PumpUntil(loop, [&] { return observation.error.has_value() || observation.done; })) {
    return false;
  }
  if (observation.error.has_value()) {
    std::cout << "FAIL: held-lease recv failed: " << observation.error->message() << '\n';
    return false;
  }
  return observation.first == kFirst && observation.second == kSecond && observation.stopped;
}

bool CheckSharedBufferPool() {
  Loop loop;
  switch (InitLoop(loop, 4, 256)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  auto first_pair = MakeSocketPair();
  auto second_pair = MakeSocketPair();
  if (!first_pair.has_value() || !second_pair.has_value()) {
    std::cout << "FAIL: shared-pool socketpair failed\n";
    return false;
  }
  auto first_receiver = std::move(first_pair->first);
  auto first_sender = std::move(first_pair->second);
  auto second_receiver = std::move(second_pair->first);
  auto second_sender = std::move(second_pair->second);

  RecvSourceOptions options;
  options.source.event_capacity = 2;
  options.source.buffer_capacity = 2;
  options.buffer_size = 256;

  auto first_source_result = RecvSource::Create(&loop, first_receiver.Get(), options);
  auto second_source_result = RecvSource::Create(&loop, second_receiver.Get(), options);
  if (!first_source_result.has_value() || !second_source_result.has_value()) {
    const auto& error = !first_source_result.has_value() ? first_source_result.Error()
                                                         : second_source_result.Error();
    if (IsEnvironmentSkip(error)) {
      std::cout << "SKIP: shared provided buffer ring unavailable: " << error.message() << '\n';
      return true;
    }
    std::cout << "FAIL: shared RecvSource creation failed: " << error.message() << '\n';
    return false;
  }
  auto first_source = std::move(*first_source_result);
  auto second_source = std::move(*second_source_result);

  Observation first_observation;
  Observation second_observation;
  alyrn::coro::SpawnDetach(loop, ReceiveOne(&first_source, &first_observation));
  alyrn::coro::SpawnDetach(loop, ReceiveOne(&second_source, &second_observation));
  alyrn::uring::detail::LoopAccess::RunReady(loop);

  constexpr std::string_view kFirstPayload = "shared-first";
  constexpr std::string_view kSecondPayload = "shared-second";
  if (::send(first_sender.Get(), kFirstPayload.data(), kFirstPayload.size(), MSG_NOSIGNAL) !=
          static_cast<ssize_t>(kFirstPayload.size()) ||
      ::send(second_sender.Get(), kSecondPayload.data(), kSecondPayload.size(), MSG_NOSIGNAL) !=
          static_cast<ssize_t>(kSecondPayload.size())) {
    std::cout << "FAIL: shared-pool send failed: " << alyrn::CurrentErrno().message() << '\n';
    return false;
  }

  if (!PumpUntil(loop, [&] {
        return (first_observation.done || first_observation.error.has_value()) &&
               (second_observation.done || second_observation.error.has_value());
      })) {
    return false;
  }
  if (first_observation.error.has_value() || second_observation.error.has_value()) {
    std::cout << "FAIL: shared-pool recv failed: "
              << (first_observation.error.has_value() ? first_observation.error->message()
                                                      : second_observation.error->message())
              << '\n';
    return false;
  }
  return first_observation.payload == kFirstPayload &&
         second_observation.payload == kSecondPayload && first_observation.stopped &&
         second_observation.stopped;
}

bool CheckEof() {
  Loop loop;
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
    std::cout << "FAIL: socketpair failed: " << pair.Error().message() << '\n';
    return false;
  }
  auto receiver = std::move(pair->first);
  auto sender = std::move(pair->second);

  auto source_result = RecvSource::Create(&loop, receiver.Get());
  if (!source_result.has_value()) {
    if (IsEnvironmentSkip(source_result.Error())) {
      std::cout << "SKIP: provided buffer ring unavailable: " << source_result.Error().message()
                << '\n';
      return true;
    }
    std::cout << "FAIL: RecvSource creation failed: " << source_result.Error().message() << '\n';
    return false;
  }
  auto source = std::move(*source_result);

  Observation observation;
  alyrn::coro::SpawnDetach(loop, ReceiveOne(&source, &observation));
  alyrn::uring::detail::LoopAccess::RunReady(loop);
  sender.Reset();

  if (!PumpUntil(loop, [&] { return observation.done || observation.error.has_value(); })) {
    return false;
  }
  if (observation.error.has_value()) {
    std::cout << "FAIL: EOF recv source failed: " << observation.error->message() << '\n';
    return false;
  }
  return observation.eof && observation.stopped;
}

bool CheckQueuePauseThenRearm() {
  Loop loop;
  switch (InitLoop(loop, 64, 256)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  auto pair = MakeSocketPair();
  if (!pair.has_value()) {
    std::cout << "FAIL: socketpair failed: " << pair.Error().message() << '\n';
    return false;
  }
  auto receiver = std::move(pair->first);
  auto sender = std::move(pair->second);

  RecvSourceOptions options;
  options.source.pending_depth = 1;
  options.source.event_capacity = 1;
  options.source.buffer_capacity = 2;
  options.buffer_size = 256;

  auto source_result = RecvSource::Create(&loop, receiver.Get(), options);
  if (!source_result.has_value()) {
    if (IsEnvironmentSkip(source_result.Error())) {
      std::cout << "SKIP: provided buffer ring unavailable: " << source_result.Error().message()
                << '\n';
      return true;
    }
    std::cout << "FAIL: RecvSource creation failed: " << source_result.Error().message() << '\n';
    return false;
  }
  auto source = std::move(*source_result);

  PauseResumeObservation observation;
  alyrn::coro::SpawnDetach(loop, ReceivePauseThenResume(&source, &loop, &observation));
  alyrn::uring::detail::LoopAccess::RunReady(loop);

  const auto send_payload = [&sender](std::string_view payload) -> bool {
    const auto sent = ::send(sender.Get(), payload.data(), payload.size(), MSG_NOSIGNAL);
    if (sent == static_cast<ssize_t>(payload.size())) {
      return true;
    }
    std::cout << "FAIL: send failed: " << alyrn::CurrentErrno().message() << '\n';
    return false;
  };

  constexpr std::string_view kFirst = "first";
  constexpr std::string_view kQueued = "queued";
  constexpr std::string_view kResumed = "resumed";
  if (!send_payload(kFirst)) {
    return false;
  }
  if (!PumpUntil(loop,
                 [&] { return observation.error.has_value() || observation.first_received; })) {
    return false;
  }
  if (observation.error.has_value()) {
    std::cout << "FAIL: first recv failed: " << observation.error->message() << '\n';
    return false;
  }

  if (!send_payload(kQueued)) {
    return false;
  }
  if (!PumpUntil(
          loop, [&] { return observation.error.has_value() || observation.low_water_consumed; },
          128)) {
    return false;
  }
  if (observation.error.has_value()) {
    std::cout << "FAIL: recv source did not drain to low-water: " << observation.error->message()
              << '\n';
    return false;
  }

  if (!send_payload(kResumed)) {
    return false;
  }
  if (!PumpUntil(loop, [&] { return observation.error.has_value() || observation.done; }, 128)) {
    return false;
  }
  if (observation.error.has_value()) {
    std::cout << "FAIL: recv source did not re-arm after low-water: "
              << observation.error->message() << '\n';
    return false;
  }

  return observation.first == kFirst && observation.queued == kQueued &&
         observation.resumed == kResumed && observation.first_received &&
         observation.low_water_consumed && observation.resumed_received && observation.stopped;
}

}  // namespace

int main() {
  if (!CheckRecvAndLease()) {
    return 1;
  }
  if (!CheckHeldLeases()) {
    return 1;
  }
  if (!CheckSharedBufferPool()) {
    return 1;
  }
  if (!CheckEof()) {
    return 1;
  }
  if (!CheckQueuePauseThenRearm()) {
    return 1;
  }
  std::cout << "luring recv source/BufferLease smoke: PASS\n";
  return 0;
}
