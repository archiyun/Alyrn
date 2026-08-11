// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <expected>
#include <iostream>
#include <optional>
#include <system_error>
#include <utility>
#include <vector>

#include "coropact/base/error.h"
#include "coropact/base/try.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/spawn.h"
#include "coropact/luring/listener.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/detail/loop_access.h"
#include "coropact/luring/options.h"
#include "coropact/luring/timer.h"
#include "coropact/net/accept_source.h"
#include "coropact/net/endpoint.h"

namespace {

using coropact::base::Error;
using coropact::base::Result;
using coropact::luring::LUringAcceptSource;
using coropact::luring::detail::CompletionEvent;
using coropact::luring::LUringListener;
using coropact::luring::LUringLoop;
using coropact::luring::LUringOptions;
using coropact::net::Endpoint;
using coropact::net::detail::AcceptSourceState;
using coropact::net::detail::AcceptSourceStateMachine;
using coropact::net::detail::EventDisposition;
using coropact::net::detail::MultishotRequestDisposition;

constexpr int kClientCount = 4;

enum class LoopInitStatus {
  kReady,
  kSkip,
  kFail,
};

class UniqueFd {
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

  void Reset() noexcept {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

private:
  int fd_{-1};
};

struct Observation {
  int accepted{0};
  bool stopped{false};
  bool unsupported{false};
  std::optional<Error> error;
};

struct CancelObservation {
  bool accepted{false};
  bool stopped{false};
  bool terminal{false};
  bool unsupported{false};
  std::optional<Error> error;
  bool done{false};
};

struct CloseObservation {
  bool source_end{false};
  bool close_succeeded{false};
  bool unsupported{false};
  std::optional<Error> error;
  bool done{false};
};

struct BackpressureObservation {
  bool first_received{false};
  bool queued_received{false};
  bool stop_succeeded{false};
  bool normal_end{false};
  bool unsupported{false};
  std::optional<Error> error;
  bool done{false};
};

// This observation deliberately separates the high-water and low-water
// phases. A successful third accept proves that the source did not turn a
// full queue into a logical terminal: it cancelled the old physical request,
// observed its terminal CQE, and armed a fresh request after consumption.
struct PauseResumeObservation {
  bool first_received{false};
  bool low_water_consumed{false};
  bool resumed_received{false};
  bool stop_succeeded{false};
  bool normal_end{false};
  bool unsupported{false};
  std::optional<Error> error;
  bool done{false};
};

struct StopObservation {
  bool succeeded{false};
  bool done{false};
  std::optional<Error> error;
};

bool IsUnsupported(Error error) {
  return error.value() == EINVAL ||
         error.value() == ENOSYS ||
         error.value() == ENOTSUP ||
         error.value() == EOPNOTSUPP ||
         error.value() == EPERM;
}

bool IsMultishotUnsupportedResult(int result) {
  return result == -EINVAL || result == -EOPNOTSUPP;
}

Endpoint LoopbackAddress(std::uint16_t port) {
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(port);
  return Endpoint(address);
}

LoopInitStatus InitLoop(LUringLoop& loop) {
  LUringOptions options;
  options.entries = 32;
  options.submit_batch = 1;

  auto initialized = loop.Init(options);
  if (initialized.has_value()) {
    return LoopInitStatus::kReady;
  }
  if (initialized.error() == std::errc::operation_not_supported ||
      initialized.error() == std::errc::operation_not_permitted) {
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
    auto completed = coropact::luring::detail::LoopAccess::WaitCompletions(loop);
    if (!completed.has_value()) {
      std::cout << "FAIL: waiting for CQE failed: "
                << completed.error().message() << '\n';
      return false;
    }
    coropact::luring::detail::LoopAccess::RunReady(loop);
  }
  return predicate();
}

Result<int> ConnectClient(const Endpoint& address) {
  const int fd = ::socket(
      AF_INET,
      SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
      IPPROTO_TCP);
  if (fd < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }

  const int result = ::connect(
      fd,
      address.SockAddr(),
      address.SockAddrLen());

  if (result < 0 && errno != EINPROGRESS) {
    const auto error = coropact::base::CurrentErrno();
    ::close(fd);
    return std::unexpected(error);
  }

  return fd;
}

void CheckCompletionEvent() {
  const CompletionEvent terminal{-ECANCELED, 0};
  assert(terminal.result == -ECANCELED);
  assert(!terminal.More());

  const CompletionEvent more{7, IORING_CQE_F_MORE};
  assert(more.result == 7);
  assert(more.More());

  const CompletionEvent buffer_more{
      3, IORING_CQE_F_MORE | IORING_CQE_F_BUF_MORE};
  assert(buffer_more.More());
  assert(buffer_more.BufferMore());

  const CompletionEvent notification{0, IORING_CQE_F_NOTIF};
  assert(notification.Notification());
  assert(!notification.More());
}

// Backend-neutral contract harness. It models only the source-side protocol:
// a terminal completion releases the old request, then the source attempts to
// rearm one request. The fake submitter lets the test deterministically fail
// that rearm without manufacturing an impossible kernel CQE ordering.
class FakeMultishotSourceHarness final {
public:
  explicit FakeMultishotSourceHarness(
      AcceptSourceStateMachine state) noexcept
      : state_(std::move(state)) {}

  Result<void> Start() noexcept {
    auto started = state_.Start();
    if (!started.has_value()) {
      return started;
    }
    return EnsureSubmission();
  }

  void FailNextSubmission(Error error) noexcept {
    submit_error_ = error;
  }

  Result<void> Complete(
      CompletionEvent event,
      EventDisposition disposition) noexcept {
    const bool unsupported_multishot =
        multishot_enabled_ && !event.More() &&
        IsMultishotUnsupportedResult(event.result);
    const auto request = event.More()
                             ? MultishotRequestDisposition::kMore
                             : MultishotRequestDisposition::kTerminal;
    if (request == MultishotRequestDisposition::kTerminal) {
      request_submitted_ = false;
    }

    if (unsupported_multishot) {
      // The first terminal unsupported CQE changes the selected physical path,
      // but it is not a logical source error. The source remains active and
      // rearms as a one-shot accept request.
      multishot_enabled_ = false;
      disposition = EventDisposition::kNone;
    }

    auto recorded = state_.CompleteMultishotEvent(disposition, request);
    if (!recorded.has_value()) {
      return recorded;
    }

    if (request == MultishotRequestDisposition::kTerminal &&
        state_.State() == AcceptSourceState::kActive) {
      return EnsureSubmission();
    }
    return {};
  }

  bool ConsumeEvent() noexcept { return state_.ConsumeEvent(); }

  [[nodiscard]]
  std::size_t SubmissionCount() const noexcept {
    return submission_count_;
  }

  [[nodiscard]]
  bool RequestSubmitted() const noexcept {
    return request_submitted_;
  }

  [[nodiscard]]
  bool MultishotEnabled() const noexcept {
    return multishot_enabled_;
  }

  [[nodiscard]]
  AcceptSourceState State() const noexcept {
    return state_.State();
  }

  [[nodiscard]]
  std::size_t QueuedEvents() const noexcept {
    return state_.QueuedEvents();
  }

private:
  Result<void> EnsureSubmission() noexcept {
    if (state_.State() != AcceptSourceState::kActive ||
        request_submitted_ || !state_.CanArm() || !state_.TryArm()) {
      return {};
    }

    ++submission_count_;
    if (submit_error_.has_value()) {
      const auto error = *submit_error_;
      submit_error_.reset();
      auto rolled_back = state_.CompleteMultishotEvent(
          EventDisposition::kNone,
          MultishotRequestDisposition::kTerminal);
      assert(rolled_back.has_value());
      state_.RequestStop();
      terminal_error_ = error;
      return std::unexpected(error);
    }

    request_submitted_ = true;
    return {};
  }

  AcceptSourceStateMachine state_;
  std::optional<Error> submit_error_;
  std::optional<Error> terminal_error_;
  std::size_t submission_count_{0};
  bool request_submitted_{false};
  bool multishot_enabled_{true};
};

void CheckFakeRearmSubmitFailure() {
  auto state_result = AcceptSourceStateMachine::Create({1, 4});
  assert(state_result.has_value());
  FakeMultishotSourceHarness source(std::move(*state_result));

  assert(source.Start().has_value());
  assert(source.SubmissionCount() == 1);
  assert(source.RequestSubmitted());

  // F_MORE keeps the original request in flight and must not rearm.
  assert(source.Complete(
      CompletionEvent{11, IORING_CQE_F_MORE},
      EventDisposition::kProduced).has_value());
  assert(source.SubmissionCount() == 1);
  assert(source.RequestSubmitted());
  assert(source.QueuedEvents() == 1);

  // A terminal positive event releases the old request and triggers rearm.
  // The injected failure must become a source terminal condition while the
  // two already produced events remain drainable.
  source.FailNextSubmission(coropact::base::MakeErrno(EIO));
  auto rearm = source.Complete(
      CompletionEvent{12, 0},
      EventDisposition::kProduced);
  assert(!rearm.has_value());
  assert(rearm.error().value() == EIO);
  assert(source.SubmissionCount() == 2);
  assert(!source.RequestSubmitted());
  assert(source.State() == AcceptSourceState::kDraining);
  assert(source.QueuedEvents() == 2);

  assert(source.ConsumeEvent());
  assert(source.State() == AcceptSourceState::kDraining);
  assert(source.ConsumeEvent());
  assert(source.State() == AcceptSourceState::kTerminal);
}

void CheckFakeMultishotFallback() {
  auto state_result = AcceptSourceStateMachine::Create({1, 4});
  assert(state_result.has_value());
  FakeMultishotSourceHarness source(std::move(*state_result));

  assert(source.Start().has_value());
  assert(source.MultishotEnabled());
  assert(source.SubmissionCount() == 1);

  // An unsupported terminal CQE is a runtime path-selection result, not a
  // logical AcceptSource failure. The next request must be one-shot.
  assert(source.Complete(
      CompletionEvent{-EINVAL, 0},
      EventDisposition::kNone).has_value());
  assert(!source.MultishotEnabled());
  assert(source.State() == AcceptSourceState::kActive);
  assert(source.SubmissionCount() == 2);
  assert(source.RequestSubmitted());

  // A one-shot completion still belongs to the same logical source and is
  // rearmed after each terminal completion.
  assert(source.Complete(
      CompletionEvent{17, 0},
      EventDisposition::kProduced).has_value());
  assert(source.State() == AcceptSourceState::kActive);
  assert(source.SubmissionCount() == 3);
  assert(source.RequestSubmitted());
  assert(source.QueuedEvents() == 1);

  assert(source.ConsumeEvent());
}

coropact::coro::DetachedTask Consume(
    LUringAcceptSource* source,
    Observation* observation) {
  for (int i = 0; i < kClientCount; ++i) {
    auto result = co_await source->Next();

    if (!result.has_value()) {
      if (IsUnsupported(result.error())) {
        observation->unsupported = true;
      } else {
        observation->error = result.error();
      }

      auto stopped = co_await source->Stop();
      if (!stopped.has_value()) {
        observation->error = stopped.error();
      }
      co_return;
    }

    if (!result->has_value()) {
      observation->error = coropact::base::MakeErrno(ECONNABORTED);
      co_return;
    }

    ++observation->accepted;
  }

  auto stopped = co_await source->Stop();
  observation->stopped = stopped.has_value();
  if (!stopped.has_value()) {
    observation->error = stopped.error();
  }
}

coropact::coro::DetachedTask ConsumeOneThenStop(
    LUringAcceptSource* source,
    CancelObservation* observation) {
  auto result = co_await source->Next();
  if (!result.has_value()) {
    if (IsUnsupported(result.error())) {
      observation->unsupported = true;
    } else {
      observation->error = result.error();
    }
    observation->done = true;
    co_return;
  }
  if (!result->has_value()) {
    observation->error = coropact::base::MakeErrno(ECONNABORTED);
    observation->done = true;
    co_return;
  }
  observation->accepted = true;

  // Stop while the multishot request is still active. This exercises the
  // independent cancel operation and the two-CQE convergence path.
  auto stopped = co_await source->Stop();
  observation->stopped = stopped.has_value();
  if (!stopped.has_value()) {
    observation->error = stopped.error();
    observation->done = true;
    co_return;
  }

  auto end = co_await source->Next();
  if (!end.has_value()) {
    observation->error = end.error();
  } else {
    observation->terminal = !end->has_value();
  }
  observation->done = true;
}

coropact::coro::DetachedTask WaitForSourceEnd(
    LUringAcceptSource* source,
    CloseObservation* observation) {
  auto result = co_await source->Next();
  if (!result.has_value()) {
    if (IsUnsupported(result.error())) {
      observation->unsupported = true;
    } else {
      observation->error = result.error();
    }
  } else {
    observation->source_end = !result->has_value();
  }
  observation->done = true;
}

coropact::coro::DetachedTask CloseListener(
    LUringListener* listener,
    CloseObservation* observation) {
  auto result = co_await listener->Close();
  observation->close_succeeded = result.has_value();
  if (!result.has_value()) {
    observation->error = result.error();
  }
}

coropact::coro::DetachedTask StopSource(
    LUringAcceptSource* source,
    StopObservation* observation) {
  auto result = co_await source->Stop();
  observation->succeeded = result.has_value();
  if (!result.has_value()) {
    observation->error = result.error();
  }
  observation->done = true;
}

coropact::coro::DetachedTask FillQueueThenStop(
    LUringAcceptSource* source,
    LUringLoop* loop,
    BackpressureObservation* observation) {
  auto first = co_await source->Next();
  if (!first.has_value()) {
    if (IsUnsupported(first.error())) {
      observation->unsupported = true;
    } else {
      observation->error = first.error();
    }
    observation->done = true;
    co_return;
  }
  if (!first->has_value()) {
    observation->error = coropact::base::MakeErrno(ECONNABORTED);
    observation->done = true;
    co_return;
  }
  observation->first_received = true;

  // Leave time for a burst to place one event in the bounded queue and a
  // later F_MORE CQE to hit the full-queue path.
  auto delay = co_await coropact::luring::SleepFor(
      *loop, std::chrono::milliseconds(50));
  if (!delay.has_value()) {
    observation->error = delay.error();
    observation->done = true;
    co_return;
  }

  auto stopped = co_await source->Stop();
  observation->stop_succeeded = stopped.has_value();
  if (!stopped.has_value()) {
    observation->error = stopped.error();
    observation->done = true;
    co_return;
  }

  auto queued = co_await source->Next();
  if (!queued.has_value()) {
    observation->error = queued.error();
    observation->done = true;
    co_return;
  }
  if (!queued->has_value()) {
    observation->error = coropact::base::MakeErrno(ECONNABORTED);
    observation->done = true;
    co_return;
  }
  observation->queued_received = true;

  auto terminal = co_await source->Next();
  if (!terminal.has_value()) {
    observation->error = terminal.error();
  } else if (!terminal->has_value()) {
    observation->normal_end = true;
  } else {
    observation->error = coropact::base::MakeErrno(ECONNABORTED);
  }
  observation->done = true;
}

coropact::coro::DetachedTask PauseThenResume(
    LUringAcceptSource* source,
    LUringLoop* loop,
    PauseResumeObservation* observation) {
  auto first = co_await source->Next();
  if (!first.has_value()) {
    if (IsUnsupported(first.error())) {
      observation->unsupported = true;
    } else {
      observation->error = first.error();
    }
    observation->done = true;
    co_return;
  }
  if (!first->has_value()) {
    observation->error = coropact::base::MakeErrno(ECONNABORTED);
    observation->done = true;
    co_return;
  }
  observation->first_received = true;

  // The test sends the second connection while this coroutine is asleep.
  // With event_capacity == 1 that event fills the source queue and starts
  // the native cancel/terminal-CQE convergence path.
  auto delay = co_await coropact::luring::SleepFor(
      *loop, std::chrono::milliseconds(50));
  if (!delay.has_value()) {
    observation->error = delay.error();
    observation->done = true;
    co_return;
  }

  auto queued = co_await source->Next();
  if (!queued.has_value() || !queued->has_value()) {
    observation->error = queued.has_value()
                             ? coropact::base::MakeErrno(ECONNABORTED)
                             : queued.error();
    observation->done = true;
    co_return;
  }
  observation->low_water_consumed = true;

  // This await must be fulfilled by a connection sent only after the queue
  // crossed the low-water mark. It therefore proves physical re-arm, not
  // merely draining an already queued accept.
  auto resumed = co_await source->Next();
  if (!resumed.has_value() || !resumed->has_value()) {
    observation->error = resumed.has_value()
                             ? coropact::base::MakeErrno(ECONNABORTED)
                             : resumed.error();
    observation->done = true;
    co_return;
  }
  observation->resumed_received = true;

  auto stopped = co_await source->Stop();
  observation->stop_succeeded = stopped.has_value();
  if (!stopped.has_value()) {
    observation->error = stopped.error();
    observation->done = true;
    co_return;
  }

  auto terminal = co_await source->Next();
  if (!terminal.has_value()) {
    observation->error = terminal.error();
  } else if (!terminal->has_value()) {
    observation->normal_end = true;
  } else {
    observation->error = coropact::base::MakeErrno(ECONNABORTED);
  }
  observation->done = true;
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

  auto listener_result =
      LUringListener::Create(&loop, LoopbackAddress(0));
  if (!listener_result.has_value()) {
    std::cout << "FAIL: listener creation failed: "
              << listener_result.error().message() << '\n';
    return false;
  }
  auto listener = std::move(*listener_result);
  auto source_result = listener.AcceptSource();
  if (!source_result.has_value()) {
    std::cout << "FAIL: source creation failed: "
              << source_result.error().message() << '\n';
    return false;
  }
  auto source = std::move(*source_result);

  // Init has already armed the internal wake poll, so the next user submit
  // is the source's initial multishot accept request.
  loop.FailNextSubmissionsForTesting(1, EIO);
  CloseObservation observation;
  coropact::coro::SpawnDetach(
      loop, WaitForSourceEnd(&source, &observation));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  if (!PumpUntil(loop, [&] {
        return observation.done || observation.unsupported ||
               observation.error.has_value();
      })) {
    return false;
  }
  if (observation.unsupported) {
    std::cout << "SKIP: multishot accept unavailable\n";
    return true;
  }
  if (!observation.error.has_value() ||
      observation.error->value() != EIO) {
    std::cout << "FAIL: initial submit failure was not propagated\n";
    return false;
  }
  return true;
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

  auto listener_result =
      LUringListener::Create(&loop, LoopbackAddress(0));
  if (!listener_result.has_value()) {
    std::cout << "FAIL: listener creation failed: "
              << listener_result.error().message() << '\n';
    return false;
  }
  auto listener = std::move(*listener_result);
  auto source_result = listener.AcceptSource();
  if (!source_result.has_value()) {
    std::cout << "FAIL: source creation failed: "
              << source_result.error().message() << '\n';
    return false;
  }
  auto source = std::move(*source_result);

  CloseObservation end_observation;
  StopObservation first_stop;
  coropact::coro::SpawnDetach(
      loop, WaitForSourceEnd(&source, &end_observation));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  // Stop revokes source admission before it attempts the cancel SQE. A local
  // preparation failure returns EIO but deliberately leaves the source in
  // Stopping, so the caller must retain it and retry Stop().
  loop.FailNextSubmissionsForTesting(1, EIO);
  coropact::coro::SpawnDetach(
      loop, StopSource(&source, &first_stop));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  if (!first_stop.done || first_stop.succeeded || !first_stop.error.has_value() ||
      first_stop.error->value() != EIO) {
    std::cout << "FAIL: accept cancel submit failure was not propagated\n";
    return false;
  }

  StopObservation retry_stop;
  coropact::coro::SpawnDetach(
      loop, StopSource(&source, &retry_stop));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  if (!PumpUntil(loop, [&] {
        return end_observation.unsupported ||
               end_observation.error.has_value() ||
               (retry_stop.done && end_observation.done);
      })) {
    return false;
  }
  if (end_observation.unsupported) {
    std::cout << "SKIP: multishot accept unavailable\n";
    return true;
  }
  if (end_observation.error.has_value()) {
    std::cout << "FAIL: source cleanup after cancel submit failure failed: "
              << end_observation.error->message() << '\n';
    return false;
  }

  return retry_stop.succeeded && end_observation.source_end;
}

#endif

bool CheckMultishotAccept() {
  LUringLoop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  auto listener_result =
      LUringListener::Create(&loop, LoopbackAddress(0));
  if (!listener_result.has_value()) {
    std::cout << "FAIL: listener creation failed: "
              << listener_result.error().message() << '\n';
    return false;
  }

  auto listener = std::move(*listener_result);
  auto source_result = listener.AcceptSource({
      .pending_depth = 1,
      .event_capacity = 16,
  });
  if (!source_result.has_value()) {
    std::cout << "FAIL: AcceptSource creation failed: "
              << source_result.error().message() << '\n';
    return false;
  }

  auto source = std::move(*source_result);
  Observation observation;

  coropact::coro::SpawnDetach(
      loop,
      Consume(&source, &observation));

  // Start the consumer and submit the multishot request.
  coropact::luring::detail::LoopAccess::RunReady(loop);

  auto address = listener.LocalAddress();
  if (!address.has_value()) {
    std::cout << "FAIL: LocalAddress failed: "
              << address.error().message() << '\n';
    return false;
  }

  std::vector<UniqueFd> clients;
  clients.reserve(kClientCount);

  for (int i = 0; i < kClientCount; ++i) {
    auto client = ConnectClient(*address);
    if (!client.has_value()) {
      std::cout << "FAIL: client connect failed: "
                << client.error().message() << '\n';
      return false;
    }
    clients.emplace_back(*client);
  }

  if (!PumpUntil(loop, [&] {
        return observation.unsupported || observation.error.has_value() ||
               (observation.stopped && observation.accepted == kClientCount);
      })) {
    return false;
  }

  if (observation.unsupported) {
    std::cout << "SKIP: multishot accept unavailable\n";
    return true;
  }

  if (observation.error.has_value()) {
    std::cout << "FAIL: multishot accept failed: "
              << observation.error->message() << '\n';
    return false;
  }

  return observation.accepted == kClientCount && observation.stopped;
}

bool CheckStopCancelsActiveSource() {
  LUringLoop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  auto listener_result =
      LUringListener::Create(&loop, LoopbackAddress(0));
  if (!listener_result.has_value()) {
    std::cout << "FAIL: listener creation failed: "
              << listener_result.error().message() << '\n';
    return false;
  }
  auto listener = std::move(*listener_result);
  auto source_result = listener.AcceptSource({.pending_depth = 1,
                                              .event_capacity = 4});
  if (!source_result.has_value()) {
    std::cout << "FAIL: source creation failed: "
              << source_result.error().message() << '\n';
    return false;
  }
  auto source = std::move(*source_result);

  CancelObservation observation;
  coropact::coro::SpawnDetach(
      loop, ConsumeOneThenStop(&source, &observation));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  auto address = listener.LocalAddress();
  if (!address.has_value()) {
    std::cout << "FAIL: LocalAddress failed: "
              << address.error().message() << '\n';
    return false;
  }
  auto client = ConnectClient(*address);
  if (!client.has_value()) {
    std::cout << "FAIL: client connect failed: "
              << client.error().message() << '\n';
    return false;
  }
  UniqueFd client_fd(*client);

  if (!PumpUntil(loop, [&] {
        return observation.unsupported || observation.error.has_value() ||
               observation.done;
      })) {
    return false;
  }
  if (observation.unsupported) {
    std::cout << "SKIP: multishot accept unavailable\n";
    return true;
  }
  if (observation.error.has_value()) {
    std::cout << "FAIL: active-source cancellation failed: "
              << observation.error->message() << '\n';
    return false;
  }

  return observation.accepted && observation.stopped && observation.terminal;
}

bool CheckListenerCloseCancelsActiveSource() {
  LUringLoop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  auto listener_result =
      LUringListener::Create(&loop, LoopbackAddress(0));
  if (!listener_result.has_value()) {
    std::cout << "FAIL: listener creation failed: "
              << listener_result.error().message() << '\n';
    return false;
  }
  auto listener = std::move(*listener_result);
  auto source_result = listener.AcceptSource();
  if (!source_result.has_value()) {
    std::cout << "FAIL: source creation failed: "
              << source_result.error().message() << '\n';
    return false;
  }
  auto source = std::move(*source_result);

  CloseObservation observation;
  coropact::coro::SpawnDetach(
      loop, WaitForSourceEnd(&source, &observation));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  coropact::coro::SpawnDetach(
      loop, CloseListener(&listener, &observation));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  if (!PumpUntil(loop, [&] {
        return observation.unsupported || observation.error.has_value() ||
               (observation.done && observation.close_succeeded);
      })) {
    return false;
  }
  if (observation.unsupported) {
    std::cout << "SKIP: multishot accept unavailable\n";
    return true;
  }
  if (observation.error.has_value()) {
    std::cout << "FAIL: listener close failed: "
              << observation.error->message() << '\n';
    return false;
  }

  return observation.source_end && observation.close_succeeded;
}

bool CheckQueueBackpressure() {
  LUringLoop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  auto listener_result =
      LUringListener::Create(&loop, LoopbackAddress(0));
  if (!listener_result.has_value()) {
    std::cout << "FAIL: listener creation failed: "
              << listener_result.error().message() << '\n';
    return false;
  }
  auto listener = std::move(*listener_result);
  auto source_result = listener.AcceptSource({.pending_depth = 1,
                                              .event_capacity = 1});
  if (!source_result.has_value()) {
    std::cout << "FAIL: source creation failed: "
              << source_result.error().message() << '\n';
    return false;
  }
  auto source = std::move(*source_result);

  BackpressureObservation observation;
  coropact::coro::SpawnDetach(
      loop, FillQueueThenStop(&source, &loop, &observation));
  coropact::luring::detail::LoopAccess::RunReady(loop);

  auto address = listener.LocalAddress();
  if (!address.has_value()) {
    std::cout << "FAIL: LocalAddress failed: "
              << address.error().message() << '\n';
    return false;
  }

  // First connection wakes the pending Next. The remaining burst is sent
  // while the consumer is sleeping, so the one-event queue pauses admission.
  std::vector<UniqueFd> clients;
  clients.reserve(8);
  auto first_client = ConnectClient(*address);
  if (!first_client.has_value()) {
    std::cout << "FAIL: first client connect failed: "
              << first_client.error().message() << '\n';
    return false;
  }
  clients.emplace_back(*first_client);

  if (!PumpUntil(loop, [&] {
        return observation.unsupported || observation.error.has_value() ||
               observation.first_received;
      })) {
    return false;
  }
  if (observation.unsupported) {
    std::cout << "SKIP: multishot accept unavailable\n";
    return true;
  }
  if (observation.error.has_value()) {
    std::cout << "FAIL: first accept failed: "
              << observation.error->message() << '\n';
    return false;
  }

  for (int i = 0; i < 7; ++i) {
    auto client = ConnectClient(*address);
    if (!client.has_value()) {
      std::cout << "FAIL: burst client connect failed: "
                << client.error().message() << '\n';
      return false;
    }
    clients.emplace_back(*client);
  }

  if (!PumpUntil(loop, [&] {
        return observation.unsupported || observation.error.has_value() ||
               observation.done;
      }, 128)) {
    return false;
  }
  if (observation.unsupported) {
    std::cout << "SKIP: multishot accept unavailable\n";
    return true;
  }
  if (observation.error.has_value()) {
    std::cout << "FAIL: queue backpressure path failed: "
              << observation.error->message() << '\n';
    return false;
  }

  return observation.first_received &&
         observation.queued_received &&
         observation.stop_succeeded &&
         observation.normal_end;
}

bool RunQueuePauseThenRearmScenario(bool inject_close_submit_failure) {
  LUringLoop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  auto listener_result = LUringListener::Create(&loop, LoopbackAddress(0));
  if (!listener_result.has_value()) {
    std::cout << "FAIL: listener creation failed: "
              << listener_result.error().message() << '\n';
    return false;
  }
  auto listener = std::move(*listener_result);
  auto source_result = listener.AcceptSource({.pending_depth = 1,
                                              .event_capacity = 1});
  if (!source_result.has_value()) {
    std::cout << "FAIL: source creation failed: "
              << source_result.error().message() << '\n';
    return false;
  }
  auto source = std::move(*source_result);

  PauseResumeObservation observation;
  coropact::coro::SpawnDetach(loop, PauseThenResume(&source, &loop, &observation));
  coropact::luring::detail::LoopAccess::RunReady(loop);

#if defined(COROPACT_ENABLE_TEST_HOOKS)
  if (inject_close_submit_failure) {
    // Close preparation has not committed until its cancel SQE is accepted.
    // A local failure must leave the active source in Active, so the same
    // high-water pause and low-water re-arm trace remains possible below.
    loop.FailNextSubmissionsForTesting(1, EIO);
    CloseObservation close_observation;
    coropact::coro::SpawnDetach(
        loop, CloseListener(&listener, &close_observation));
    coropact::luring::detail::LoopAccess::RunReady(loop);

    if (close_observation.close_succeeded ||
        !close_observation.error.has_value() ||
        close_observation.error->value() != EIO) {
      std::cout << "FAIL: listener close preparation failure did not return EIO\n";
      return false;
    }
  }
#else
  (void)(inject_close_submit_failure);
#endif

  auto address = listener.LocalAddress();
  if (!address.has_value()) {
    std::cout << "FAIL: LocalAddress failed: "
              << address.error().message() << '\n';
    return false;
  }

  std::vector<UniqueFd> clients;
  clients.reserve(3);
  auto connect = [&]() -> bool {
    auto client = ConnectClient(*address);
    if (!client.has_value()) {
      std::cout << "FAIL: client connect failed: "
                << client.error().message() << '\n';
      return false;
    }
    clients.emplace_back(*client);
    return true;
  };

  if (!connect()) {
    return false;
  }
  if (!PumpUntil(loop, [&] {
        return observation.unsupported || observation.error.has_value() ||
               observation.first_received;
      })) {
    return false;
  }
  if (observation.unsupported) {
    std::cout << "SKIP: multishot accept unavailable\n";
    return true;
  }
  if (observation.error.has_value()) {
    std::cout << "FAIL: first accept failed: "
              << observation.error->message() << '\n';
    return false;
  }

  if (!connect()) {
    return false;
  }
  if (!PumpUntil(loop, [&] {
        return observation.unsupported || observation.error.has_value() ||
               observation.low_water_consumed;
      }, 128)) {
    return false;
  }
  if (observation.unsupported) {
    std::cout << "SKIP: multishot accept unavailable\n";
    return true;
  }
  if (observation.error.has_value()) {
    std::cout << "FAIL: source did not drain to low-water: "
              << observation.error->message() << '\n';
    return false;
  }

  if (!connect()) {
    return false;
  }
  if (!PumpUntil(loop, [&] {
        return observation.unsupported || observation.error.has_value() ||
               observation.done;
      }, 128)) {
    return false;
  }
  if (observation.unsupported) {
    std::cout << "SKIP: multishot accept unavailable\n";
    return true;
  }
  if (observation.error.has_value()) {
    std::cout << "FAIL: source did not re-arm after low-water: "
              << observation.error->message() << '\n';
    return false;
  }

  return observation.first_received && observation.low_water_consumed &&
         observation.resumed_received && observation.stop_succeeded &&
         observation.normal_end;
}

bool CheckQueuePauseThenRearm() {
  return RunQueuePauseThenRearmScenario(false);
}

#if defined(COROPACT_ENABLE_TEST_HOOKS)
bool CheckListenerCloseSubmitFailurePreservesActiveSource() {
  return RunQueuePauseThenRearmScenario(true);
}
#endif

}  // namespace

int main() {
  CheckCompletionEvent();
  CheckFakeRearmSubmitFailure();
  CheckFakeMultishotFallback();
#if defined(COROPACT_ENABLE_TEST_HOOKS)
  if (!CheckInitialSubmitFailure()) {
    return 1;
  }
  if (!CheckCancelSubmitFailure()) {
    return 1;
  }
  if (!CheckListenerCloseSubmitFailurePreservesActiveSource()) {
    return 1;
  }
#endif
  if (!CheckMultishotAccept()) {
    return 1;
  }
  if (!CheckStopCancelsActiveSource()) {
    return 1;
  }
  if (!CheckListenerCloseCancelsActiveSource()) {
    return 1;
  }
  if (!CheckQueueBackpressure()) {
    return 1;
  }
  if (!CheckQueuePauseThenRearm()) {
    return 1;
  }

  std::cout << "luring multishot accept/state smoke: PASS\n";
  return 0;
}
