// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
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

#include "alyrn/result.h"
#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/uring/listener.h"
#include "alyrn/uring/loop.h"
#include "alyrn/detail/uring/loop_access.h"
#include "alyrn/uring/options.h"
#include "alyrn/uring/timer.h"
#include "alyrn/net/accept_source.h"
#include "alyrn/net/endpoint.h"

namespace {

using alyrn::Error;
using alyrn::Result;
using alyrn::uring::AcceptSource;
using alyrn::uring::detail::CompletionEvent;
using alyrn::uring::Listener;
using alyrn::uring::Loop;
using alyrn::uring::Options;
using alyrn::net::Endpoint;
using alyrn::net::detail::AcceptSourceState;
using alyrn::net::detail::AcceptSourceStateMachine;
using alyrn::net::detail::EventDisposition;
using alyrn::net::detail::MultishotRequestDisposition;

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
  bool options_applied{false};
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

LoopInitStatus InitLoop(Loop& loop) {
  Options options;
  options.entries = 32;

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
bool PumpUntil(Loop& loop, Predicate&& predicate, int max_iterations = 64) {
  for (int i = 0; i < max_iterations && !predicate(); ++i) {
    auto completed = alyrn::uring::detail::LoopAccess::WaitCompletions(loop);
    if (!completed.has_value()) {
      std::cout << "FAIL: waiting for CQE failed: "
                << completed.error().message() << '\n';
      return false;
    }
    alyrn::uring::detail::LoopAccess::RunReady(loop);
  }
  return predicate();
}

Result<int> ConnectClient(const Endpoint& address) {
  const int fd = ::socket(
      AF_INET,
      SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
      IPPROTO_TCP);
  if (fd < 0) {
    return std::unexpected(alyrn::CurrentErrno());
  }

  const int result = ::connect(
      fd,
      address.SockAddr(),
      address.SockAddrLen());

  if (result < 0 && errno != EINPROGRESS) {
    const auto error = alyrn::CurrentErrno();
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
  source.FailNextSubmission(alyrn::Errno(EIO));
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

alyrn::coro::DetachedTask Consume(
    AcceptSource* source,
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
      observation->error = alyrn::Errno(ECONNABORTED);
      co_return;
    }

    if (observation->accepted == 0) {
      const int accepted_fd = result->value().Fd();
      int no_delay = 0;
      auto no_delay_length = static_cast<socklen_t>(sizeof(no_delay));
      int keep_alive = 0;
      auto keep_alive_length = static_cast<socklen_t>(sizeof(keep_alive));
      observation->options_applied =
          ::getsockopt(accepted_fd, IPPROTO_TCP, TCP_NODELAY, &no_delay, &no_delay_length) == 0 &&
          no_delay == 1 &&
          ::getsockopt(accepted_fd, SOL_SOCKET, SO_KEEPALIVE, &keep_alive,
                       &keep_alive_length) == 0 &&
          keep_alive == 1;
    }

    ++observation->accepted;
  }

  auto stopped = co_await source->Stop();
  observation->stopped = stopped.has_value();
  if (!stopped.has_value()) {
    observation->error = stopped.error();
  }
}

alyrn::coro::DetachedTask ConsumeOneThenStop(
    AcceptSource* source,
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
    observation->error = alyrn::Errno(ECONNABORTED);
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

alyrn::coro::DetachedTask WaitForSourceEnd(
    AcceptSource* source,
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

alyrn::coro::DetachedTask CloseListener(
    Listener* listener,
    CloseObservation* observation) {
  auto result = co_await listener->Close();
  observation->close_succeeded = result.has_value();
  if (!result.has_value()) {
    observation->error = result.error();
  }
}

/* Only the test-hook scenarios drive this directly. */

alyrn::coro::DetachedTask FillQueueThenStop(
    AcceptSource* source,
    Loop* loop,
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
    observation->error = alyrn::Errno(ECONNABORTED);
    observation->done = true;
    co_return;
  }
  observation->first_received = true;

  // Leave time for a burst to place one event in the bounded queue and a
  // later F_MORE CQE to hit the full-queue path.
  auto delay = co_await alyrn::uring::SleepFor(
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
    observation->error = alyrn::Errno(ECONNABORTED);
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
    observation->error = alyrn::Errno(ECONNABORTED);
  }
  observation->done = true;
}

alyrn::coro::DetachedTask PauseThenResume(
    AcceptSource* source,
    Loop* loop,
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
    observation->error = alyrn::Errno(ECONNABORTED);
    observation->done = true;
    co_return;
  }
  observation->first_received = true;

  // The test sends the second connection while this coroutine is asleep.
  // With event_capacity == 1 that event fills the source queue and starts
  // the native cancel/terminal-CQE convergence path.
  auto delay = co_await alyrn::uring::SleepFor(
      *loop, std::chrono::milliseconds(50));
  if (!delay.has_value()) {
    observation->error = delay.error();
    observation->done = true;
    co_return;
  }

  auto queued = co_await source->Next();
  if (!queued.has_value() || !queued->has_value()) {
    observation->error = queued.has_value()
                             ? alyrn::Errno(ECONNABORTED)
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
                             ? alyrn::Errno(ECONNABORTED)
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
    observation->error = alyrn::Errno(ECONNABORTED);
  }
  observation->done = true;
}


bool CheckMultishotAccept() {
  Loop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  alyrn::uring::ListenOptions options;
  options.tcp_options.no_delay = true;
  options.tcp_options.keep_alive = true;
  auto listener_result = Listener::Create(&loop, LoopbackAddress(0), options);
  if (!listener_result.has_value()) {
    std::cout << "FAIL: listener creation failed: "
              << listener_result.error().message() << '\n';
    return false;
  }

  auto listener = std::move(*listener_result);
  auto source_result = listener.CreateAcceptSource({
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

  alyrn::coro::SpawnDetach(
      loop,
      Consume(&source, &observation));

  // Start the consumer and submit the multishot request.
  alyrn::uring::detail::LoopAccess::RunReady(loop);

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

  return observation.accepted == kClientCount && observation.stopped &&
         observation.options_applied;
}

bool CheckStopCancelsActiveSource() {
  Loop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  auto listener_result =
      Listener::Create(&loop, LoopbackAddress(0));
  if (!listener_result.has_value()) {
    std::cout << "FAIL: listener creation failed: "
              << listener_result.error().message() << '\n';
    return false;
  }
  auto listener = std::move(*listener_result);
  auto source_result = listener.CreateAcceptSource({.pending_depth = 1,
                                              .event_capacity = 4});
  if (!source_result.has_value()) {
    std::cout << "FAIL: source creation failed: "
              << source_result.error().message() << '\n';
    return false;
  }
  auto source = std::move(*source_result);

  CancelObservation observation;
  alyrn::coro::SpawnDetach(
      loop, ConsumeOneThenStop(&source, &observation));
  alyrn::uring::detail::LoopAccess::RunReady(loop);

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
  Loop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  auto listener_result =
      Listener::Create(&loop, LoopbackAddress(0));
  if (!listener_result.has_value()) {
    std::cout << "FAIL: listener creation failed: "
              << listener_result.error().message() << '\n';
    return false;
  }
  auto listener = std::move(*listener_result);
  auto source_result = listener.CreateAcceptSource();
  if (!source_result.has_value()) {
    std::cout << "FAIL: source creation failed: "
              << source_result.error().message() << '\n';
    return false;
  }
  auto source = std::move(*source_result);

  CloseObservation observation;
  alyrn::coro::SpawnDetach(
      loop, WaitForSourceEnd(&source, &observation));
  alyrn::uring::detail::LoopAccess::RunReady(loop);

  alyrn::coro::SpawnDetach(
      loop, CloseListener(&listener, &observation));
  alyrn::uring::detail::LoopAccess::RunReady(loop);

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
  Loop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  auto listener_result =
      Listener::Create(&loop, LoopbackAddress(0));
  if (!listener_result.has_value()) {
    std::cout << "FAIL: listener creation failed: "
              << listener_result.error().message() << '\n';
    return false;
  }
  auto listener = std::move(*listener_result);
  auto source_result = listener.CreateAcceptSource({.pending_depth = 1,
                                              .event_capacity = 1});
  if (!source_result.has_value()) {
    std::cout << "FAIL: source creation failed: "
              << source_result.error().message() << '\n';
    return false;
  }
  auto source = std::move(*source_result);

  BackpressureObservation observation;
  alyrn::coro::SpawnDetach(
      loop, FillQueueThenStop(&source, &loop, &observation));
  alyrn::uring::detail::LoopAccess::RunReady(loop);

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

bool RunQueuePauseThenRearmScenario() {
  Loop loop;
  switch (InitLoop(loop)) {
    case LoopInitStatus::kReady:
      break;
    case LoopInitStatus::kSkip:
      return true;
    case LoopInitStatus::kFail:
      return false;
  }

  auto listener_result = Listener::Create(&loop, LoopbackAddress(0));
  if (!listener_result.has_value()) {
    std::cout << "FAIL: listener creation failed: "
              << listener_result.error().message() << '\n';
    return false;
  }
  auto listener = std::move(*listener_result);
  auto source_result = listener.CreateAcceptSource({.pending_depth = 1,
                                              .event_capacity = 1});
  if (!source_result.has_value()) {
    std::cout << "FAIL: source creation failed: "
              << source_result.error().message() << '\n';
    return false;
  }
  auto source = std::move(*source_result);

  PauseResumeObservation observation;
  alyrn::coro::SpawnDetach(loop, PauseThenResume(&source, &loop, &observation));
  alyrn::uring::detail::LoopAccess::RunReady(loop);


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
  return RunQueuePauseThenRearmScenario();
}


}  // namespace

int main() {
  CheckCompletionEvent();
  CheckFakeRearmSubmitFailure();
  CheckFakeMultishotFallback();
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
