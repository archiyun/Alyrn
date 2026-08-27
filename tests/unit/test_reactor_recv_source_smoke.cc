// SPDX-License-Identifier: MIT

#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstring>
#include <expected>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "alyrn/backend/recv_source.h"
#include "alyrn/result.h"
#include "alyrn/coro/awaitable.h"
#include "alyrn/coro/detached_task.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/io/recv_source.h"
#include "alyrn/reactor/loop.h"
#include "alyrn/reactor/recv_source.h"

namespace {

using alyrn::Error;
using alyrn::coro::DetachedTask;
using alyrn::reactor::Loop;
using alyrn::reactor::RecvSource;
using alyrn::reactor::RecvSourceOptions;

static_assert(alyrn::io::AsyncRecvSource<RecvSource>);
static_assert(alyrn::coro::Awaiter<decltype(std::declval<RecvSource&>().Next())>);

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

bool MakeSocketPair(int fds[2]) {
  return ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) == 0;
}

bool MakeDatagramSocketPair(int fds[2]) {
  return ::socketpair(AF_UNIX, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) == 0;
}

std::string BytesToString(const alyrn::net::RecvEvent& event) {
  const auto bytes = event.buffer.Bytes();
  return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

DetachedTask ReceiveOne(RecvSource* source, Loop* loop,
                        std::optional<RecvSource::NextResult>* result, std::string* payload,
                        bool* received_event, bool* stop_succeeded) {
  auto received = co_await source->Next();
  if (received.has_value() && received->has_value()) {
    *received_event = true;
    *payload = BytesToString(**received);
    // The source cannot finish while this lease is alive. Release it before
    // awaiting Stop so the fixed buffer can be returned to the pool.
    received->reset();
  }
  result->emplace(std::move(received));

  auto stopped = co_await source->Stop();
  *stop_succeeded = stopped.has_value();
  loop->RequestStop();
}

DetachedTask ReceivePending(RecvSource* source, Loop* loop,
                            std::optional<RecvSource::NextResult>* result, bool* received_event,
                            bool* stop_succeeded) {
  auto received = co_await source->Next();
  if (received.has_value() && received->has_value()) {
    *received_event = true;
    received->reset();
  }
  result->emplace(std::move(received));
  auto stopped = co_await source->Stop();
  *stop_succeeded = stopped.has_value();
  loop->RequestStop();
}

DetachedTask WaitForEnd(RecvSource* source, Loop* loop,
                        std::optional<RecvSource::NextResult>* result) {
  result->emplace(co_await source->Next());
  loop->RequestStop();
}

DetachedTask StopOnly(RecvSource* source, bool* stop_succeeded) {
  auto stopped = co_await source->Stop();
  *stop_succeeded = stopped.has_value();
}

DetachedTask StopThenObserveTerminalAfterLoopStop(
    RecvSource* source, Loop* loop, std::optional<alyrn::Result<void>>* stop,
    std::optional<RecvSource::NextResult>* terminal, bool* with_scheduler) {
  stop->emplace(co_await source->Stop());
  loop->RequestStop();
  terminal->emplace(co_await source->Next());
  *with_scheduler = alyrn::coro::Scheduler::TryCurrent() == loop;
}

DetachedTask ReceiveTwo(RecvSource* source, Loop* loop, int sender,
                        std::array<std::string, 2>* payloads, bool* stop_succeeded) {
  auto first = co_await source->Next();
  if (!first.has_value() || !first->has_value()) {
    loop->RequestStop();
    co_return;
  }
  (*payloads)[0] = BytesToString(**first);
  first->reset();

  // Write the second record after the first lease has been returned. This
  // keeps the two logical events distinct even on a stream socket.
  loop->RunAfter(alyrn::time::Duration::zero(), [sender] {
    constexpr std::string_view kSecond = "second";
    (void)::send(sender, kSecond.data(), kSecond.size(), MSG_NOSIGNAL);
  });

  auto second = co_await source->Next();
  if (!second.has_value() || !second->has_value()) {
    loop->RequestStop();
    co_return;
  }
  (*payloads)[1] = BytesToString(**second);
  second->reset();

  auto stopped = co_await source->Stop();
  *stop_succeeded = stopped.has_value();
  loop->RequestStop();
}

DetachedTask HoldLeaseThenStop(RecvSource* source, Loop* loop,
                               std::optional<alyrn::net::RecvEvent>* held, bool* stop_started,
                               bool* stop_succeeded) {
  auto received = co_await source->Next();
  if (!received.has_value() || !received->has_value()) {
    loop->RequestStop();
    co_return;
  }
  held->emplace(std::move(**received));

  *stop_started = true;
  auto stopped = co_await source->Stop();
  *stop_succeeded = stopped.has_value();
  loop->RequestStop();
}

struct PauseResumeObservation {
  bool first_received{false};
  bool low_water_consumed{false};
  bool resumed_received{false};
  bool stopped{false};
  bool done{false};
  bool timed_out{false};
  bool send_failed{false};
  std::string first;
  std::string queued;
  std::string resumed;
  std::optional<Error> error;
};

bool TakeRecvEvent(RecvSource::NextResult received, PauseResumeObservation* observation,
                   std::string* payload) {
  if (!received.has_value()) {
    observation->error = received.error();
    return false;
  }
  if (!received->has_value()) {
    observation->error = alyrn::Errno(ECONNRESET);
    return false;
  }

  *payload = BytesToString(**received);
  received->reset();
  return true;
}

DetachedTask ReceiveFirstForPause(RecvSource* source, Loop* loop,
                                  PauseResumeObservation* observation) {
  auto first = co_await source->Next();
  if (!TakeRecvEvent(std::move(first), observation, &observation->first)) {
    observation->done = true;
    loop->RequestStop();
    co_return;
  }
  observation->first_received = true;
}

DetachedTask DrainPausedSource(RecvSource* source, Loop* loop,
                               PauseResumeObservation* observation) {
  auto queued = co_await source->Next();
  if (!TakeRecvEvent(std::move(queued), observation, &observation->queued)) {
    observation->done = true;
    loop->RequestStop();
    co_return;
  }
  observation->low_water_consumed = true;

  auto resumed = co_await source->Next();
  if (!TakeRecvEvent(std::move(resumed), observation, &observation->resumed)) {
    observation->done = true;
    loop->RequestStop();
    co_return;
  }
  observation->resumed_received = true;

  auto stopped = co_await source->Stop();
  if (!stopped.has_value()) {
    observation->error = stopped.error();
  } else {
    observation->stopped = true;
  }
  observation->done = true;
  loop->RequestStop();
}

DetachedTask StopSourceOnTimeout(RecvSource* source, Loop* loop) {
  (void)(co_await source->Stop());
  loop->RequestStop();
}

bool CheckImmediateReceive() {
  int fds[2] = {-1, -1};
  if (!Check(MakeSocketPair(fds), "socketpair failed")) {
    return false;
  }

  constexpr std::string_view kPayload = "reactor-immediate";
  const auto sent = ::send(fds[1], kPayload.data(), kPayload.size(), MSG_NOSIGNAL);
  if (!Check(sent == static_cast<ssize_t>(kPayload.size()), "initial send failed")) {
    ::close(fds[0]);
    ::close(fds[1]);
    return false;
  }

  Loop loop;
  auto source_result = RecvSource::Create(&loop, fds[0]);
  if (!Check(source_result.has_value(), "immediate source creation failed")) {
    ::close(fds[0]);
    ::close(fds[1]);
    return false;
  }
  auto source = std::move(*source_result);

  std::optional<RecvSource::NextResult> result;
  std::string payload;
  bool received_event = false;
  bool stop_succeeded = false;
  alyrn::coro::SpawnDetach(
      loop, ReceiveOne(&source, &loop, &result, &payload, &received_event, &stop_succeeded));
  loop.Run();

  ::close(fds[1]);
  ::close(fds[0]);
  return Check(result.has_value(), "immediate receive did not finish") &&
         Check(result->has_value(), "immediate receive returned an error") &&
         Check(received_event, "immediate receive returned EOF") &&
         Check(payload == kPayload, "immediate receive payload mismatch") &&
         Check(stop_succeeded, "immediate receive Stop failed");
}

bool CheckPendingReceive() {
  int fds[2] = {-1, -1};
  if (!Check(MakeSocketPair(fds), "socketpair failed")) {
    return false;
  }

  Loop loop;
  auto source_result = RecvSource::Create(&loop, fds[0]);
  if (!Check(source_result.has_value(), "pending source creation failed")) {
    ::close(fds[0]);
    ::close(fds[1]);
    return false;
  }
  auto source = std::move(*source_result);

  std::optional<RecvSource::NextResult> result;
  bool received_event = false;
  bool stop_succeeded = false;
  alyrn::coro::SpawnDetach(
      loop, ReceivePending(&source, &loop, &result, &received_event, &stop_succeeded));
  loop.RunAfter(alyrn::time::Duration::zero(), [sender = fds[1]] {
    constexpr std::string_view kPayload = "reactor-pending";
    (void)::send(sender, kPayload.data(), kPayload.size(), MSG_NOSIGNAL);
  });
  loop.Run();

  ::close(fds[1]);
  ::close(fds[0]);
  return Check(result.has_value(), "pending receive did not finish") &&
         Check(result->has_value(), "pending receive returned an error") &&
         Check(received_event, "pending receive returned EOF") &&
         Check(!result->value().has_value(), "pending receive retained a released lease") &&
         Check(stop_succeeded, "pending receive Stop failed");
}

bool CheckEof() {
  int fds[2] = {-1, -1};
  if (!Check(MakeSocketPair(fds), "socketpair failed")) {
    return false;
  }

  Loop loop;
  auto source_result = RecvSource::Create(&loop, fds[0]);
  if (!Check(source_result.has_value(), "EOF source creation failed")) {
    ::close(fds[0]);
    ::close(fds[1]);
    return false;
  }
  auto source = std::move(*source_result);
  ::close(fds[1]);
  fds[1] = -1;

  std::optional<RecvSource::NextResult> result;
  std::string payload;
  bool received_event = false;
  bool stop_succeeded = false;
  alyrn::coro::SpawnDetach(
      loop, ReceiveOne(&source, &loop, &result, &payload, &received_event, &stop_succeeded));
  loop.Run();

  ::close(fds[0]);
  return Check(result.has_value(), "EOF receive did not finish") &&
         Check(result->has_value(), "EOF receive returned an error") &&
         Check(!received_event, "EOF receive unexpectedly produced data") &&
         Check(!result->value().has_value(), "EOF receive returned an event") &&
         Check(stop_succeeded, "EOF receive Stop failed");
}

bool CheckQueuedEvents() {
  int fds[2] = {-1, -1};
  if (!Check(MakeSocketPair(fds), "socketpair failed")) {
    return false;
  }

  Loop loop;
  RecvSourceOptions options;
  options.source.event_capacity = 2;
  options.source.buffer_capacity = 2;
  options.buffer_size = 32;
  auto source_result = RecvSource::Create(&loop, fds[0], options);
  if (!Check(source_result.has_value(), "queued source creation failed")) {
    ::close(fds[0]);
    ::close(fds[1]);
    return false;
  }
  auto source = std::move(*source_result);

  std::array<std::string, 2> payloads;
  bool stop_succeeded = false;
  constexpr std::string_view kFirst = "first";
  const auto sent = ::send(fds[1], kFirst.data(), kFirst.size(), MSG_NOSIGNAL);
  if (!Check(sent == static_cast<ssize_t>(kFirst.size()), "first queued send failed")) {
    ::close(fds[0]);
    ::close(fds[1]);
    return false;
  }

  alyrn::coro::SpawnDetach(loop, ReceiveTwo(&source, &loop, fds[1], &payloads, &stop_succeeded));
  loop.Run();

  ::close(fds[1]);
  ::close(fds[0]);
  return Check(payloads[0] == kFirst, "first queued event payload mismatch") &&
         Check(payloads[1] == "second", "second queued event payload mismatch") &&
         Check(stop_succeeded, "queued event Stop failed");
}

bool CheckStopWaitsForLease() {
  int fds[2] = {-1, -1};
  if (!Check(MakeSocketPair(fds), "socketpair failed")) {
    return false;
  }

  constexpr std::string_view kPayload = "held-lease";
  const auto sent = ::send(fds[1], kPayload.data(), kPayload.size(), MSG_NOSIGNAL);
  if (!Check(sent == static_cast<ssize_t>(kPayload.size()), "lease send failed")) {
    ::close(fds[0]);
    ::close(fds[1]);
    return false;
  }

  Loop loop;
  auto source_result = RecvSource::Create(&loop, fds[0]);
  if (!Check(source_result.has_value(), "lease source creation failed")) {
    ::close(fds[0]);
    ::close(fds[1]);
    return false;
  }
  auto source = std::move(*source_result);

  std::optional<alyrn::net::RecvEvent> held;
  bool stop_started = false;
  bool stop_succeeded = false;
  alyrn::coro::SpawnDetach(
      loop, HoldLeaseThenStop(&source, &loop, &held, &stop_started, &stop_succeeded));
  loop.RunAfter(alyrn::time::Milliseconds(1), [&held] { held.reset(); });
  loop.Run();

  ::close(fds[1]);
  ::close(fds[0]);
  return Check(stop_started, "Stop was not started while lease was held") &&
         Check(!held.has_value(), "held lease was not released") &&
         Check(stop_succeeded, "Stop did not finish after lease release");
}

bool CheckStopWakesPendingNext() {
  int fds[2] = {-1, -1};
  if (!Check(MakeSocketPair(fds), "socketpair failed")) {
    return false;
  }

  Loop loop;
  auto source_result = RecvSource::Create(&loop, fds[0]);
  if (!Check(source_result.has_value(), "stop source creation failed")) {
    ::close(fds[0]);
    ::close(fds[1]);
    return false;
  }
  auto source = std::move(*source_result);

  std::optional<RecvSource::NextResult> result;
  bool stop_succeeded = false;
  alyrn::coro::SpawnDetach(loop, WaitForEnd(&source, &loop, &result));
  loop.RunAfter(alyrn::time::Duration::zero(),
                [&] { alyrn::coro::SpawnDetach(loop, StopOnly(&source, &stop_succeeded)); });
  loop.Run();

  ::close(fds[1]);
  ::close(fds[0]);
  return Check(result.has_value(), "Stop did not wake pending Next") &&
         Check(result->has_value(), "Stop wake returned an error") &&
         Check(!result->value().has_value(), "Stop wake did not return EOF") &&
         Check(stop_succeeded, "pending Next Stop failed");
}

bool CheckTerminalNextAfterLoopStop() {
  int fds[2] = {-1, -1};
  if (!Check(MakeSocketPair(fds), "socketpair failed")) {
    return false;
  }

  Loop loop;
  auto source_result = RecvSource::Create(&loop, fds[0]);
  if (!Check(source_result.has_value(), "terminal source creation failed")) {
    ::close(fds[0]);
    ::close(fds[1]);
    return false;
  }
  auto source = std::move(*source_result);

  std::optional<alyrn::Result<void>> stop;
  std::optional<RecvSource::NextResult> terminal;
  bool with_scheduler = false;
  alyrn::coro::SpawnDetach(loop, StopThenObserveTerminalAfterLoopStop(
                                        &source, &loop, &stop, &terminal, &with_scheduler));
  loop.Run();

  ::close(fds[1]);
  ::close(fds[0]);
  return Check(stop.has_value() && stop->has_value(), "terminal source Stop failed") &&
         Check(terminal.has_value() && terminal->has_value() && !terminal->value().has_value(),
               "terminal recv Next changed after loop stop was requested") &&
         Check(with_scheduler, "terminal recv Next lost scheduler affinity");
}

bool CheckQueuePauseThenRearm() {
  int fds[2] = {-1, -1};
  if (!Check(MakeDatagramSocketPair(fds), "datagram socketpair failed")) {
    return false;
  }

  Loop loop;
  RecvSourceOptions options;
  options.source.pending_depth = 1;
  options.source.event_capacity = 1;
  options.source.buffer_capacity = 2;
  options.buffer_size = 256;
  auto source_result = RecvSource::Create(&loop, fds[0], options);
  if (!Check(source_result.has_value(), "pause source creation failed")) {
    ::close(fds[0]);
    ::close(fds[1]);
    return false;
  }
  auto source = std::move(*source_result);

  PauseResumeObservation observation;
  const auto send_payload = [sender = fds[1], &observation](std::string_view payload) {
    if (::send(sender, payload.data(), payload.size(), MSG_NOSIGNAL) !=
        static_cast<ssize_t>(payload.size())) {
      observation.send_failed = true;
      observation.error = alyrn::CurrentErrno();
    }
  };

  alyrn::coro::SpawnDetach(loop, ReceiveFirstForPause(&source, &loop, &observation));
  loop.RunAfter(alyrn::time::Duration::zero(), [send_payload] { send_payload("first"); });
  loop.RunAfter(alyrn::time::Milliseconds(20), [send_payload] { send_payload("queued"); });
  loop.RunAfter(alyrn::time::Milliseconds(50), [&] {
    alyrn::coro::SpawnDetach(loop, DrainPausedSource(&source, &loop, &observation));
  });
  loop.RunAfter(alyrn::time::Milliseconds(70), [send_payload] { send_payload("resumed"); });
  loop.RunAfter(alyrn::time::Milliseconds(500), [&] {
    if (!observation.done) {
      observation.timed_out = true;
      alyrn::coro::SpawnDetach(loop, StopSourceOnTimeout(&source, &loop));
    }
  });
  loop.Run();

  ::close(fds[1]);
  ::close(fds[0]);
  return Check(!observation.timed_out, "paused recv source timed out") &&
         Check(!observation.send_failed, "paused recv source send failed") &&
         Check(!observation.error.has_value(), "paused recv source returned an error") &&
         Check(observation.first_received, "paused recv source did not receive first event") &&
         Check(observation.low_water_consumed,
               "paused recv source did not consume the queued event") &&
         Check(observation.resumed_received, "paused recv source did not re-arm after low-water") &&
         Check(observation.first == "first", "paused recv source first payload mismatch") &&
         Check(observation.queued == "queued", "paused recv source queued payload mismatch") &&
         Check(observation.resumed == "resumed", "paused recv source resumed payload mismatch") &&
         Check(observation.stopped, "paused recv source Stop failed");
}

}  // namespace

int main() {
  if (!CheckImmediateReceive() || !CheckPendingReceive() || !CheckEof() || !CheckQueuedEvents() ||
      !CheckStopWaitsForLease() || !CheckStopWakesPendingNext() ||
      !CheckTerminalNextAfterLoopStop() || !CheckQueuePauseThenRearm()) {
    return 1;
  }
  std::cout << "reactor recv source smoke: PASS\n";
  return 0;
}
