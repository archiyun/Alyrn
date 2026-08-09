// Copyright (c) 2026 Arsenova
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

#include "coropact/base/error.h"
#include "coropact/coro/detached_task.h"
#include "coropact/coro/spawn.h"
#include "coropact/io/recv_source.h"
#include "coropact/reactor/loop.h"
#include "coropact/reactor/recv_source.h"

namespace {

using coropact::base::Error;
using coropact::coro::DetachedTask;
using coropact::reactor::EventLoop;
using coropact::reactor::ReactorRecvSource;
using coropact::reactor::ReactorRecvSourceOptions;

static_assert(coropact::io::AsyncRecvSource<ReactorRecvSource>);

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

bool MakeSocketPair(int fds[2]) {
  return ::socketpair(
             AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) == 0;
}

std::string BytesToString(const coropact::net::RecvEvent& event) {
  const auto bytes = event.buffer.Bytes();
  return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

DetachedTask ReceiveOne(
    ReactorRecvSource* source,
    EventLoop* loop,
    std::optional<ReactorRecvSource::Result>* result,
    std::string* payload,
    bool* received_event,
    bool* stop_succeeded) {
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
  loop->Quit();
}

DetachedTask ReceivePending(
    ReactorRecvSource* source,
    EventLoop* loop,
    std::optional<ReactorRecvSource::Result>* result,
    bool* received_event,
    bool* stop_succeeded) {
  auto received = co_await source->Next();
  if (received.has_value() && received->has_value()) {
    *received_event = true;
    received->reset();
  }
  result->emplace(std::move(received));
  auto stopped = co_await source->Stop();
  *stop_succeeded = stopped.has_value();
  loop->Quit();
}

DetachedTask WaitForEnd(
    ReactorRecvSource* source,
    EventLoop* loop,
    std::optional<ReactorRecvSource::Result>* result) {
  result->emplace(co_await source->Next());
  loop->Quit();
}

DetachedTask StopOnly(ReactorRecvSource* source, bool* stop_succeeded) {
  auto stopped = co_await source->Stop();
  *stop_succeeded = stopped.has_value();
}

DetachedTask ReceiveTwo(
    ReactorRecvSource* source,
    EventLoop* loop,
    int sender,
    std::array<std::string, 2>* payloads,
    bool* stop_succeeded) {
  auto first = co_await source->Next();
  if (!first.has_value() || !first->has_value()) {
    loop->Quit();
    co_return;
  }
  (*payloads)[0] = BytesToString(**first);
  first->reset();

  // Write the second record after the first lease has been returned. This
  // keeps the two logical events distinct even on a stream socket.
  loop->RunAfter(0.0, [sender] {
    constexpr std::string_view kSecond = "second";
    (void)::send(sender, kSecond.data(), kSecond.size(), MSG_NOSIGNAL);
  });

  auto second = co_await source->Next();
  if (!second.has_value() || !second->has_value()) {
    loop->Quit();
    co_return;
  }
  (*payloads)[1] = BytesToString(**second);
  second->reset();

  auto stopped = co_await source->Stop();
  *stop_succeeded = stopped.has_value();
  loop->Quit();
}

DetachedTask HoldLeaseThenStop(
    ReactorRecvSource* source,
    EventLoop* loop,
    std::optional<coropact::net::RecvEvent>* held,
    bool* stop_started,
    bool* stop_succeeded) {
  auto received = co_await source->Next();
  if (!received.has_value() || !received->has_value()) {
    loop->Quit();
    co_return;
  }
  held->emplace(std::move(**received));

  *stop_started = true;
  auto stopped = co_await source->Stop();
  *stop_succeeded = stopped.has_value();
  loop->Quit();
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

  EventLoop loop;
  auto source_result = ReactorRecvSource::Create(&loop, fds[0]);
  if (!Check(source_result.has_value(), "immediate source creation failed")) {
    ::close(fds[0]);
    ::close(fds[1]);
    return false;
  }
  auto source = std::move(*source_result);

  std::optional<ReactorRecvSource::Result> result;
  std::string payload;
  bool received_event = false;
  bool stop_succeeded = false;
  coropact::coro::SpawnDetach(
      loop,
      ReceiveOne(&source, &loop, &result, &payload, &received_event,
                 &stop_succeeded));
  loop.Loop();

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

  EventLoop loop;
  auto source_result = ReactorRecvSource::Create(&loop, fds[0]);
  if (!Check(source_result.has_value(), "pending source creation failed")) {
    ::close(fds[0]);
    ::close(fds[1]);
    return false;
  }
  auto source = std::move(*source_result);

  std::optional<ReactorRecvSource::Result> result;
  bool received_event = false;
  bool stop_succeeded = false;
  coropact::coro::SpawnDetach(
      loop,
      ReceivePending(&source, &loop, &result, &received_event,
                     &stop_succeeded));
  loop.RunAfter(0.0, [sender = fds[1]] {
    constexpr std::string_view kPayload = "reactor-pending";
    (void)::send(sender, kPayload.data(), kPayload.size(), MSG_NOSIGNAL);
  });
  loop.Loop();

  ::close(fds[1]);
  ::close(fds[0]);
  return Check(result.has_value(), "pending receive did not finish") &&
         Check(result->has_value(), "pending receive returned an error") &&
         Check(received_event, "pending receive returned EOF") &&
         Check(!result->value().has_value(),
               "pending receive retained a released lease") &&
         Check(stop_succeeded, "pending receive Stop failed");
}

bool CheckEof() {
  int fds[2] = {-1, -1};
  if (!Check(MakeSocketPair(fds), "socketpair failed")) {
    return false;
  }

  EventLoop loop;
  auto source_result = ReactorRecvSource::Create(&loop, fds[0]);
  if (!Check(source_result.has_value(), "EOF source creation failed")) {
    ::close(fds[0]);
    ::close(fds[1]);
    return false;
  }
  auto source = std::move(*source_result);
  ::close(fds[1]);
  fds[1] = -1;

  std::optional<ReactorRecvSource::Result> result;
  std::string payload;
  bool received_event = false;
  bool stop_succeeded = false;
  coropact::coro::SpawnDetach(
      loop,
      ReceiveOne(&source, &loop, &result, &payload, &received_event,
                 &stop_succeeded));
  loop.Loop();

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

  EventLoop loop;
  ReactorRecvSourceOptions options;
  options.source.event_capacity = 2;
  options.source.buffer_capacity = 2;
  options.buffer_size = 32;
  auto source_result = ReactorRecvSource::Create(&loop, fds[0], options);
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

  coropact::coro::SpawnDetach(
      loop, ReceiveTwo(&source, &loop, fds[1], &payloads, &stop_succeeded));
  loop.Loop();

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

  EventLoop loop;
  auto source_result = ReactorRecvSource::Create(&loop, fds[0]);
  if (!Check(source_result.has_value(), "lease source creation failed")) {
    ::close(fds[0]);
    ::close(fds[1]);
    return false;
  }
  auto source = std::move(*source_result);

  std::optional<coropact::net::RecvEvent> held;
  bool stop_started = false;
  bool stop_succeeded = false;
  coropact::coro::SpawnDetach(
      loop,
      HoldLeaseThenStop(&source, &loop, &held, &stop_started, &stop_succeeded));
  loop.RunAfter(0.001, [&held] { held.reset(); });
  loop.Loop();

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

  EventLoop loop;
  auto source_result = ReactorRecvSource::Create(&loop, fds[0]);
  if (!Check(source_result.has_value(), "stop source creation failed")) {
    ::close(fds[0]);
    ::close(fds[1]);
    return false;
  }
  auto source = std::move(*source_result);

  std::optional<ReactorRecvSource::Result> result;
  bool stop_succeeded = false;
  coropact::coro::SpawnDetach(loop, WaitForEnd(&source, &loop, &result));
  loop.RunAfter(0.0, [&] {
    coropact::coro::SpawnDetach(loop, StopOnly(&source, &stop_succeeded));
  });
  loop.Loop();

  ::close(fds[1]);
  ::close(fds[0]);
  return Check(result.has_value(), "Stop did not wake pending Next") &&
         Check(result->has_value(), "Stop wake returned an error") &&
         Check(!result->value().has_value(), "Stop wake did not return EOF") &&
         Check(stop_succeeded, "pending Next Stop failed");
}

}  // namespace

int main() {
  if (!CheckImmediateReceive() || !CheckPendingReceive() ||
      !CheckEof() || !CheckQueuedEvents() || !CheckStopWaitsForLease() ||
      !CheckStopWakesPendingNext()) {
    return 1;
  }
  std::cout << "reactor recv source smoke: PASS\n";
  return 0;
}
