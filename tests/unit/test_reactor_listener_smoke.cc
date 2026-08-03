// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <expected>
#include <iostream>
#include <optional>

#include "coropact/base/error.h"
#include "coropact/coro/spawn.h"
#include "coropact/coro/task.h"
#include "coropact/io/async_listener.h"
#include "coropact/net/endpoint.h"
#include "coropact/reactor/event_loop.h"
#include "coropact/reactor/reactor_connect.h"
#include "coropact/reactor/reactor_listener.h"
#include "coropact/reactor/reactor_stream.h"

namespace {

using AcceptResult = coropact::base::Result<typename coropact::reactor::ReactorListener::Stream>;
using AcceptSource = coropact::reactor::ReactorAcceptSource;
using AcceptSourceResult = AcceptSource::Result;

static_assert(coropact::io::AsyncListener<coropact::reactor::ReactorListener>);

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

int ConnectNonBlocking(const coropact::net::Endpoint& address) {
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    return -1;
  }

  int rc = ::connect(fd, address.sock_addr(), address.sock_addr_len());
  if (rc == 0 || errno == EINPROGRESS) {
    return fd;
  }

  ::close(fd);
  return -1;
}

coropact::coro::DetachedTask AcceptOnce(coropact::reactor::ReactorListener* listener,
                                        coropact::reactor::EventLoop* loop,
                                        std::optional<AcceptResult>* out) {
  out->emplace(co_await listener->Accept());
  loop->Quit();
}

coropact::coro::DetachedTask AcceptSourceTwice(
    AcceptSource* source, coropact::reactor::EventLoop* loop,
    std::optional<AcceptSourceResult>* first,
    std::optional<AcceptSourceResult>* second, bool* stop_succeeded) {
  first->emplace(co_await source->Next());
  second->emplace(co_await source->Next());
  auto stopped = co_await source->Stop();
  *stop_succeeded = stopped.has_value();
  loop->Quit();
}

coropact::coro::DetachedTask WaitForSourceEnd(
    AcceptSource* source, coropact::reactor::EventLoop* loop, bool* got_end) {
  auto result = co_await source->Next();
  *got_end = result.has_value() && !result->has_value();
  loop->Quit();
}

coropact::coro::DetachedTask StopSource(AcceptSource* source, bool* succeeded) {
  auto result = co_await source->Stop();
  *succeeded = result.has_value();
}

coropact::coro::DetachedTask CloseListener(
    coropact::reactor::ReactorListener* listener, bool* succeeded) {
  auto result = co_await listener->Close();
  *succeeded = result.has_value();
}

bool CheckFactories() {
  auto null_listener =
      coropact::reactor::ReactorListener::Create(nullptr, coropact::net::Endpoint(0));
  if (!Check(!null_listener.has_value() && null_listener.error() == std::errc::invalid_argument,
             "listener factory accepted a null EventLoop")) {
    return false;
  }

  auto null_connector = coropact::reactor::ReactorConnector::Create(nullptr);
  if (!Check(!null_connector.has_value() && null_connector.error() == std::errc::invalid_argument,
             "connector factory accepted a null EventLoop")) {
    return false;
  }

  coropact::reactor::EventLoop loop;
  auto listener = coropact::reactor::ReactorListener::Create(&loop, coropact::net::Endpoint(0));
  if (!Check(listener.has_value(), "listener factory failed for a valid socket")) {
    if (!listener.has_value()) {
      std::cout << "factory error: " << listener.error().message() << '\n';
    }
    return false;
  }

  auto address = listener->LocalAddress();
  if (!Check(address.has_value(), "factory listener local address lookup failed")) {
    return false;
  }

  auto conflicting_listener = coropact::reactor::ReactorListener::Create(&loop, *address);
  return Check(!conflicting_listener.has_value() &&
                   conflicting_listener.error() == std::errc::address_in_use,
               "listener factory did not return bind errors");
}

bool CheckPendingAccept() {
  coropact::reactor::EventLoop loop;
  coropact::reactor::ReactorListener listener(&loop, coropact::net::Endpoint(0));

  auto listen_addr = listener.LocalAddress();
  if (!listen_addr.has_value()) {
    std::cout << "FAIL: listener local address failed\n";
    return false;
  }

  std::optional<AcceptResult> result;
  int client_fd = -1;

  coropact::coro::SpawnDetach(loop, AcceptOnce(&listener, &loop, &result));
  loop.RunAfter(0.0, [&] { client_fd = ConnectNonBlocking(*listen_addr); });

  loop.Loop();

  if (client_fd >= 0) {
    ::close(client_fd);
  }

  return Check(result.has_value(), "pending accept did not finish") &&
         Check(result->has_value(), "pending accept returned error");
}

bool CheckCloseCancelsPendingAccept() {
  coropact::reactor::EventLoop loop;
  coropact::reactor::ReactorListener listener(&loop, coropact::net::Endpoint(0));

  std::optional<AcceptResult> result;

  coropact::coro::SpawnDetach(loop, AcceptOnce(&listener, &loop, &result));
  loop.RunAfter(0.0, [&] { coropact::coro::Spawn(loop, listener.Close()).Detach(); });

  loop.Loop();

  return Check(result.has_value(), "cancelled accept did not finish") &&
         Check(!result->has_value(), "cancelled accept unexpectedly succeeded") &&
         Check(result->error() == std::errc::operation_canceled,
               "cancelled accept did not return ECANCELED");
}

bool CheckAcceptSourceQueueAndStop() {
  coropact::reactor::EventLoop loop;
  coropact::reactor::ReactorListener listener(&loop, coropact::net::Endpoint(0));

  auto source_result = listener.AcceptSource({.pending_depth = 1, .event_capacity = 1});
  if (!Check(source_result.has_value(), "failed to create reactor AcceptSource")) {
    return false;
  }
  AcceptSource source = std::move(*source_result);

  auto listen_addr = listener.LocalAddress();
  if (!Check(listen_addr.has_value(), "AcceptSource listener address lookup failed")) {
    return false;
  }

  std::optional<AcceptSourceResult> first;
  std::optional<AcceptSourceResult> second;
  bool stop_succeeded = false;
  int first_client = -1;
  int second_client = -1;

  coropact::coro::SpawnDetach(
      loop, AcceptSourceTwice(&source, &loop, &first, &second, &stop_succeeded));
  loop.RunAfter(0.0, [&] {
    first_client = ConnectNonBlocking(*listen_addr);
    second_client = ConnectNonBlocking(*listen_addr);
  });
  loop.Loop();

  if (first_client >= 0) {
    ::close(first_client);
  }
  if (second_client >= 0) {
    ::close(second_client);
  }

  return Check(first_client >= 0 && second_client >= 0,
               "AcceptSource clients failed to connect") &&
         Check(first.has_value() && first->has_value() && first->value().has_value(),
               "AcceptSource did not deliver its first stream") &&
         Check(second.has_value() && second->has_value() && second->value().has_value(),
               "AcceptSource did not deliver its second stream") &&
         Check(stop_succeeded, "AcceptSource Stop failed after queued events drained");
}

bool CheckAcceptSourceStopWakesPendingNext() {
  coropact::reactor::EventLoop loop;
  coropact::reactor::ReactorListener listener(&loop, coropact::net::Endpoint(0));

  auto source_result = listener.AcceptSource();
  if (!Check(source_result.has_value(), "failed to create pending reactor AcceptSource")) {
    return false;
  }
  AcceptSource source = std::move(*source_result);

  bool got_end = false;
  bool stop_succeeded = false;
  coropact::coro::SpawnDetach(loop, WaitForSourceEnd(&source, &loop, &got_end));
  loop.RunAfter(0.0, [&] {
    coropact::coro::SpawnDetach(loop, StopSource(&source, &stop_succeeded));
  });
  loop.Loop();

  return Check(got_end, "AcceptSource Stop did not wake pending Next with end-of-source") &&
         Check(stop_succeeded, "AcceptSource Stop returned an error");
}

bool CheckAcceptSourceListenerCloseWakesPendingNext() {
  coropact::reactor::EventLoop loop;
  coropact::reactor::ReactorListener listener(&loop, coropact::net::Endpoint(0));

  auto source_result = listener.AcceptSource();
  if (!Check(source_result.has_value(), "failed to create close-test AcceptSource")) {
    return false;
  }
  AcceptSource source = std::move(*source_result);

  bool got_end = false;
  bool close_succeeded = false;
  coropact::coro::SpawnDetach(loop, WaitForSourceEnd(&source, &loop, &got_end));
  loop.RunAfter(0.0, [&] {
    coropact::coro::SpawnDetach(loop, CloseListener(&listener, &close_succeeded));
  });
  loop.Loop();

  return Check(got_end, "listener Close did not terminate pending AcceptSource::Next") &&
         Check(close_succeeded, "listener Close returned an error");
}

}  // namespace

int main() {
  if (!CheckFactories()) return 1;
  if (!CheckPendingAccept()) return 1;
  if (!CheckCloseCancelsPendingAccept()) return 1;
  if (!CheckAcceptSourceQueueAndStop()) return 1;
  if (!CheckAcceptSourceStopWakesPendingNext()) return 1;
  if (!CheckAcceptSourceListenerCloseWakesPendingNext()) return 1;

  std::cout << "reactor listener smoke: PASS\n";
  return 0;
}
