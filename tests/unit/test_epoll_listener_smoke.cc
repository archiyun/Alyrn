// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <expected>
#include <iostream>
#include <optional>
#include <thread>

#include "alyrn/result.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/coro/sync_wait.h"
#include "alyrn/coro/task.h"
#include "alyrn/io/async_listener.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/epoll/connector.h"
#include "alyrn/epoll/listener.h"
#include "alyrn/epoll/loop.h"
#include "alyrn/epoll/stream.h"

namespace {

using AcceptResult = alyrn::Result<typename alyrn::epoll::Listener::StreamType>;
using AcceptSource = alyrn::epoll::AcceptSource;
using AcceptSourceResult = AcceptSource::NextResult;

static_assert(alyrn::io::AsyncListener<alyrn::epoll::Listener>);

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

int ConnectNonBlocking(const alyrn::net::Endpoint& address) {
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    return -1;
  }

  int rc = ::connect(fd, address.SockAddr(), address.SockAddrLen());
  if (rc == 0 || errno == EINPROGRESS) {
    return fd;
  }

  ::close(fd);
  return -1;
}

int GetSocketOption(int fd, int level, int option) {
  int value = 0;
  auto length = static_cast<socklen_t>(sizeof(value));
  if (::getsockopt(fd, level, option, &value, &length) < 0) {
    return -1;
  }
  return value;
}

alyrn::coro::DetachedTask AcceptOnce(alyrn::epoll::Listener* listener,
                                        alyrn::epoll::Loop* loop,
                                        std::optional<AcceptResult>* out) {
  out->emplace(co_await listener->Accept());
  loop->RequestStop();
}

alyrn::coro::DetachedTask AcceptAndCheckTcpOptions(
    alyrn::epoll::Listener* listener, alyrn::epoll::Loop* loop,
    bool* accepted, bool* options_applied) {
  auto result = co_await listener->Accept();
  *accepted = result.has_value();
  if (*accepted) {
    const int fd = result->Fd();
    *options_applied =
        GetSocketOption(fd, IPPROTO_TCP, TCP_NODELAY) == 1 &&
        GetSocketOption(fd, SOL_SOCKET, SO_KEEPALIVE) == 1;
  }
  loop->RequestStop();
}

alyrn::coro::DetachedTask AcceptThenAccept(alyrn::epoll::Listener* listener,
                                              alyrn::epoll::Loop* loop,
                                              std::optional<AcceptResult>* first,
                                              std::optional<AcceptResult>* second) {
  first->emplace(co_await listener->Accept());
  second->emplace(co_await listener->Accept());
  loop->RequestStop();
}

alyrn::coro::DetachedTask AcceptSourceTwice(AcceptSource* source,
                                               alyrn::epoll::Loop* loop,
                                               std::optional<AcceptSourceResult>* first,
                                               std::optional<AcceptSourceResult>* second,
                                               bool* stop_succeeded) {
  first->emplace(co_await source->Next());
  second->emplace(co_await source->Next());
  auto stopped = co_await source->Stop();
  *stop_succeeded = stopped.has_value();
  loop->RequestStop();
}

alyrn::coro::DetachedTask WaitForSourceEnd(AcceptSource* source,
                                              alyrn::epoll::Loop* loop, bool* got_end) {
  auto result = co_await source->Next();
  *got_end = result.has_value() && !result->has_value();
  loop->RequestStop();
}

alyrn::coro::DetachedTask StopSource(AcceptSource* source, bool* succeeded) {
  auto result = co_await source->Stop();
  *succeeded = result.has_value();
}

alyrn::coro::DetachedTask CloseListener(alyrn::epoll::Listener* listener,
                                           bool* succeeded) {
  auto result = co_await listener->Close();
  *succeeded = result.has_value();
}

struct CompetingAcceptObservation {
  std::optional<AcceptResult> first;
  std::optional<AcceptResult> second;
  std::optional<alyrn::Result<void>> close;
  int first_resume_count{0};
  int second_resume_count{0};
  int finished{0};
  bool timed_out{false};
};

alyrn::coro::DetachedTask ObserveFirstPendingAccept(alyrn::epoll::Listener* listener,
                                                       alyrn::epoll::Loop* loop,
                                                       CompetingAcceptObservation* observation) {
  observation->first.emplace(co_await listener->Accept());
  ++observation->first_resume_count;
  if (++observation->finished == 2) {
    loop->RequestStop();
  }
}

alyrn::coro::DetachedTask ObserveCompetingAccept(alyrn::epoll::Listener* listener,
                                                    alyrn::epoll::Loop* loop,
                                                    CompetingAcceptObservation* observation) {
  observation->second.emplace(co_await listener->Accept());
  ++observation->second_resume_count;
  observation->close.emplace(co_await listener->Close());
  if (++observation->finished == 2) {
    loop->RequestStop();
  }
}

bool CheckFactories() {
  auto null_listener =
      alyrn::epoll::Listener::Create(nullptr, alyrn::net::Endpoint(0));
  if (!Check(!null_listener.has_value() && null_listener.error() == std::errc::invalid_argument,
             "listener factory accepted a null Loop")) {
    return false;
  }

  auto null_connector = alyrn::epoll::Connector::Create(nullptr);
  if (!Check(!null_connector.has_value() && null_connector.error() == std::errc::invalid_argument,
             "connector factory accepted a null Loop")) {
    return false;
  }

  alyrn::epoll::Loop loop;
  auto listener = alyrn::epoll::Listener::Create(&loop, alyrn::net::Endpoint(0));
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

  auto conflicting_listener = alyrn::epoll::Listener::Create(&loop, *address);
  return Check(!conflicting_listener.has_value() &&
                   conflicting_listener.error() == std::errc::address_in_use,
               "listener factory did not return bind errors");
}

bool CheckPendingAccept() {
  alyrn::epoll::Loop loop;
  alyrn::epoll::Listener listener(&loop, alyrn::net::Endpoint(0));

  auto listen_addr = listener.LocalAddress();
  if (!listen_addr.has_value()) {
    std::cout << "FAIL: listener local address failed\n";
    return false;
  }

  std::optional<AcceptResult> result;
  int client_fd = -1;

  alyrn::coro::SpawnDetach(loop, AcceptOnce(&listener, &loop, &result));
  loop.RunAfter(alyrn::time::Duration::zero(),
                [&] { client_fd = ConnectNonBlocking(*listen_addr); });

  loop.Run();

  if (client_fd >= 0) {
    ::close(client_fd);
  }

  return Check(result.has_value(), "pending accept did not finish") &&
         Check(result->has_value(), "pending accept returned error");
}

bool CheckAcceptedTcpOptions() {
  alyrn::epoll::Loop loop;
  alyrn::epoll::ListenerOptions options;
  options.tcp_options.no_delay = true;
  options.tcp_options.keep_alive = true;
  alyrn::epoll::Listener listener(&loop, alyrn::net::Endpoint(0), options);

  auto listen_addr = listener.LocalAddress();
  if (!Check(listen_addr.has_value(), "listener local address failed")) {
    return false;
  }

  int client_fd = -1;
  bool accepted = false;
  bool options_applied = false;
  alyrn::coro::SpawnDetach(
      loop, AcceptAndCheckTcpOptions(&listener, &loop, &accepted, &options_applied));
  loop.RunAfter(alyrn::time::Duration::zero(),
                [&] { client_fd = ConnectNonBlocking(*listen_addr); });
  loop.Run();

  bool ok = Check(client_fd >= 0, "accept options test client failed to connect") &&
            Check(accepted, "accept options test did not return a stream") &&
            Check(options_applied,
                  "Listener did not apply TCP options before stream shutdown");

  if (client_fd >= 0) {
    ::close(client_fd);
  }
  return ok;
}

bool CheckAcceptReleasesSlotBeforeContinuation() {
  alyrn::epoll::Loop loop;
  alyrn::epoll::Listener listener(&loop, alyrn::net::Endpoint(0));

  auto listen_addr = listener.LocalAddress();
  if (!Check(listen_addr.has_value(), "listener local address failed")) {
    return false;
  }

  std::optional<AcceptResult> first;
  std::optional<AcceptResult> second;
  int first_client = -1;
  int second_client = -1;

  alyrn::coro::SpawnDetach(loop, AcceptThenAccept(&listener, &loop, &first, &second));
  loop.RunAfter(alyrn::time::Duration::zero(), [&] {
    first_client = ConnectNonBlocking(*listen_addr);
    second_client = ConnectNonBlocking(*listen_addr);
  });
  loop.Run();

  if (first_client >= 0) {
    ::close(first_client);
  }
  if (second_client >= 0) {
    ::close(second_client);
  }

  return Check(first_client >= 0 && second_client >= 0, "accept test clients failed to connect") &&
         Check(first.has_value() && first->has_value(), "first pending accept returned error") &&
         Check(second.has_value() && second->has_value(),
               "second accept did not reuse the released pending slot");
}

bool CheckCloseCancelsPendingAccept() {
  alyrn::epoll::Loop loop;
  alyrn::epoll::Listener listener(&loop, alyrn::net::Endpoint(0));

  std::optional<AcceptResult> result;

  alyrn::coro::SpawnDetach(loop, AcceptOnce(&listener, &loop, &result));
  loop.RunAfter(alyrn::time::Duration::zero(),
                [&] { alyrn::coro::Spawn(loop, listener.Close()).Detach(); });

  loop.Run();

  return Check(result.has_value(), "cancelled accept did not finish") &&
         Check(!result->has_value(), "cancelled accept unexpectedly succeeded") &&
         Check(result->error() == std::errc::operation_canceled,
               "cancelled accept did not return ECANCELED");
}

bool CheckCompetingAcceptIsRejected() {
  alyrn::epoll::Loop loop;
  alyrn::epoll::Listener listener(&loop, alyrn::net::Endpoint(0));
  CompetingAcceptObservation observation;

  alyrn::coro::SpawnDetach(loop, ObserveFirstPendingAccept(&listener, &loop, &observation));
  alyrn::coro::SpawnDetach(loop, ObserveCompetingAccept(&listener, &loop, &observation));
  loop.RunAfter(alyrn::time::Milliseconds(500), [&] {
    observation.timed_out = true;
    loop.RequestStop();
  });
  loop.Run();

  return Check(!observation.timed_out, "competing Accept test timed out") &&
         Check(observation.second.has_value() && !observation.second->has_value() &&
                   observation.second->error() == std::errc::device_or_resource_busy,
               "second pending Accept did not return EBUSY") &&
         Check(observation.first.has_value() && !observation.first->has_value() &&
                   observation.first->error() == std::errc::operation_canceled,
               "Close did not cancel the first pending Accept") &&
         Check(observation.close.has_value() && observation.close->has_value(),
               "Close failed after rejecting the competing Accept") &&
         Check(observation.first_resume_count == 1 && observation.second_resume_count == 1,
               "a competing Accept resumed more than once");
}

bool CheckAcceptSourceQueueAndStop() {
  alyrn::epoll::Loop loop;
  alyrn::epoll::Listener listener(&loop, alyrn::net::Endpoint(0));

  auto source_result = listener.CreateAcceptSource({.pending_depth = 1, .event_capacity = 1});
  if (!Check(source_result.has_value(), "failed to create epoll AcceptSource")) {
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

  alyrn::coro::SpawnDetach(loop,
                              AcceptSourceTwice(&source, &loop, &first, &second, &stop_succeeded));
  loop.RunAfter(alyrn::time::Duration::zero(), [&] {
    first_client = ConnectNonBlocking(*listen_addr);
    second_client = ConnectNonBlocking(*listen_addr);
  });
  loop.Run();

  if (first_client >= 0) {
    ::close(first_client);
  }
  if (second_client >= 0) {
    ::close(second_client);
  }

  return Check(first_client >= 0 && second_client >= 0, "AcceptSource clients failed to connect") &&
         Check(first.has_value() && first->has_value() && first->value().has_value(),
               "AcceptSource did not deliver its first stream") &&
         Check(second.has_value() && second->has_value() && second->value().has_value(),
               "AcceptSource did not deliver its second stream") &&
         Check(stop_succeeded, "AcceptSource Stop failed after queued events drained");
}

bool CheckAcceptSourceStopWakesPendingNext() {
  alyrn::epoll::Loop loop;
  alyrn::epoll::Listener listener(&loop, alyrn::net::Endpoint(0));

  auto source_result = listener.CreateAcceptSource();
  if (!Check(source_result.has_value(), "failed to create pending epoll AcceptSource")) {
    return false;
  }
  AcceptSource source = std::move(*source_result);

  bool got_end = false;
  bool stop_succeeded = false;
  alyrn::coro::SpawnDetach(loop, WaitForSourceEnd(&source, &loop, &got_end));
  loop.RunAfter(alyrn::time::Duration::zero(),
                [&] { alyrn::coro::SpawnDetach(loop, StopSource(&source, &stop_succeeded)); });
  loop.Run();

  return Check(got_end, "AcceptSource Stop did not wake pending Next with end-of-source") &&
         Check(stop_succeeded, "AcceptSource Stop returned an error");
}

bool CheckAcceptSourceListenerCloseWakesPendingNext() {
  alyrn::epoll::Loop loop;
  alyrn::epoll::Listener listener(&loop, alyrn::net::Endpoint(0));

  auto source_result = listener.CreateAcceptSource();
  if (!Check(source_result.has_value(), "failed to create close-test AcceptSource")) {
    return false;
  }
  AcceptSource source = std::move(*source_result);

  bool got_end = false;
  bool close_succeeded = false;
  alyrn::coro::SpawnDetach(loop, WaitForSourceEnd(&source, &loop, &got_end));
  loop.RunAfter(alyrn::time::Duration::zero(), [&] {
    alyrn::coro::SpawnDetach(loop, CloseListener(&listener, &close_succeeded));
  });
  loop.Run();

  return Check(got_end, "listener Close did not terminate pending AcceptSource::Next") &&
         Check(close_succeeded, "listener Close returned an error");
}

bool CheckAcceptSourceStopRejectsForeignLoop() {
  alyrn::epoll::Loop loop;
  alyrn::epoll::Listener listener(&loop, alyrn::net::Endpoint(0));
  auto source_result = listener.CreateAcceptSource();
  if (!Check(source_result.has_value(), "failed to create foreign Stop test source")) {
    return false;
  }
  AcceptSource source = std::move(*source_result);

  std::optional<alyrn::Result<void>> stop_result;
  std::thread foreign([&] { stop_result.emplace(alyrn::coro::SyncWait(source.Stop())); });
  foreign.join();

  return Check(stop_result.has_value(), "foreign AcceptSource::Stop did not return") &&
         Check(!stop_result->has_value() && stop_result->error() == std::errc::invalid_argument,
               "foreign AcceptSource::Stop must return EINVAL");
}

}  // namespace

int main() {
  if (!CheckFactories()) return 1;
  if (!CheckPendingAccept()) return 1;
  if (!CheckAcceptedTcpOptions()) return 1;
  if (!CheckAcceptReleasesSlotBeforeContinuation()) return 1;
  if (!CheckCloseCancelsPendingAccept()) return 1;
  if (!CheckCompetingAcceptIsRejected()) return 1;
  if (!CheckAcceptSourceQueueAndStop()) return 1;
  if (!CheckAcceptSourceStopWakesPendingNext()) return 1;
  if (!CheckAcceptSourceListenerCloseWakesPendingNext()) return 1;
  if (!CheckAcceptSourceStopRejectsForeignLoop()) return 1;

  std::cout << "epoll listener smoke: PASS\n";
  return 0;
}
