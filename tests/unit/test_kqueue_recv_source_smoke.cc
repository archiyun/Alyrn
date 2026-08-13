// SPDX-License-Identifier: MIT

#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <optional>
#include <string>
#include <string_view>

#include "coropact/backend/recv_source.h"
#include "coropact/result.h"
#include "coropact/coro/detached_task.h"
#include "coropact/coro/spawn.h"
#include "coropact/coro/work.h"
#include "coropact/io/recv_source.h"
#include "coropact/kqueue/loop.h"
#include "coropact/kqueue/recv_source.h"
#include "coropact/net/socket.h"

namespace {

using coropact::kqueue::KqueueLoop;
using coropact::kqueue::KqueueRecvSource;
using coropact::kqueue::KqueueRecvSourceOptions;

static_assert(coropact::io::AsyncRecvSource<KqueueRecvSource>);

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

bool MakeSocketPair(int fds[2]) {
  if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
    return false;
  }
  return coropact::net::SetNonBlocking(fds[0]).has_value() &&
         coropact::net::SetNonBlocking(fds[1]).has_value() &&
         coropact::net::SetCloseOnExec(fds[0]).has_value() &&
         coropact::net::SetCloseOnExec(fds[1]).has_value();
}

std::string BytesToString(const coropact::net::RecvEvent& event) {
  const auto bytes = event.buffer.Bytes();
  return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

coropact::coro::DetachedTask ReceiveOne(KqueueRecvSource* source, KqueueLoop* loop,
                                        std::optional<KqueueRecvSource::NextResult>* result,
                                        std::string* payload, bool* received_event,
                                        bool* stop_succeeded) {
  auto received = co_await source->Next();
  if (received.has_value() && received->has_value()) {
    *received_event = true;
    *payload = BytesToString(**received);
    received->reset();
  }
  result->emplace(std::move(received));
  auto stopped = co_await source->Stop();
  *stop_succeeded = stopped.has_value();
  loop->RequestStop();
}

bool CheckImmediateReceive() {
  int fds[2] = {-1, -1};
  if (!MakeSocketPair(fds)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  constexpr std::string_view kPayload = "hello-kqueue";
  if (::write(fds[1], kPayload.data(), kPayload.size()) != static_cast<ssize_t>(kPayload.size())) {
    ::close(fds[0]);
    ::close(fds[1]);
    return Check(false, "peer write failed");
  }

  KqueueLoop loop;
  auto source = KqueueRecvSource::Create(&loop, fds[0]);
  if (!Check(source.has_value(), "RecvSource create failed")) {
    ::close(fds[0]);
    ::close(fds[1]);
    return false;
  }

  std::optional<KqueueRecvSource::NextResult> result;
  std::string payload;
  bool received_event = false;
  bool stop_succeeded = false;
  coropact::coro::SpawnDetach(loop, ReceiveOne(&*source, &loop, &result, &payload, &received_event,
                                               &stop_succeeded));
  loop.Run();

  ::close(fds[0]);
  ::close(fds[1]);
  return Check(received_event, "did not receive an event") &&
         Check(payload == kPayload, "payload mismatch") &&
         Check(stop_succeeded, "Stop failed");
}

bool CheckPendingReceive() {
  int fds[2] = {-1, -1};
  if (!MakeSocketPair(fds)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  KqueueLoop loop;
  auto source = KqueueRecvSource::Create(&loop, fds[0]);
  if (!Check(source.has_value(), "RecvSource create failed")) {
    ::close(fds[0]);
    ::close(fds[1]);
    return false;
  }

  class WriteWork final : public coropact::coro::Work {
  public:
    WriteWork(int fd, std::string_view payload) noexcept : fd_(fd), payload_(payload) {
      SetRun(&RunWrite);
    }

  private:
    static void RunWrite(coropact::coro::Work* work) noexcept {
      auto* self = static_cast<WriteWork*>(work);
      (void)::write(self->fd_, self->payload_.data(), self->payload_.size());
    }

    int fd_;
    std::string_view payload_;
  };

  constexpr std::string_view kPayload = "later";
  WriteWork write{fds[1], kPayload};
  std::optional<KqueueRecvSource::NextResult> result;
  std::string payload;
  bool received_event = false;
  bool stop_succeeded = false;
  coropact::coro::SpawnDetach(loop, ReceiveOne(&*source, &loop, &result, &payload, &received_event,
                                               &stop_succeeded));
  loop.Schedule(&write);
  loop.Run();

  ::close(fds[0]);
  ::close(fds[1]);
  return Check(received_event, "pending receive did not fire") &&
         Check(payload == kPayload, "pending payload mismatch") &&
         Check(stop_succeeded, "Stop failed after pending receive");
}

bool CheckInvalidCreate() {
  KqueueLoop loop;
  auto source = KqueueRecvSource::Create(nullptr, 0);
  return Check(!source.has_value() && source.error() == std::errc::invalid_argument,
               "RecvSource must reject a null loop");
}

}  // namespace

int main() {
  if (!CheckInvalidCreate()) return 1;
  if (!CheckImmediateReceive()) return 1;
  if (!CheckPendingReceive()) return 1;
  std::cout << "kqueue recv source smoke: PASS\n";
  return 0;
}
