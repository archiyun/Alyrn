// SPDX-License-Identifier: MIT

#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <optional>
#include <string>
#include <string_view>

#include "alyrn/backend/recv_source.h"
#include "alyrn/result.h"
#include "alyrn/coro/detached_task.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/coro/work.h"
#include "alyrn/io/recv_source.h"
#include "alyrn/kqueue/loop.h"
#include "alyrn/kqueue/recv_source.h"
#include "alyrn/net/socket.h"

namespace {

using alyrn::kqueue::Loop;
using alyrn::kqueue::RecvSource;
using alyrn::kqueue::RecvSourceOptions;

static_assert(alyrn::io::AsyncRecvSource<RecvSource>);

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
  return alyrn::net::SetNonBlocking(fds[0]).has_value() &&
         alyrn::net::SetNonBlocking(fds[1]).has_value() &&
         alyrn::net::SetCloseOnExec(fds[0]).has_value() &&
         alyrn::net::SetCloseOnExec(fds[1]).has_value();
}

std::string BytesToString(const alyrn::net::RecvEvent& event) {
  const auto bytes = event.buffer.Bytes();
  return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

alyrn::coro::DetachedTask ReceiveOne(RecvSource* source, Loop* loop,
                                        std::optional<RecvSource::NextResult>* result,
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

  Loop loop;
  auto source = RecvSource::Create(&loop, fds[0]);
  if (!Check(source.has_value(), "RecvSource create failed")) {
    ::close(fds[0]);
    ::close(fds[1]);
    return false;
  }

  std::optional<RecvSource::NextResult> result;
  std::string payload;
  bool received_event = false;
  bool stop_succeeded = false;
  alyrn::coro::SpawnDetach(loop, ReceiveOne(&*source, &loop, &result, &payload, &received_event,
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

  Loop loop;
  auto source = RecvSource::Create(&loop, fds[0]);
  if (!Check(source.has_value(), "RecvSource create failed")) {
    ::close(fds[0]);
    ::close(fds[1]);
    return false;
  }

  class WriteWork final : public alyrn::coro::Work {
  public:
    WriteWork(int fd, std::string_view payload) noexcept : fd_(fd), payload_(payload) {
      SetRun(&RunWrite);
    }

  private:
    static void RunWrite(alyrn::coro::Work* work) noexcept {
      auto* self = static_cast<WriteWork*>(work);
      (void)::write(self->fd_, self->payload_.data(), self->payload_.size());
    }

    int fd_;
    std::string_view payload_;
  };

  constexpr std::string_view kPayload = "later";
  WriteWork write{fds[1], kPayload};
  std::optional<RecvSource::NextResult> result;
  std::string payload;
  bool received_event = false;
  bool stop_succeeded = false;
  alyrn::coro::SpawnDetach(loop, ReceiveOne(&*source, &loop, &result, &payload, &received_event,
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
  Loop loop;
  auto source = RecvSource::Create(nullptr, 0);
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
