// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
// Runs the same application-observable connector scenarios against every
// enabled network backend.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <expected>
#include <functional>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/spawn.h"
#include "coropact/coro/task.h"
#include "coropact/reactor/connector.h"
#include "coropact/reactor/loop.h"

#if defined(COROPACT_ENABLE_URING)
#include "coropact/luring/connector.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/options.h"
#endif

namespace {

class UniqueFd {
public:
  UniqueFd() noexcept = default;
  explicit UniqueFd(int fd) noexcept : fd_(fd) {}
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
  ~UniqueFd() noexcept { Reset(); }

  void Reset(int fd = -1) noexcept {
    if (fd_ >= 0) {
      (void)::close(fd_);
    }
    fd_ = fd;
  }

private:
  int fd_{-1};
};

struct ListenEndpoint {
  UniqueFd fd;
  std::uint16_t port{0};
};

coropact::base::Result<ListenEndpoint> ListenLoopback() noexcept {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }

  auto fail = [fd](coropact::base::Error error) -> coropact::base::Result<ListenEndpoint> {
    (void)::close(fd);
    return std::unexpected(error);
  };

  int enabled = 1;
  if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) < 0) {
    return fail(coropact::base::CurrentErrno());
  }

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(0);
  if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
    return fail(coropact::base::CurrentErrno());
  }
  if (::listen(fd, SOMAXCONN) < 0) {
    return fail(coropact::base::CurrentErrno());
  }

  socklen_t length = sizeof(address);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length) < 0) {
    return fail(coropact::base::CurrentErrno());
  }
  return ListenEndpoint{.fd = UniqueFd(fd), .port = ntohs(address.sin_port)};
}

bool Expect(bool condition, std::string_view backend, std::string_view message) {
  if (condition) {
    return true;
  }
  std::cerr << "FAIL [" << backend << "]: " << message << '\n';
  return false;
}

struct ReactorHarness {
  using Loop = coropact::reactor::EventLoop;
  using Connector = coropact::reactor::ReactorConnector;

  static constexpr std::string_view Name() noexcept { return "Reactor"; }
  static bool Init(Loop&) noexcept { return true; }
  static bool Skip() noexcept { return false; }

  static coropact::base::Result<Connector> CreateConnector(Loop& loop) noexcept {
    return Connector::Create(&loop);
  }

  static bool RunAfter(Loop& loop, std::chrono::milliseconds delay,
                       std::function<void()> callback) {
    loop.RunAfter(std::chrono::duration<double>(delay).count(), std::move(callback));
    return true;
  }

  static void Run(Loop& loop) { loop.Run(); }
};

#if defined(COROPACT_ENABLE_URING)
struct LUringHarness {
  using Loop = coropact::luring::LUringLoop;
  using Connector = coropact::luring::LUringConnector;

  static constexpr std::string_view Name() noexcept { return "io_uring"; }

  static bool Init(Loop& loop) noexcept {
    coropact::luring::LUringOptions options;
    options.entries = 32;
    options.submit_batch = 1;
    auto initialized = loop.Init(options);
    if (initialized.has_value()) {
      init_error = {};
      return true;
    }
    init_error = initialized.error();
    return false;
  }

  static bool Skip() noexcept {
    return init_error == std::errc::operation_not_supported ||
           init_error == std::errc::operation_not_permitted;
  }

  static coropact::base::Result<Connector> CreateConnector(Loop& loop) noexcept {
    return Connector::Create(&loop);
  }

  static bool RunAfter(Loop& loop, std::chrono::milliseconds delay,
                       std::function<void()> callback) {
    auto timer = loop.RunAfter(delay, std::move(callback));
    if (timer.has_value()) {
      return true;
    }
    std::cerr << "FAIL [io_uring]: timer setup: " << timer.error().message() << '\n';
    return false;
  }

  static void Run(Loop& loop) noexcept { loop.Run(); }

  static inline coropact::base::Error init_error{};
};
#endif

template <class Connector>
struct ConnectObservation {
  using Result = coropact::base::Result<typename Connector::Stream>;

  std::optional<Result> result;
  int resume_count{0};
  bool resumed_with_scheduler{false};
  bool timed_out{false};
};

template <class Connector, class Loop>
auto ObserveConnect(Connector& connector, Loop& loop, std::string_view host, std::uint16_t port,
                    ConnectObservation<Connector>& observation) -> coropact::coro::DetachedTask {
  observation.result.emplace(co_await connector.Connect(host, port));
  ++observation.resume_count;
  observation.resumed_with_scheduler = coropact::coro::Scheduler::TryCurrent() == &loop;
  loop.RequestStop();
}

template <class Connector, class Loop>
auto ObservePreparedConnect(
    coropact::coro::Task<coropact::base::Result<typename Connector::Stream>> task, Loop& loop,
    ConnectObservation<Connector>& observation) -> coropact::coro::DetachedTask {
  observation.result.emplace(co_await std::move(task));
  ++observation.resume_count;
  observation.resumed_with_scheduler = coropact::coro::Scheduler::TryCurrent() == &loop;
  loop.RequestStop();
}

template <class Harness>
bool Initialize(typename Harness::Loop& loop) {
  if (Harness::Init(loop)) {
    return true;
  }
  if (Harness::Skip()) {
    std::cout << "SKIP [" << Harness::Name() << "]: backend unavailable\n";
    return false;
  }
  std::cerr << "FAIL [" << Harness::Name() << "]: loop initialization\n";
  return false;
}

template <class Harness>
bool CheckConnectSuccessContract() {
  auto listener = ListenLoopback();
  if (!listener.has_value()) {
    std::cerr << "FAIL [" << Harness::Name()
              << "]: listener creation: " << listener.error().message() << '\n';
    return false;
  }

  typename Harness::Loop loop;
  if (!Initialize<Harness>(loop)) {
    return Harness::Skip();
  }
  auto connector = Harness::CreateConnector(loop);
  if (!connector.has_value()) {
    std::cerr << "FAIL [" << Harness::Name()
              << "]: connector creation: " << connector.error().message() << '\n';
    return false;
  }

  ConnectObservation<typename Harness::Connector> observation;
  std::string host = "127.0.0.1";
  auto connect_task = connector->Connect(host, listener->port);
  host.assign("not-an-ip");
  coropact::coro::SpawnDetach(loop, ObservePreparedConnect<typename Harness::Connector>(
                                        std::move(connect_task), loop, observation));
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(500), [&] {
        observation.timed_out = true;
        loop.RequestStop();
      })) {
    return false;
  }
  Harness::Run(loop);

  return Expect(!observation.timed_out, Harness::Name(), "successful Connect timed out") &&
         Expect(observation.result.has_value() && observation.result->has_value(), Harness::Name(),
                "Connect to a listening endpoint failed") &&
         Expect(observation.resume_count == 1, Harness::Name(),
                "successful Connect resumed more than once") &&
         Expect(observation.resumed_with_scheduler, Harness::Name(),
                "successful Connect lost scheduler affinity");
}

template <class Connector>
struct ConnectCloseObservation {
  using Result = coropact::base::Result<typename Connector::Stream>;

  std::optional<Result> connect;
  std::optional<coropact::base::Result<void>> close;
  bool stream_valid_before_close{false};
  bool resumed_with_scheduler{false};
  bool timed_out{false};
};

template <class Connector, class Loop>
auto ObserveConnectThenClose(Connector& connector, Loop& loop, std::uint16_t port,
                             ConnectCloseObservation<Connector>& observation)
    -> coropact::coro::DetachedTask {
  observation.connect.emplace(co_await connector.Connect("127.0.0.1", port));
  if (observation.connect->has_value()) {
    auto& stream = observation.connect->value();
    observation.stream_valid_before_close = stream.Fd() >= 0;
    observation.close.emplace(co_await stream.Close());
  }
  observation.resumed_with_scheduler = coropact::coro::Scheduler::TryCurrent() == &loop;
  loop.RequestStop();
}

template <class Harness>
bool CheckConnectResultReleaseContract() {
  auto listener = ListenLoopback();
  if (!listener.has_value()) {
    std::cerr << "FAIL [" << Harness::Name()
              << "]: listener creation: " << listener.error().message() << '\n';
    return false;
  }

  typename Harness::Loop loop;
  if (!Initialize<Harness>(loop)) {
    return Harness::Skip();
  }
  auto connector = Harness::CreateConnector(loop);
  if (!connector.has_value()) {
    std::cerr << "FAIL [" << Harness::Name()
              << "]: connector creation: " << connector.error().message() << '\n';
    return false;
  }

  ConnectCloseObservation<typename Harness::Connector> observation;
  coropact::coro::SpawnDetach(
      loop, ObserveConnectThenClose(*connector, loop, listener->port, observation));
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(500), [&] {
        observation.timed_out = true;
        loop.RequestStop();
      })) {
    return false;
  }
  Harness::Run(loop);

  return Expect(!observation.timed_out, Harness::Name(), "Connect followed by Close timed out") &&
         Expect(observation.connect.has_value() && observation.connect->has_value(),
                Harness::Name(), "Connect did not publish a stream before continuation") &&
         Expect(observation.stream_valid_before_close, Harness::Name(),
                "Connect published a stream without a live descriptor") &&
         Expect(observation.close.has_value() && observation.close->has_value(), Harness::Name(),
                "stream Close failed immediately after Connect") &&
         Expect(observation.resumed_with_scheduler, Harness::Name(),
                "Connect followed by Close lost scheduler affinity");
}

template <class Harness>
bool CheckConnectionRefusedContract() {
  auto endpoint = ListenLoopback();
  if (!endpoint.has_value()) {
    std::cerr << "FAIL [" << Harness::Name()
              << "]: port reservation: " << endpoint.error().message() << '\n';
    return false;
  }
  const std::uint16_t port = endpoint->port;
  endpoint->fd.Reset();

  typename Harness::Loop loop;
  if (!Initialize<Harness>(loop)) {
    return Harness::Skip();
  }
  auto connector = Harness::CreateConnector(loop);
  if (!connector.has_value()) {
    return false;
  }

  ConnectObservation<typename Harness::Connector> observation;
  coropact::coro::SpawnDetach(loop,
                              ObserveConnect(*connector, loop, "127.0.0.1", port, observation));
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(500), [&] {
        observation.timed_out = true;
        loop.RequestStop();
      })) {
    return false;
  }
  Harness::Run(loop);

  return Expect(!observation.timed_out, Harness::Name(), "refused Connect timed out") &&
         Expect(observation.result.has_value() && !observation.result->has_value() &&
                    observation.result->error() == std::errc::connection_refused,
                Harness::Name(), "Connect did not preserve ECONNREFUSED") &&
         Expect(observation.resume_count == 1, Harness::Name(),
                "refused Connect resumed more than once") &&
         Expect(observation.resumed_with_scheduler, Harness::Name(),
                "refused Connect lost scheduler affinity");
}

template <class Harness>
bool CheckInvalidHostContract() {
  typename Harness::Loop loop;
  if (!Initialize<Harness>(loop)) {
    return Harness::Skip();
  }
  auto connector = Harness::CreateConnector(loop);
  if (!connector.has_value()) {
    return false;
  }

  ConnectObservation<typename Harness::Connector> observation;
  coropact::coro::SpawnDetach(loop, ObserveConnect(*connector, loop, "not-an-ip", 80, observation));
  Harness::Run(loop);

  return Expect(observation.result.has_value() && !observation.result->has_value() &&
                    observation.result->error() == std::errc::invalid_argument,
                Harness::Name(), "invalid numeric host did not return EINVAL") &&
         Expect(observation.resume_count == 1, Harness::Name(),
                "invalid-host Connect resumed more than once") &&
         Expect(observation.resumed_with_scheduler, Harness::Name(),
                "invalid-host Connect lost scheduler affinity");
}

template <class Connector, class Loop>
auto ObserveConnectAfterStopRequest(Connector& connector, Loop& loop,
                                    ConnectObservation<Connector>& observation)
    -> coropact::coro::DetachedTask {
  loop.RequestStop();
  observation.result.emplace(co_await connector.Connect("127.0.0.1", 9));
  ++observation.resume_count;
  observation.resumed_with_scheduler = coropact::coro::Scheduler::TryCurrent() == &loop;
}

template <class Harness>
bool CheckConnectAfterStopRequestContract() {
  typename Harness::Loop loop;
  if (!Initialize<Harness>(loop)) {
    return Harness::Skip();
  }
  auto connector = Harness::CreateConnector(loop);
  if (!connector.has_value()) {
    return false;
  }

  ConnectObservation<typename Harness::Connector> observation;
  coropact::coro::SpawnDetach(loop, ObserveConnectAfterStopRequest(*connector, loop, observation));
  Harness::Run(loop);

  return Expect(observation.result.has_value() && !observation.result->has_value() &&
                    observation.result->error() == std::errc::operation_canceled,
                Harness::Name(), "Connect succeeded after loop stop was requested") &&
         Expect(observation.resume_count == 1, Harness::Name(),
                "post-stop Connect resumed more than once") &&
         Expect(observation.resumed_with_scheduler, Harness::Name(),
                "post-stop Connect lost scheduler affinity");
}

template <class Connector>
struct ConcurrentConnectObservation {
  using Result = coropact::base::Result<typename Connector::Stream>;

  std::optional<Result> first;
  std::optional<Result> second;
  int first_resume_count{0};
  int second_resume_count{0};
  int finished{0};
  bool resumed_with_scheduler{true};
  bool timed_out{false};
};

template <class Connector, class Loop>
auto ObserveConcurrentConnect(
    Connector& connector, Loop& loop, std::uint16_t port,
    std::optional<typename ConcurrentConnectObservation<Connector>::Result>& result,
    int& resume_count, ConcurrentConnectObservation<Connector>& observation)
    -> coropact::coro::DetachedTask {
  result.emplace(co_await connector.Connect("127.0.0.1", port));
  ++resume_count;
  observation.resumed_with_scheduler &= coropact::coro::Scheduler::TryCurrent() == &loop;
  if (++observation.finished == 2) {
    loop.RequestStop();
  }
}

template <class Harness>
bool CheckConcurrentConnectContract() {
  auto listener = ListenLoopback();
  if (!listener.has_value()) {
    return false;
  }

  typename Harness::Loop loop;
  if (!Initialize<Harness>(loop)) {
    return Harness::Skip();
  }
  auto connector = Harness::CreateConnector(loop);
  if (!connector.has_value()) {
    return false;
  }

  ConcurrentConnectObservation<typename Harness::Connector> observation;
  coropact::coro::SpawnDetach(
      loop, ObserveConcurrentConnect(*connector, loop, listener->port, observation.first,
                                     observation.first_resume_count, observation));
  coropact::coro::SpawnDetach(
      loop, ObserveConcurrentConnect(*connector, loop, listener->port, observation.second,
                                     observation.second_resume_count, observation));
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(500), [&] {
        observation.timed_out = true;
        loop.RequestStop();
      })) {
    return false;
  }
  Harness::Run(loop);

  return Expect(!observation.timed_out, Harness::Name(), "concurrent Connect timed out") &&
         Expect(observation.first.has_value() && observation.first->has_value() &&
                    observation.second.has_value() && observation.second->has_value(),
                Harness::Name(),
                "connector did not support independent concurrent Connect calls") &&
         Expect(observation.first_resume_count == 1 && observation.second_resume_count == 1,
                Harness::Name(), "a concurrent Connect resumed more than once") &&
         Expect(observation.resumed_with_scheduler, Harness::Name(),
                "concurrent Connect lost scheduler affinity");
}

template <class Harness>
bool RunBackendSuite() {
  return CheckConnectSuccessContract<Harness>() && CheckConnectResultReleaseContract<Harness>() &&
         CheckConnectionRefusedContract<Harness>() && CheckInvalidHostContract<Harness>() &&
         CheckConnectAfterStopRequestContract<Harness>() &&
         CheckConcurrentConnectContract<Harness>();
}

}  // namespace

int main() {
  if (!RunBackendSuite<ReactorHarness>()) {
    return 1;
  }
#if defined(COROPACT_ENABLE_URING)
  if (!RunBackendSuite<LUringHarness>()) {
    return 1;
  }
#endif
  std::cout << "connector lifecycle conformance: PASS\n";
  return 0;
}
