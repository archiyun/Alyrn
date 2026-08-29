// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <concepts>
#include <optional>
#include <print>
#include <stop_token>
#include <system_error>
#include <thread>

#include "alyrn/coro/detached_task.h"
#include "alyrn/result.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/epoll/runtime.h"

#ifdef ALYRN_ENABLE_URING
#include "alyrn/uring/runtime.h"
#endif

using namespace alyrn;

namespace {

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::println("FAIL: {}", message);
    return false;
  }
  return true;
}

class BoundPort final {
public:
  BoundPort() = default;
  explicit BoundPort(int fd, std::uint16_t port) noexcept : fd_(fd), port_(port) {}
  ~BoundPort() noexcept { Close(); }

  BoundPort(const BoundPort&) = delete;
  BoundPort& operator=(const BoundPort&) = delete;

  BoundPort(BoundPort&& other) noexcept
      : fd_(std::exchange(other.fd_, -1)), port_(other.port_) {}
  BoundPort& operator=(BoundPort&& other) noexcept {
    if (this != &other) {
      Close();
      fd_ = std::exchange(other.fd_, -1);
      port_ = other.port_;
    }
    return *this;
  }

  std::uint16_t port() const noexcept { return port_; }

  void Close() noexcept {
    if (fd_ >= 0) {
      (void)::close(std::exchange(fd_, -1));
    }
  }

private:
  int fd_{-1};
  std::uint16_t port_{0};
};

alyrn::Result<BoundPort> BindLoopbackPort() {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    return std::unexpected(alyrn::CurrentErrno());
  }

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(0);
  if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
    auto error = alyrn::CurrentErrno();
    (void)::close(fd);
    return std::unexpected(error);
  }

  socklen_t address_length = sizeof(address);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &address_length) < 0) {
    auto error = alyrn::CurrentErrno();
    (void)::close(fd);
    return std::unexpected(error);
  }
  return BoundPort{fd, ntohs(address.sin_port)};
}

template <typename Runtime>
bool WaitUntilStarted(Runtime& runtime) {
  constexpr auto kTimeout = std::chrono::seconds(1);
  constexpr auto kRetryDelay = std::chrono::milliseconds(1);
  const auto deadline = std::chrono::steady_clock::now() + kTimeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (runtime.Started()) {
      return true;
    }
    std::this_thread::sleep_for(kRetryDelay);
  }
  return runtime.Started();
}

coro::DetachedTask HandleEpoll(epoll::Stream) { co_return; }

static_assert(std::same_as<decltype(Runtime::Create<runtime::Epoll>(
                              net::Endpoint::Loopback(0), HandleEpoll)),
                           Runtime>);
static_assert(std::same_as<runtime::Auto, runtime::Epoll>);
static_assert(std::same_as<decltype(Runtime::Create(
                              net::Endpoint::Loopback(0), HandleEpoll)),
                           Runtime>);
static_assert(std::same_as<decltype(Runtime::Create<runtime::Auto>(
                              net::Endpoint::Loopback(0), HandleEpoll)),
                           Runtime>);

bool CheckEpollRuntime() {
  auto missing_handler = Runtime::Builder<runtime::Epoll>{net::Endpoint::Loopback(0)}
                             .Workers(1)
                             .Build();
  auto missing_started = missing_handler.Start();
  if (!Check(!missing_started.has_value() && missing_started.error() == std::errc::invalid_argument,
             "Epoll Runtime must reject a missing connection handler")) {
    return false;
  }

  auto zero_workers = Runtime::Builder<runtime::Epoll>{net::Endpoint::Loopback(0)}
                          .Workers(0)
                          .OnConnection(HandleEpoll)
                          .Build();
  auto zero_workers_started = zero_workers.Start();
  if (!Check(!zero_workers_started.has_value() && zero_workers_started.error().value() == EINVAL,
             "Epoll Runtime must reject zero workers")) {
    return false;
  }

  auto runtime = Runtime::Create<runtime::Epoll>(net::Endpoint::Loopback(0), HandleEpoll);
  auto started = runtime.Start();
  if (!Check(started.has_value(), "Epoll Runtime failed to start")) {
    return false;
  }
  const bool was_started = runtime.Started();
  runtime.Stop();
  runtime.Stop();
  auto restarted = runtime.Start();
  return Check(was_started, "Epoll Runtime did not report started") &&
         Check(!runtime.Started(), "Epoll Runtime did not stop") &&
         Check(!restarted.has_value() && restarted.error().value() == EALREADY,
               "Epoll Runtime must reject restart after Stop");
}

bool CheckEpollRuntimeRunWithPreCancelledToken() {
  std::stop_source stop_source;
  stop_source.request_stop();

  auto runtime = Runtime::Builder<runtime::Epoll>{net::Endpoint::Loopback(0)}
                     .Workers(1)
                     .OnConnection(HandleEpoll)
                     .Build();
  auto ran = runtime.Run(stop_source.get_token());
  if (!Check(ran.has_value(), "Epoll Runtime::Run failed")) {
    return false;
  }
  auto restarted = runtime.Start();
  return Check(!runtime.Started(), "Epoll Runtime::Run returned before stopping workers") &&
         Check(!restarted.has_value() && restarted.error().value() == EALREADY,
               "Epoll Runtime::Run must leave Runtime stopped");
}

bool CheckEpollRunStopsFromRuntimeRequest() {
  auto runtime = Runtime::Builder<runtime::Epoll>{net::Endpoint::Loopback(0)}
                     .Workers(1)
                     .OnConnection(HandleEpoll)
                     .Build();
  std::optional<Result<void>> run_result;
  std::jthread runner{[&] { run_result.emplace(runtime.Run({})); }};

  if (!WaitUntilStarted(runtime)) {
    runtime.RequestStop();
    runner.join();
    return Check(false, "Epoll Runtime::Run did not start");
  }

  runtime.RequestStop();
  runner.join();
  return Check(run_result.has_value() && run_result->has_value(),
               "Epoll Runtime::Run failed after RequestStop") &&
         Check(!runtime.Started(), "Epoll Runtime::Run did not join after RequestStop");
}

bool CheckEpollRequestStopFromForeignThread() {
  auto runtime = Runtime::Builder<runtime::Epoll>{net::Endpoint::Loopback(0)}
                     .Workers(1)
                     .OnConnection(HandleEpoll)
                     .Build();
  auto started = runtime.Start();
  if (!Check(started.has_value(), "Epoll Runtime failed to start for RequestStop")) {
    return false;
  }

  std::jthread requester{[&runtime] { runtime.RequestStop(); }};
  requester.join();

  const bool draining = runtime.Started();
  runtime.Stop();
  return Check(draining, "Epoll RequestStop must not join workers") &&
         Check(!runtime.Started(), "Epoll Stop must join requested workers");
}

bool CheckEpollStartFailureCanRetry() {
  auto reserved = BindLoopbackPort();
  if (!Check(reserved.has_value(), "failed to reserve Epoll retry test port")) {
    return false;
  }

  auto runtime = Runtime::Builder<runtime::Epoll>{net::Endpoint::Loopback(reserved->port())}
                     .Workers(1)
                     .OnConnection(HandleEpoll)
                     .Build();
  auto rejected = runtime.Start();
  if (!Check(!rejected.has_value() && rejected.error().value() == EADDRINUSE,
             "Epoll Runtime must report an occupied port")) {
    return false;
  }

  reserved->Close();
  auto started = runtime.Start();
  if (!Check(started.has_value(), "Epoll Runtime could not retry after bind failure")) {
    return false;
  }
  runtime.Stop();
  return Check(!runtime.Started(), "Epoll retry Runtime did not stop");
}

#ifdef ALYRN_ENABLE_URING

coro::DetachedTask HandleUring(uring::Stream) { co_return; }

static_assert(std::same_as<decltype(Runtime::Create<runtime::Uring>(
                              net::Endpoint::Loopback(0), HandleUring)),
                           Runtime>);

bool IsEnvironmentSkip(std::error_code error) {
  return error == std::errc::operation_not_supported || error == std::errc::operation_not_permitted;
}

bool CheckUringRuntime() {
  auto zero_workers = Runtime::Builder<runtime::Uring>{net::Endpoint::Loopback(0)}
                          .Workers(0)
                          .OnConnection(HandleUring)
                          .Build();
  auto zero_workers_started = zero_workers.Start();
  if (!Check(!zero_workers_started.has_value() && zero_workers_started.error().value() == EINVAL,
             "luring Runtime must reject zero workers")) {
    return false;
  }

  auto runtime = Runtime::Create<runtime::Uring>(net::Endpoint::Loopback(0), HandleUring);
  auto started = runtime.Start();
  if (!started.has_value()) {
    if (IsEnvironmentSkip(started.error())) {
      std::print("SKIP: io_uring unavailable: {}\n", started.error().message());
      return true;
    }
    std::print("FAIL: luring Runtime failed to start: {}\n", started.error().message());
    return false;
  }
  const bool was_started = runtime.Started();
  runtime.Stop();
  runtime.Stop();
  auto restarted = runtime.Start();
  return Check(was_started, "luring Runtime did not report started") &&
         Check(!runtime.Started(), "luring Runtime did not stop") &&
         Check(!restarted.has_value() && restarted.error().value() == EALREADY,
               "luring Runtime must reject restart after Stop");
}

bool CheckUringRuntimeRunWithPreCancelledToken() {
  std::stop_source stop_source;
  stop_source.request_stop();

  auto runtime = Runtime::Builder<runtime::Uring>{net::Endpoint::Loopback(0)}
                     .Workers(1)
                     .OnConnection(HandleUring)
                     .Build();
  auto ran = runtime.Run(stop_source.get_token());
  if (!ran.has_value()) {
    if (IsEnvironmentSkip(ran.error())) {
      std::print("SKIP: io_uring unavailable: {}\n", ran.error().message());
      return true;
    }
    std::print("FAIL: luring Runtime::Run failed: {}\n", ran.error().message());
    return false;
  }
  auto restarted = runtime.Start();
  return Check(!runtime.Started(), "luring Runtime::Run returned before stopping workers") &&
         Check(!restarted.has_value() && restarted.error().value() == EALREADY,
               "luring Runtime::Run must leave Runtime stopped");
}

bool CheckUringRunStopsFromRuntimeRequest() {
  auto runtime = Runtime::Builder<runtime::Uring>{net::Endpoint::Loopback(0)}
                     .Workers(1)
                     .OnConnection(HandleUring)
                     .Build();
  std::optional<Result<void>> run_result;
  std::jthread runner{[&] { run_result.emplace(runtime.Run({})); }};

  if (!WaitUntilStarted(runtime)) {
    runtime.RequestStop();
    runner.join();
    if (run_result.has_value() && !run_result->has_value() &&
        IsEnvironmentSkip(run_result->error())) {
      std::print("SKIP: io_uring unavailable: {}\n", run_result->error().message());
      return true;
    }
    return Check(false, "luring Runtime::Run did not start");
  }

  runtime.RequestStop();
  runner.join();
  if (!Check(run_result.has_value(), "luring Runtime::Run did not return a result")) {
    return false;
  }
  if (!run_result->has_value()) {
    if (IsEnvironmentSkip(run_result->error())) {
      std::print("SKIP: io_uring unavailable: {}\n", run_result->error().message());
      return true;
    }
    std::print("FAIL: luring Runtime::Run failed after RequestStop: {}\n",
               run_result->error().message());
    return false;
  }
  return Check(!runtime.Started(), "luring Runtime::Run did not join after RequestStop");
}

bool CheckUringRequestStopFromForeignThread() {
  auto runtime = Runtime::Builder<runtime::Uring>{net::Endpoint::Loopback(0)}
                     .Workers(1)
                     .OnConnection(HandleUring)
                     .Build();
  auto started = runtime.Start();
  if (!started.has_value()) {
    if (IsEnvironmentSkip(started.error())) {
      std::print("SKIP: io_uring unavailable: {}\n", started.error().message());
      return true;
    }
    std::print("FAIL: luring Runtime failed to start for RequestStop: {}\n",
               started.error().message());
    return false;
  }

  std::jthread requester{[&runtime] { runtime.RequestStop(); }};
  requester.join();

  const bool draining = runtime.Started();
  runtime.Stop();
  return Check(draining, "luring RequestStop must not join workers") &&
         Check(!runtime.Started(), "luring Stop must join requested workers");
}

bool CheckUringStartFailureCanRetry() {
  auto reserved = BindLoopbackPort();
  if (!Check(reserved.has_value(), "failed to reserve luring retry test port")) {
    return false;
  }

  auto runtime = Runtime::Builder<runtime::Uring>{net::Endpoint::Loopback(reserved->port())}
                     .Workers(1)
                     .OnConnection(HandleUring)
                     .Build();
  auto rejected = runtime.Start();
  if (!rejected.has_value() && IsEnvironmentSkip(rejected.error())) {
    std::print("SKIP: io_uring unavailable: {}\n", rejected.error().message());
    return true;
  }
  if (!Check(!rejected.has_value() && rejected.error().value() == EADDRINUSE,
             "luring Runtime must report an occupied port")) {
    return false;
  }

  reserved->Close();
  auto started = runtime.Start();
  if (!started.has_value()) {
    std::print("FAIL: luring Runtime could not retry after bind failure: {}\n",
               started.error().message());
    return false;
  }
  runtime.Stop();
  return Check(!runtime.Started(), "luring retry Runtime did not stop");
}

#endif

}  // namespace

int main() {
  bool ok = CheckEpollRuntime();
  ok = CheckEpollRuntimeRunWithPreCancelledToken() && ok;
  ok = CheckEpollRunStopsFromRuntimeRequest() && ok;
  ok = CheckEpollRequestStopFromForeignThread() && ok;
  ok = CheckEpollStartFailureCanRetry() && ok;
#ifdef ALYRN_ENABLE_URING
  ok = CheckUringRuntime() && ok;
  ok = CheckUringRuntimeRunWithPreCancelledToken() && ok;
  ok = CheckUringRunStopsFromRuntimeRequest() && ok;
  ok = CheckUringRequestStopFromForeignThread() && ok;
  ok = CheckUringStartFailureCanRetry() && ok;
#endif
  return ok ? 0 : 1;
}
