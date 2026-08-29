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
#include "alyrn/kqueue/runtime.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/detail/net/socket.h"

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
  auto created = alyrn::net::CreateNonBlockingSocket(AF_INET);
  if (!created.has_value()) {
    return std::unexpected(created.error());
  }
  const int fd = *created;

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

coro::DetachedTask HandleKqueue(kqueue::Stream) { co_return; }

static_assert(std::same_as<decltype(Runtime::Create<runtime::Kqueue>(
                              net::Endpoint::Loopback(0), HandleKqueue)),
                           Runtime>);
static_assert(std::same_as<runtime::Auto, runtime::Kqueue>);
static_assert(std::same_as<decltype(Runtime::Create(
                              net::Endpoint::Loopback(0), HandleKqueue)),
                           Runtime>);
static_assert(std::same_as<decltype(Runtime::Create<runtime::Auto>(
                              net::Endpoint::Loopback(0), HandleKqueue)),
                           Runtime>);

bool CheckKqueueRuntime() {
  auto missing_handler = Runtime::Builder<runtime::Kqueue>{net::Endpoint::Loopback(0)}
                             .Workers(1)
                             .Build();
  auto missing_started = missing_handler.Start();
  if (!Check(!missing_started.has_value() && missing_started.error() == std::errc::invalid_argument,
             "kqueue Runtime must reject a missing connection handler")) {
    return false;
  }

  auto zero_workers = Runtime::Builder<runtime::Kqueue>{net::Endpoint::Loopback(0)}
                          .Workers(0)
                          .OnConnection(HandleKqueue)
                          .Build();
  auto zero_workers_started = zero_workers.Start();
  if (!Check(!zero_workers_started.has_value() && zero_workers_started.error().value() == EINVAL,
             "kqueue Runtime must reject zero workers")) {
    return false;
  }

  auto runtime = Runtime::Create<runtime::Kqueue>(net::Endpoint::Loopback(0), HandleKqueue);
  auto started = runtime.Start();
  if (!Check(started.has_value(), "kqueue Runtime failed to start")) {
    if (!started.has_value()) {
      std::println("start error: {}", started.error().message());
    }
    return false;
  }
  const bool was_started = runtime.Started();
  runtime.Stop();
  runtime.Stop();
  auto restarted = runtime.Start();
  return Check(was_started, "kqueue Runtime did not report started") &&
         Check(!runtime.Started(), "kqueue Runtime did not stop") &&
         Check(!restarted.has_value() && restarted.error().value() == EALREADY,
               "kqueue Runtime must reject restart after Stop");
}

bool CheckKqueueRuntimeRunWithPreCancelledToken() {
  std::stop_source stop_source;
  stop_source.request_stop();

  auto runtime = Runtime::Builder<runtime::Kqueue>{net::Endpoint::Loopback(0)}
                     .Workers(1)
                     .OnConnection(HandleKqueue)
                     .Build();
  auto ran = runtime.Run(stop_source.get_token());
  if (!Check(ran.has_value(), "kqueue Runtime::Run failed")) {
    return false;
  }
  auto restarted = runtime.Start();
  return Check(!runtime.Started(), "kqueue Runtime::Run returned before stopping workers") &&
         Check(!restarted.has_value() && restarted.error().value() == EALREADY,
               "kqueue Runtime::Run must leave Runtime stopped");
}

bool CheckKqueueRunStopsFromRuntimeRequest() {
  auto runtime = Runtime::Builder<runtime::Kqueue>{net::Endpoint::Loopback(0)}
                     .Workers(1)
                     .OnConnection(HandleKqueue)
                     .Build();
  std::optional<Result<void>> run_result;
  std::jthread runner{[&] { run_result.emplace(runtime.Run({})); }};

  if (!WaitUntilStarted(runtime)) {
    runtime.RequestStop();
    runner.join();
    return Check(false, "kqueue Runtime::Run did not start");
  }

  runtime.RequestStop();
  runner.join();
  return Check(run_result.has_value() && run_result->has_value(),
               "kqueue Runtime::Run failed after RequestStop") &&
         Check(!runtime.Started(), "kqueue Runtime::Run did not join after RequestStop");
}

bool CheckKqueueRequestStopFromForeignThread() {
  auto runtime = Runtime::Builder<runtime::Kqueue>{net::Endpoint::Loopback(0)}
                     .Workers(1)
                     .OnConnection(HandleKqueue)
                     .Build();
  auto started = runtime.Start();
  if (!Check(started.has_value(), "kqueue Runtime failed to start for RequestStop")) {
    return false;
  }

  std::jthread requester{[&runtime] { runtime.RequestStop(); }};
  requester.join();

  const bool draining = runtime.Started();
  runtime.Stop();
  return Check(draining, "kqueue RequestStop must not join workers") &&
         Check(!runtime.Started(), "kqueue Stop must join requested workers");
}

bool CheckKqueueStartFailureCanRetry() {
  auto reserved = BindLoopbackPort();
  if (!Check(reserved.has_value(), "failed to reserve kqueue retry test port")) {
    return false;
  }

  auto runtime = Runtime::Builder<runtime::Kqueue>{net::Endpoint::Loopback(reserved->port())}
                     .Workers(1)
                     .OnConnection(HandleKqueue)
                     .Build();
  auto rejected = runtime.Start();
  if (!Check(!rejected.has_value() && rejected.error().value() == EADDRINUSE,
             "kqueue Runtime must report an occupied port")) {
    if (!rejected.has_value()) {
      std::println("reject error: {}", rejected.error().message());
    } else {
      std::println("occupied port unexpectedly started");
    }
    return false;
  }

  reserved->Close();
  auto started = runtime.Start();
  if (!Check(started.has_value(), "kqueue Runtime could not retry after bind failure")) {
    if (!started.has_value()) {
      std::println("retry error: {}", started.error().message());
    }
    return false;
  }
  runtime.Stop();
  return Check(!runtime.Started(), "kqueue retry Runtime did not stop");
}

}  // namespace

int main() {
  bool ok = CheckKqueueRuntime();
  ok = CheckKqueueRuntimeRunWithPreCancelledToken() && ok;
  ok = CheckKqueueRunStopsFromRuntimeRequest() && ok;
  ok = CheckKqueueRequestStopFromForeignThread() && ok;
  ok = CheckKqueueStartFailureCanRetry() && ok;
  return ok ? 0 : 1;
}
