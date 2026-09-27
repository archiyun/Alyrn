// SPDX-License-Identifier: MIT

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <concepts>
#include <memory>
#include <optional>
#include <print>
#include <stop_token>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include "alyrn/channel.h"
#include "alyrn/coro/detached_task.h"
#include "alyrn/epoll/runtime.h"
#include "alyrn/epoll/timer.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/result.h"
#include "alyrn/spawn.h"
#include "alyrn/time/clock.h"

#ifdef ALYRN_ENABLE_URING
#include "alyrn/uring/runtime.h"
#include "alyrn/uring/timer.h"
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

bool IsEnvironmentSkip(std::error_code error) {
  return error == std::errc::operation_not_supported || error == std::errc::operation_not_permitted;
}

template <typename Predicate>
bool WaitFor(Predicate predicate) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return predicate();
}

alyrn::Result<int> ConnectLoopback(std::uint16_t port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    return std::unexpected(alyrn::CurrentErrno());
  }
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(port);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
    auto error = alyrn::CurrentErrno();
    (void)::close(fd);
    return std::unexpected(error);
  }
  return fd;
}

// Reserves an ephemeral loopback port number for a Runtime under test.
alyrn::Result<std::uint16_t> PickLoopbackPort() {
  auto reserved = BindLoopbackPort();
  if (!reserved.HasValue()) {
    return std::unexpected(reserved.Error());
  }
  const std::uint16_t port = reserved->port();
  reserved->Close();
  return port;
}

std::atomic<int> g_observed_no_delay{-1};

template <typename Stream>
coro::DetachedTask RecordNoDelay(Stream stream) {
  int value = 0;
  socklen_t length = sizeof(value);
  const int result = ::getsockopt(stream.Fd(), IPPROTO_TCP, TCP_NODELAY, &value, &length);
  g_observed_no_delay.store(result == 0 ? value : -2, std::memory_order_release);
  co_return;
}

// Accepted streams must carry the Builder's TCP options (and only those).
template <typename Backend, typename Stream>
bool CheckAcceptedTcpOptions(std::optional<net::TcpOptions> tcp_options, int expected,
                             const char* message) {
  auto port = PickLoopbackPort();
  if (!Check(port.HasValue(), "failed to reserve TCP options test port")) {
    return false;
  }

  g_observed_no_delay.store(-1, std::memory_order_release);
  Runtime::Builder<Backend> builder{net::Endpoint::Loopback(*port)};
  builder.Workers(1).OnConnection(RecordNoDelay<Stream>);
  if (tcp_options.has_value()) {
    builder.Tcp(*tcp_options);
  }
  auto runtime = builder.Build();
  auto started = runtime.Start();
  if (!started.HasValue()) {
    if (IsEnvironmentSkip(started.Error())) {
      std::print("SKIP: runtime backend unavailable: {}\n", started.Error().message());
      return true;
    }
    std::print("FAIL: TCP options Runtime failed to start: {}\n", started.Error().message());
    return false;
  }

  auto client = ConnectLoopback(*port);
  const bool observed =
      client.HasValue() &&
      WaitFor([] { return g_observed_no_delay.load(std::memory_order_acquire) != -1; });
  if (client.HasValue()) {
    (void)::close(*client);
  }
  runtime.Stop();
  return Check(client.HasValue(), "TCP options test client failed to connect") &&
         Check(observed, "TCP options test handler did not run") &&
         Check(g_observed_no_delay.load(std::memory_order_acquire) == expected, message);
}

// Per-worker state created by OnWorkerStart and closed by OnWorkerStop, the
// shape of a stateful server (for example a chat hub owned by each worker).
thread_local std::unique_ptr<Channel<int>> t_worker_channel;
std::atomic<int> g_handler_saw_worker_state{-1};

struct WorkerHookObservation {
  std::atomic<int> started{0};
  std::atomic<int> stopped{0};
  std::atomic<int> waiters_finished{0};
  std::atomic<bool> hooks_in_loop_context{true};
};

coro::DetachedTask WaitForWorkerChannelClose(Channel<int>& channel,
                                             WorkerHookObservation& observed) {
  std::optional<int> value;
  (void)co_await (channel >> value);
  if (!value.has_value()) {
    ++observed.waiters_finished;
  }
}

template <typename Stream>
coro::DetachedTask RecordWorkerState(Stream) {
  g_handler_saw_worker_state.store(t_worker_channel != nullptr ? 1 : 0,
                                   std::memory_order_release);
  co_return;
}

// Start hooks create owner-affine state before the first connection; stop
// hooks close it before the Loop dies, and the waiters they wake complete.
template <typename Backend, typename Loop, typename Stream>
bool CheckWorkerHooks() {
  constexpr std::size_t kWorkers = 2;
  auto port = PickLoopbackPort();
  if (!Check(port.HasValue(), "failed to reserve worker hook test port")) {
    return false;
  }

  WorkerHookObservation observed;
  g_handler_saw_worker_state.store(-1, std::memory_order_release);
  auto runtime =
      Runtime::Builder<Backend>{net::Endpoint::Loopback(*port)}
          .Workers(kWorkers)
          .OnWorkerStart([&observed](Loop& loop, std::size_t) -> Result<void> {
            if (!loop.IsInLoopThread() || coro::Scheduler::TryCurrent() != &loop) {
              observed.hooks_in_loop_context = false;
            }
            t_worker_channel = std::make_unique<Channel<int>>(loop, 0);
            SpawnDetach(loop, WaitForWorkerChannelClose(*t_worker_channel, observed));
            ++observed.started;
            return {};
          })
          .OnWorkerStop([&observed](Loop& loop, std::size_t) {
            if (!loop.IsInLoopThread() || coro::Scheduler::TryCurrent() != &loop) {
              observed.hooks_in_loop_context = false;
            }
            t_worker_channel->Close();
            ++observed.stopped;
          })
          .OnConnection(RecordWorkerState<Stream>)
          .Build();

  auto started = runtime.Start();
  if (!started.HasValue()) {
    if (IsEnvironmentSkip(started.Error())) {
      std::print("SKIP: runtime backend unavailable: {}\n", started.Error().message());
      return true;
    }
    std::print("FAIL: worker hook Runtime failed to start: {}\n", started.Error().message());
    return false;
  }
  const int started_before_stop = observed.started.load();

  auto client = ConnectLoopback(*port);
  const bool handled =
      client.HasValue() &&
      WaitFor([] { return g_handler_saw_worker_state.load(std::memory_order_acquire) != -1; });
  if (client.HasValue()) {
    (void)::close(*client);
  }
  runtime.Stop();

  return Check(started_before_stop == static_cast<int>(kWorkers),
               "every worker must run its start hook before Start() returns") &&
         Check(handled && g_handler_saw_worker_state.load() == 1,
               "a connection handler must see the state its worker's start hook created") &&
         Check(observed.stopped.load() == static_cast<int>(kWorkers),
               "every started worker must run its stop hook") &&
         Check(observed.waiters_finished.load() == static_cast<int>(kWorkers),
               "waiters woken by the stop hook must finish before the Loop is destroyed") &&
         Check(observed.hooks_in_loop_context.load(),
               "worker hooks must run on the worker thread in its Loop scheduling context");
}

// A failing start hook fails Start() with its error; the stop hook only runs
// for workers whose start hook succeeded.
template <typename Backend, typename Loop, typename Stream>
bool CheckWorkerStartFailure() {
  std::atomic<int> stopped{0};
  auto runtime = Runtime::Builder<Backend>{net::Endpoint::Loopback(0)}
                     .Workers(1)
                     .OnWorkerStart([](Loop&, std::size_t) -> Result<void> {
                       return std::unexpected(Errno(EACCES));
                     })
                     .OnWorkerStop([&stopped](Loop&, std::size_t) { ++stopped; })
                     .OnConnection(RecordWorkerState<Stream>)
                     .Build();
  auto started = runtime.Start();
  if (!started.HasValue() && IsEnvironmentSkip(started.Error())) {
    std::print("SKIP: runtime backend unavailable: {}\n", started.Error().message());
    return true;
  }
  return Check(!started.HasValue() && started.Error().value() == EACCES,
               "Start() must report the start hook's error") &&
         Check(stopped.load() == 0, "a worker whose start hook failed must not run its stop hook");
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
  if (!Check(!missing_started.HasValue() && missing_started.Error() == std::errc::invalid_argument,
             "Epoll Runtime must reject a missing connection handler")) {
    return false;
  }

  auto zero_workers = Runtime::Builder<runtime::Epoll>{net::Endpoint::Loopback(0)}
                          .Workers(0)
                          .OnConnection(HandleEpoll)
                          .Build();
  auto zero_workers_started = zero_workers.Start();
  if (!Check(!zero_workers_started.HasValue() && zero_workers_started.Error().value() == EINVAL,
             "Epoll Runtime must reject zero workers")) {
    return false;
  }

  auto runtime = Runtime::Create<runtime::Epoll>(net::Endpoint::Loopback(0), HandleEpoll);
  auto started = runtime.Start();
  if (!Check(started.HasValue(), "Epoll Runtime failed to start")) {
    return false;
  }
  const bool was_started = runtime.Started();
  runtime.Stop();
  runtime.Stop();
  auto restarted = runtime.Start();
  return Check(was_started, "Epoll Runtime did not report started") &&
         Check(!runtime.Started(), "Epoll Runtime did not stop") &&
         Check(!restarted.HasValue() && restarted.Error().value() == EALREADY,
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
  if (!Check(ran.HasValue(), "Epoll Runtime::Run failed")) {
    return false;
  }
  auto restarted = runtime.Start();
  return Check(!runtime.Started(), "Epoll Runtime::Run returned before stopping workers") &&
         Check(!restarted.HasValue() && restarted.Error().value() == EALREADY,
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
  return Check(run_result.has_value() && run_result->HasValue(),
               "Epoll Runtime::Run failed after RequestStop") &&
         Check(!runtime.Started(), "Epoll Runtime::Run did not join after RequestStop");
}

bool CheckEpollRequestStopFromForeignThread() {
  auto runtime = Runtime::Builder<runtime::Epoll>{net::Endpoint::Loopback(0)}
                     .Workers(1)
                     .OnConnection(HandleEpoll)
                     .Build();
  auto started = runtime.Start();
  if (!Check(started.HasValue(), "Epoll Runtime failed to start for RequestStop")) {
    return false;
  }

  std::jthread requester{[&runtime] { runtime.RequestStop(); }};
  requester.join();

  const bool draining = runtime.Started();
  runtime.Stop();
  return Check(draining, "Epoll RequestStop must not join workers") &&
         Check(!runtime.Started(), "Epoll Stop must join requested workers");
}

bool CheckEpollTcpOptions() {
  return CheckAcceptedTcpOptions<runtime::Epoll, epoll::Stream>(
             std::nullopt, 0, "Epoll Runtime must keep the OS default without Tcp()") &&
         CheckAcceptedTcpOptions<runtime::Epoll, epoll::Stream>(
             net::TcpOptions{.no_delay = true}, 1,
             "Epoll Runtime must apply Tcp() options to accepted streams");
}

bool CheckEpollWorkerHooks() {
  return CheckWorkerHooks<runtime::Epoll, epoll::Loop, epoll::Stream>() &&
         CheckWorkerStartFailure<runtime::Epoll, epoll::Loop, epoll::Stream>();
}

bool CheckEpollStartFailureCanRetry() {
  auto reserved = BindLoopbackPort();
  if (!Check(reserved.HasValue(), "failed to reserve Epoll retry test port")) {
    return false;
  }

  auto runtime = Runtime::Builder<runtime::Epoll>{net::Endpoint::Loopback(reserved->port())}
                     .Workers(1)
                     .OnConnection(HandleEpoll)
                     .Build();
  auto rejected = runtime.Start();
  if (!Check(!rejected.HasValue() && rejected.Error().value() == EADDRINUSE,
             "Epoll Runtime must report an occupied port")) {
    return false;
  }

  reserved->Close();
  auto started = runtime.Start();
  if (!Check(started.HasValue(), "Epoll Runtime could not retry after bind failure")) {
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

bool CheckUringRuntime() {
  auto zero_workers = Runtime::Builder<runtime::Uring>{net::Endpoint::Loopback(0)}
                          .Workers(0)
                          .OnConnection(HandleUring)
                          .Build();
  auto zero_workers_started = zero_workers.Start();
  if (!Check(!zero_workers_started.HasValue() && zero_workers_started.Error().value() == EINVAL,
             "luring Runtime must reject zero workers")) {
    return false;
  }

  auto runtime = Runtime::Create<runtime::Uring>(net::Endpoint::Loopback(0), HandleUring);
  auto started = runtime.Start();
  if (!started.HasValue()) {
    if (IsEnvironmentSkip(started.Error())) {
      std::print("SKIP: io_uring unavailable: {}\n", started.Error().message());
      return true;
    }
    std::print("FAIL: luring Runtime failed to start: {}\n", started.Error().message());
    return false;
  }
  const bool was_started = runtime.Started();
  runtime.Stop();
  runtime.Stop();
  auto restarted = runtime.Start();
  return Check(was_started, "luring Runtime did not report started") &&
         Check(!runtime.Started(), "luring Runtime did not stop") &&
         Check(!restarted.HasValue() && restarted.Error().value() == EALREADY,
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
  if (!ran.HasValue()) {
    if (IsEnvironmentSkip(ran.Error())) {
      std::print("SKIP: io_uring unavailable: {}\n", ran.Error().message());
      return true;
    }
    std::print("FAIL: luring Runtime::Run failed: {}\n", ran.Error().message());
    return false;
  }
  auto restarted = runtime.Start();
  return Check(!runtime.Started(), "luring Runtime::Run returned before stopping workers") &&
         Check(!restarted.HasValue() && restarted.Error().value() == EALREADY,
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
    if (run_result.has_value() && !run_result->HasValue() &&
        IsEnvironmentSkip(run_result->Error())) {
      std::print("SKIP: io_uring unavailable: {}\n", run_result->Error().message());
      return true;
    }
    return Check(false, "luring Runtime::Run did not start");
  }

  runtime.RequestStop();
  runner.join();
  if (!Check(run_result.has_value(), "luring Runtime::Run did not return a result")) {
    return false;
  }
  if (!run_result->HasValue()) {
    if (IsEnvironmentSkip(run_result->Error())) {
      std::print("SKIP: io_uring unavailable: {}\n", run_result->Error().message());
      return true;
    }
    std::print("FAIL: luring Runtime::Run failed after RequestStop: {}\n",
               run_result->Error().message());
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
  if (!started.HasValue()) {
    if (IsEnvironmentSkip(started.Error())) {
      std::print("SKIP: io_uring unavailable: {}\n", started.Error().message());
      return true;
    }
    std::print("FAIL: luring Runtime failed to start for RequestStop: {}\n",
               started.Error().message());
    return false;
  }

  std::jthread requester{[&runtime] { runtime.RequestStop(); }};
  requester.join();

  const bool draining = runtime.Started();
  runtime.Stop();
  return Check(draining, "luring RequestStop must not join workers") &&
         Check(!runtime.Started(), "luring Stop must join requested workers");
}

bool CheckUringTcpOptions() {
  return CheckAcceptedTcpOptions<runtime::Uring, uring::Stream>(
             std::nullopt, 0, "luring Runtime must keep the OS default without Tcp()") &&
         CheckAcceptedTcpOptions<runtime::Uring, uring::Stream>(
             net::TcpOptions{.no_delay = true}, 1,
             "luring Runtime must apply Tcp() options to accepted streams");
}

bool CheckUringWorkerHooks() {
  return CheckWorkerHooks<runtime::Uring, uring::Loop, uring::Stream>() &&
         CheckWorkerStartFailure<runtime::Uring, uring::Loop, uring::Stream>();
}

bool CheckUringStartFailureCanRetry() {
  auto reserved = BindLoopbackPort();
  if (!Check(reserved.HasValue(), "failed to reserve luring retry test port")) {
    return false;
  }

  auto runtime = Runtime::Builder<runtime::Uring>{net::Endpoint::Loopback(reserved->port())}
                     .Workers(1)
                     .OnConnection(HandleUring)
                     .Build();
  auto rejected = runtime.Start();
  if (!rejected.HasValue() && IsEnvironmentSkip(rejected.Error())) {
    std::print("SKIP: io_uring unavailable: {}\n", rejected.Error().message());
    return true;
  }
  if (!Check(!rejected.HasValue() && rejected.Error().value() == EADDRINUSE,
             "luring Runtime must report an occupied port")) {
    return false;
  }

  reserved->Close();
  auto started = runtime.Start();
  if (!started.HasValue()) {
    std::print("FAIL: luring Runtime could not retry after bind failure: {}\n",
               started.Error().message());
    return false;
  }
  runtime.Stop();
  return Check(!runtime.Started(), "luring retry Runtime did not stop");
}

#endif

// --- Graceful shutdown (ShutdownGrace / OnWorkerDrain) ---

std::atomic<int> g_drain_answered{0};
std::atomic<int> g_drain_canceled{0};
std::atomic<int> g_drain_idle_open{0};

// Reads one command byte. 's' answers after 300 ms, 'z' after 10 s (longer
// than any grace period used here); anything else answers at once.
template <class Stream>
coro::DetachedTask ServeCommand(Stream stream) {
  std::array<std::byte, 8> buffer{};
  auto read = co_await stream.Read(buffer);
  if (!read.HasValue() || *read == 0) {
    co_return;
  }
  if (buffer[0] == std::byte{'s'} || buffer[0] == std::byte{'z'}) {
    const auto hold = buffer[0] == std::byte{'s'} ? std::chrono::milliseconds(300)
                                                  : std::chrono::milliseconds(10'000);
    auto slept = co_await SleepFor(*stream.OwnerLoop(), hold);
    if (!slept.HasValue()) {
      ++g_drain_canceled;
      co_return;
    }
  }
  const std::array<std::byte, 4> done{std::byte{'d'}, std::byte{'o'}, std::byte{'n'},
                                      std::byte{'e'}};
  if ((co_await stream.Write(done)).HasValue()) {
    ++g_drain_answered;
  }
}

// Idle keep-alive connections that the drain hook wakes.
template <class Stream>
thread_local std::vector<Stream*> t_idle_streams;

template <class Stream>
coro::DetachedTask ServeKeepAlive(Stream stream) {
  t_idle_streams<Stream>.push_back(&stream);
  ++g_drain_idle_open;
  std::array<std::byte, 8> buffer{};
  for (;;) {
    auto read = co_await stream.Read(buffer);
    if (!read.HasValue() || *read == 0) {
      break;
    }
  }
  std::erase(t_idle_streams<Stream>, &stream);
  --g_drain_idle_open;
}

template <class Loop, class Stream>
void WakeIdleStreams(Loop&, std::size_t) {
  for (Stream* stream : t_idle_streams<Stream>) {
    (void)stream->SetReadDeadline(time::SteadyNow());
  }
}

// Sends one command byte and returns the reply read until EOF (3 s limit).
std::string SendCommand(int fd, char command) {
  if (::send(fd, &command, 1, MSG_NOSIGNAL) != 1) {
    return "send failed";
  }
  timeval limit{.tv_sec = 3, .tv_usec = 0};
  (void)::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &limit, sizeof(limit));
  std::string reply;
  char chunk[16];
  for (;;) {
    const ssize_t n = ::recv(fd, chunk, sizeof(chunk), 0);
    if (n <= 0) {
      break;
    }
    reply.append(chunk, static_cast<std::size_t>(n));
  }
  return reply;
}

bool ConnectRefused(std::uint16_t port) {
  auto late = ConnectLoopback(port);
  if (late.HasValue()) {
    (void)::close(*late);
    return false;
  }
  return late.Error() == std::errc::connection_refused;
}

// A request in flight when stop begins still gets its answer, new
// connections are refused, and the drain ends with the last handler.
template <class Backend, class Stream>
bool CheckDrainFinishesInFlightRequests(const char* name) {
  auto port = PickLoopbackPort();
  if (!Check(port.HasValue(), "drain test could not pick a port")) return false;
  g_drain_answered = 0;
  g_drain_canceled = 0;
  auto runtime = Runtime::Builder<Backend>{net::Endpoint::Loopback(*port)}
                     .Workers(2)
                     .ShutdownGrace(std::chrono::seconds(2))
                     .OnConnection([](Stream stream) { return ServeCommand(std::move(stream)); })
                     .Build();
  auto started = runtime.Start();
  if (!started.HasValue() && IsEnvironmentSkip(started.Error())) return true;
  if (!Check(started.HasValue(), "drain test runtime failed to start")) return false;

  auto client = ConnectLoopback(*port);
  if (!Check(client.HasValue(), "drain test client failed to connect")) return false;
  std::string reply;
  std::jthread reader([&] { reply = SendCommand(*client, 's'); });
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  const auto stop_began = std::chrono::steady_clock::now();
  runtime.RequestStop();
  const bool refused = WaitFor([&] { return ConnectRefused(*port); });
  reader.join();
  runtime.Stop();
  const auto stopped_after = std::chrono::steady_clock::now() - stop_began;
  (void)::close(*client);

  std::print("{} drain: answered={} canceled={} stop took {} ms\n", name, g_drain_answered.load(),
             g_drain_canceled.load(),
             std::chrono::duration_cast<std::chrono::milliseconds>(stopped_after).count());
  return Check(reply == "done", "a request in flight when stop began must still be answered") &&
         Check(refused, "new connections must be refused once the drain began") &&
         Check(stopped_after < std::chrono::milliseconds(1500),
               "the drain must end with the last handler, not wait for the grace period") &&
         Check(g_drain_canceled == 0, "work within the grace period must not be canceled");
}

// OnWorkerDrain wakes idle keep-alive readers so the drain ends promptly.
template <class Backend, class Loop, class Stream>
bool CheckDrainHookWakesIdleConnections() {
  auto port = PickLoopbackPort();
  if (!Check(port.HasValue(), "drain hook test could not pick a port")) return false;
  g_drain_idle_open = 0;
  auto runtime = Runtime::Builder<Backend>{net::Endpoint::Loopback(*port)}
                     .Workers(1)
                     .ShutdownGrace(std::chrono::seconds(5))
                     .OnWorkerDrain(&WakeIdleStreams<Loop, Stream>)
                     .OnConnection([](Stream stream) { return ServeKeepAlive(std::move(stream)); })
                     .Build();
  auto started = runtime.Start();
  if (!started.HasValue() && IsEnvironmentSkip(started.Error())) return true;
  if (!Check(started.HasValue(), "drain hook runtime failed to start")) return false;

  auto client = ConnectLoopback(*port);
  if (!Check(client.HasValue(), "drain hook client failed to connect")) return false;
  const bool opened = WaitFor([] { return g_drain_idle_open.load() == 1; });

  const auto stop_began = std::chrono::steady_clock::now();
  runtime.RequestStop();
  runtime.Stop();
  const auto stopped_after = std::chrono::steady_clock::now() - stop_began;
  char byte = 0;
  const ssize_t eof = ::recv(*client, &byte, 1, 0);
  (void)::close(*client);

  return Check(opened, "the idle connection must reach its handler") &&
         Check(stopped_after < std::chrono::seconds(1),
               "the drain hook must let idle connections end before the grace period") &&
         Check(eof == 0, "an idle client must see its connection close") &&
         Check(g_drain_idle_open == 0, "the idle handler must finish");
}

// Handlers still running when the grace period ends are canceled; a second
// RequestStop() cancels them without waiting for it.
template <class Backend, class Stream>
bool CheckDrainGraceEnds(std::chrono::milliseconds grace, bool second_request) {
  auto port = PickLoopbackPort();
  if (!Check(port.HasValue(), "grace test could not pick a port")) return false;
  g_drain_canceled = 0;
  auto runtime = Runtime::Builder<Backend>{net::Endpoint::Loopback(*port)}
                     .Workers(1)
                     .ShutdownGrace(grace)
                     .OnConnection([](Stream stream) { return ServeCommand(std::move(stream)); })
                     .Build();
  auto started = runtime.Start();
  if (!started.HasValue() && IsEnvironmentSkip(started.Error())) return true;
  if (!Check(started.HasValue(), "grace test runtime failed to start")) return false;

  auto client = ConnectLoopback(*port);
  if (!Check(client.HasValue(), "grace test client failed to connect")) return false;
  if (::send(*client, "z", 1, MSG_NOSIGNAL) != 1) return Check(false, "grace test send failed");
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  const auto stop_began = std::chrono::steady_clock::now();
  runtime.RequestStop();
  if (second_request) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    runtime.RequestStop();
  }
  runtime.Stop();
  const auto stopped_after = std::chrono::steady_clock::now() - stop_began;
  (void)::close(*client);

  const auto limit =
      second_request ? std::chrono::milliseconds(1000) : grace + std::chrono::milliseconds(1000);
  return Check(stopped_after < limit, second_request
                                          ? "a second RequestStop must cut the drain short"
                                          : "the drain must end when the grace period does") &&
         Check(second_request || stopped_after >= grace - std::chrono::milliseconds(20),
               "the drain must wait for the grace period while a handler runs") &&
         Check(g_drain_canceled == 1, "a handler past the grace period must be canceled");
}

template <class Backend, class Loop, class Stream>
bool CheckGracefulShutdown(const char* name) {
  return CheckDrainFinishesInFlightRequests<Backend, Stream>(name) &&
         CheckDrainHookWakesIdleConnections<Backend, Loop, Stream>() &&
         CheckDrainGraceEnds<Backend, Stream>(std::chrono::milliseconds(200), false) &&
         CheckDrainGraceEnds<Backend, Stream>(std::chrono::seconds(10), true);
}

}  // namespace

int main() {
  bool ok = CheckEpollRuntime();
  ok = CheckEpollRuntimeRunWithPreCancelledToken() && ok;
  ok = CheckEpollRunStopsFromRuntimeRequest() && ok;
  ok = CheckEpollRequestStopFromForeignThread() && ok;
  ok = CheckEpollStartFailureCanRetry() && ok;
  ok = CheckEpollTcpOptions() && ok;
  ok = CheckEpollWorkerHooks() && ok;
  ok = CheckGracefulShutdown<runtime::Epoll, epoll::Loop, epoll::Stream>("epoll") && ok;
#ifdef ALYRN_ENABLE_URING
  ok = CheckUringRuntime() && ok;
  ok = CheckUringRuntimeRunWithPreCancelledToken() && ok;
  ok = CheckUringRunStopsFromRuntimeRequest() && ok;
  ok = CheckUringRequestStopFromForeignThread() && ok;
  ok = CheckUringStartFailureCanRetry() && ok;
  ok = CheckUringTcpOptions() && ok;
  ok = CheckUringWorkerHooks() && ok;
  ok = CheckGracefulShutdown<runtime::Uring, uring::Loop, uring::Stream>("uring") && ok;
#endif
  return ok ? 0 : 1;
}
