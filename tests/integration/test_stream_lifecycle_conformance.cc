// SPDX-License-Identifier: MIT
// Runs the same application-observable lifecycle scenarios against every
// enabled stream backend. Backend harnesses own only setup and timer syntax.

#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <expected>
#include <functional>
#include <iostream>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/epoll/loop.h"
#include "alyrn/epoll/stream.h"
#include "alyrn/io/async_stream.h"
#include "alyrn/io/buffer.h"
#include "alyrn/io/recv.h"
#include "alyrn/net/detail/socket.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/result.h"

#if defined(ALYRN_ENABLE_URING)
#include "alyrn/uring/loop.h"
#include "alyrn/uring/options.h"
#include "alyrn/uring/stream.h"
#endif

namespace {

using ReadResult = alyrn::Result<std::size_t>;
using VoidResult = alyrn::Result<void>;
using OwnedRecvOutcome = alyrn::io::RecvOutcome;

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

  int Get() const noexcept { return fd_; }
  int Release() noexcept { return std::exchange(fd_, -1); }

  void Reset(int fd = -1) noexcept {
    if (fd_ >= 0) {
      (void)::close(fd_);
    }
    fd_ = fd;
  }

private:
  int fd_{-1};
};

bool MakeSocketPair(UniqueFd& local, UniqueFd& peer) noexcept {
  std::array<int, 2> fds{-1, -1};
  if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds.data()) < 0) {
    std::cerr << "FAIL: socketpair: " << alyrn::CurrentErrno().message() << '\n';
    return false;
  }
  local.Reset(fds[0]);
  peer.Reset(fds[1]);
  return true;
}

bool WriteExactly(int fd, std::string_view bytes) noexcept {
  while (!bytes.empty()) {
    const ssize_t written = ::write(fd, bytes.data(), bytes.size());
    if (written > 0) {
      bytes.remove_prefix(static_cast<std::size_t>(written));
      continue;
    }
    if (written < 0 && errno == EINTR) {
      continue;
    }
    return false;
  }
  return true;
}

bool FillSendBuffer(int fd) noexcept {
  std::array<std::byte, 16 * 1024> bytes{};
  for (;;) {
    const ssize_t written = ::send(fd, bytes.data(), bytes.size(), MSG_NOSIGNAL);
    if (written > 0) {
      continue;
    }
    if (written < 0 && errno == EINTR) {
      continue;
    }
    return written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK);
  }
}

bool PeerSawEof(int fd) noexcept {
  std::array<char, 1> buf{};
  const ssize_t n = ::recv(fd, buf.data(), buf.size(), 0);
  return n == 0;
}

bool Expect(bool condition, std::string_view backend, std::string_view message) {
  if (condition) {
    return true;
  }
  std::cerr << "FAIL [" << backend << "]: " << message << '\n';
  return false;
}

std::string Gather(const alyrn::io::Buffer& buffer) {
  std::string bytes;
  for (const iovec& part : buffer.ReadableIov(32)) {
    bytes.append(static_cast<const char*>(part.iov_base), part.iov_len);
  }
  return bytes;
}

struct EpollHarness {
  using Loop = alyrn::epoll::Loop;
  using Stream = alyrn::epoll::Stream;

  static constexpr std::string_view Name() noexcept { return "Epoll"; }
  static constexpr bool DropCancelsPendingOperations() noexcept { return true; }
  static bool Init(Loop&) noexcept { return true; }
  static bool Skip(const Loop&) noexcept { return false; }

  static Stream MakeStream(Loop& loop, int fd) {
    return Stream(&loop, fd, alyrn::net::Endpoint::Loopback(0));
  }

  static bool PreparePendingWriteFd(int) noexcept { return true; }

  static bool RunAfter(Loop& loop, std::chrono::milliseconds delay,
                       std::function<void()> callback) {
    loop.RunAfter(std::chrono::duration_cast<alyrn::time::Duration>(delay), std::move(callback));
    return true;
  }

  static void Run(Loop& loop) noexcept { loop.Run(); }
};

#if defined(ALYRN_ENABLE_URING)
struct UringHarness {
  using Loop = alyrn::uring::Loop;
  using Stream = alyrn::uring::Stream;

  static constexpr std::string_view Name() noexcept { return "io_uring"; }
  static constexpr bool DropCancelsPendingOperations() noexcept { return false; }

  static bool Init(Loop& loop) noexcept {
    alyrn::uring::Options options;
    options.entries = 32;
    auto initialized = loop.Init(options);
    if (initialized.has_value()) {
      return true;
    }
    init_error = initialized.error();
    return false;
  }

  static bool Skip(const Loop&) noexcept {
    return init_error == std::errc::operation_not_supported ||
           init_error == std::errc::operation_not_permitted;
  }

  static Stream MakeStream(Loop& loop, int fd) {
    return Stream(&loop, fd, alyrn::net::Endpoint::Loopback(0));
  }

  static bool PreparePendingWriteFd(int fd) noexcept {
    auto blocking = alyrn::net::SetNonBlocking(fd, false);
    if (blocking.has_value()) {
      return true;
    }
    std::cerr << "FAIL [io_uring]: failed to restore blocking socket mode: "
              << blocking.error().message() << '\n';
    return false;
  }

  static bool RunAfter(Loop& loop, std::chrono::milliseconds delay,
                       std::function<void()> callback) {
    auto timer = loop.RunAfter(delay, std::move(callback));
    if (!timer.has_value()) {
      std::cerr << "FAIL [io_uring]: timer setup: " << timer.error().message() << '\n';
      return false;
    }
    return true;
  }

  static void Run(Loop& loop) noexcept { loop.Run(); }

  static inline alyrn::Error init_error{};
};
#endif

struct PendingReadObservation {
  std::optional<ReadResult> result;
  int resume_count{0};
  bool resumed_with_scheduler{false};
  bool timed_out{false};
};

template <alyrn::io::AsyncReadStream Stream, class Loop>
auto ObservePendingRead(Stream& stream, Loop& loop, std::span<std::byte> buffer,
                        PendingReadObservation& observation) -> alyrn::coro::DetachedTask {
  observation.result.emplace(co_await stream.Read(buffer));
  ++observation.resume_count;
  observation.resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
  loop.RequestStop();
}

template <class Harness>
bool CheckPendingReadSuccessContract() {
  typename Harness::Loop loop;
  if (!Harness::Init(loop)) {
    if (Harness::Skip(loop)) {
      return true;
    }
    std::cerr << "FAIL [" << Harness::Name() << "]: loop initialization\n";
    return false;
  }

  UniqueFd local;
  UniqueFd peer;
  if (!MakeSocketPair(local, peer)) {
    return false;
  }

  auto stream = Harness::MakeStream(loop, local.Release());
  std::array<std::byte, 32> buffer{};
  PendingReadObservation observation;
  constexpr std::string_view kPayload = "pending-read";

  alyrn::coro::SpawnDetach(loop, ObservePendingRead(stream, loop, buffer, observation));
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(5),
                         [fd = peer.Get(), kPayload] { (void)WriteExactly(fd, kPayload); })) {
    return false;
  }
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(500), [&] {
        observation.timed_out = true;
        loop.RequestStop();
      })) {
    return false;
  }

  Harness::Run(loop);

  const std::string_view actual(reinterpret_cast<const char*>(buffer.data()), kPayload.size());
  bool ok = true;
  ok &= Expect(!observation.timed_out, Harness::Name(), "pending read timed out");
  ok &= Expect(observation.result.has_value() && observation.result->has_value() &&
                   **observation.result == kPayload.size() && actual == kPayload,
               Harness::Name(), "pending read returned the wrong result");
  ok &=
      Expect(observation.resume_count == 1, Harness::Name(), "pending read resumed more than once");
  ok &= Expect(observation.resumed_with_scheduler, Harness::Name(),
               "pending read lost scheduler affinity");
  return ok;
}

struct SequentialReadObservation {
  std::optional<ReadResult> first;
  std::optional<ReadResult> second;
  bool resumed_with_scheduler{false};
  bool timed_out{false};
};

template <alyrn::io::AsyncReadStream Stream, class Loop>
auto ObserveSequentialRead(Stream& stream, Loop& loop, std::span<std::byte> first_buffer,
                           std::span<std::byte> second_buffer,
                           SequentialReadObservation& observation) -> alyrn::coro::DetachedTask {
  observation.first.emplace(co_await stream.Read(first_buffer));
  observation.second.emplace(co_await stream.Read(second_buffer));
  observation.resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
  loop.RequestStop();
}

template <class Harness>
bool CheckReadReleaseBeforeContinuationContract() {
  typename Harness::Loop loop;
  if (!Harness::Init(loop)) {
    if (Harness::Skip(loop)) {
      return true;
    }
    std::cerr << "FAIL [" << Harness::Name() << "]: loop initialization\n";
    return false;
  }

  UniqueFd local;
  UniqueFd peer;
  if (!MakeSocketPair(local, peer)) {
    return false;
  }

  auto stream = Harness::MakeStream(loop, local.Release());
  constexpr std::string_view kFirstPayload = "first";
  constexpr std::string_view kSecondPayload = "second";
  std::array<std::byte, kFirstPayload.size()> first_buffer{};
  std::array<std::byte, kSecondPayload.size()> second_buffer{};
  SequentialReadObservation observation;
  std::string payload{kFirstPayload};
  payload.append(kSecondPayload);

  alyrn::coro::SpawnDetach(
      loop, ObserveSequentialRead(stream, loop, first_buffer, second_buffer, observation));
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(5),
                         [fd = peer.Get(), payload] { (void)WriteExactly(fd, payload); })) {
    return false;
  }
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(500), [&] {
        observation.timed_out = true;
        loop.RequestStop();
      })) {
    return false;
  }

  Harness::Run(loop);

  const std::string_view first_actual(reinterpret_cast<const char*>(first_buffer.data()),
                                      kFirstPayload.size());
  const std::string_view second_actual(reinterpret_cast<const char*>(second_buffer.data()),
                                       kSecondPayload.size());
  bool ok = true;
  ok &= Expect(!observation.timed_out, Harness::Name(), "sequential read timed out");
  ok &= Expect(observation.first.has_value() && observation.first->has_value() &&
                   **observation.first == kFirstPayload.size() && first_actual == kFirstPayload,
               Harness::Name(), "first sequential read returned the wrong result");
  ok &= Expect(observation.second.has_value() && observation.second->has_value() &&
                   **observation.second == kSecondPayload.size() && second_actual == kSecondPayload,
               Harness::Name(),
               "follow-up read observed a stale pending slot instead of the next payload");
  ok &= Expect(observation.resumed_with_scheduler, Harness::Name(),
               "sequential read lost scheduler affinity");
  return ok;
}

struct OwnedReadObservation {
  std::optional<OwnedRecvOutcome> outcome;
  int resume_count{0};
  bool resumed_with_scheduler{false};
  bool timed_out{false};
};

template <alyrn::io::AsyncRecvStream Stream, class Loop>
auto ObserveOwnedRead(Stream& stream, Loop& loop, alyrn::io::Buffer buffer,
                      OwnedReadObservation& observation) -> alyrn::coro::DetachedTask {
  observation.outcome.emplace(co_await stream.Recv(std::move(buffer), 32));
  ++observation.resume_count;
  observation.resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
  loop.RequestStop();
}

template <class Harness>
bool CheckPendingOwnedReadSuccessContract() {
  typename Harness::Loop loop;
  if (!Harness::Init(loop)) {
    if (Harness::Skip(loop)) {
      return true;
    }
    std::cerr << "FAIL [" << Harness::Name() << "]: loop initialization\n";
    return false;
  }

  UniqueFd local;
  UniqueFd peer;
  if (!MakeSocketPair(local, peer)) {
    return false;
  }

  auto stream = Harness::MakeStream(loop, local.Release());
  alyrn::io::Buffer buffer;
  buffer.Append("prefix:");
  OwnedReadObservation observation;
  constexpr std::string_view kPayload = "owned-read";

  alyrn::coro::SpawnDetach(loop, ObserveOwnedRead(stream, loop, std::move(buffer), observation));
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(5),
                         [fd = peer.Get(), kPayload] { (void)WriteExactly(fd, kPayload); })) {
    return false;
  }
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(500), [&] {
        observation.timed_out = true;
        loop.RequestStop();
      })) {
    return false;
  }

  Harness::Run(loop);

  bool ok = true;
  ok &= Expect(!observation.timed_out, Harness::Name(), "pending Recv timed out");
  ok &= Expect(observation.outcome.has_value() && observation.outcome->result.has_value() &&
                   *observation.outcome->result == kPayload.size(),
               Harness::Name(), "pending Recv returned the wrong result");
  ok &= Expect(
      observation.outcome.has_value() && Gather(observation.outcome->buffer) == "prefix:owned-read",
      Harness::Name(), "Recv did not commit into the returned owner");
  ok &=
      Expect(observation.resume_count == 1, Harness::Name(), "pending Recv resumed more than once");
  ok &= Expect(observation.resumed_with_scheduler, Harness::Name(),
               "pending Recv lost scheduler affinity");
  return ok;
}

struct ConcurrentReadObservation {
  std::optional<ReadResult> first;
  std::optional<ReadResult> second;
  int first_resume_count{0};
  int second_resume_count{0};
  int finished{0};
  bool timed_out{false};
};

template <alyrn::io::AsyncReadStream Stream, class Loop>
auto ObserveFirstRead(Stream& stream, Loop& loop, std::span<std::byte> buffer,
                      ConcurrentReadObservation& observation) -> alyrn::coro::DetachedTask {
  observation.first.emplace(co_await stream.Read(buffer));
  ++observation.first_resume_count;
  if (++observation.finished == 2) {
    loop.RequestStop();
  }
}

template <alyrn::io::AsyncReadStream Stream, class Loop>
auto ObserveCompetingEmptyRead(Stream& stream, Loop& loop, ConcurrentReadObservation& observation)
    -> alyrn::coro::DetachedTask {
  observation.second.emplace(co_await stream.Read(std::span<std::byte>{}));
  ++observation.second_resume_count;
  if (++observation.finished == 2) {
    loop.RequestStop();
  }
}

template <class Harness>
bool CheckReadLaneExclusivityContract() {
  typename Harness::Loop loop;
  if (!Harness::Init(loop)) {
    if (Harness::Skip(loop)) {
      return true;
    }
    std::cerr << "FAIL [" << Harness::Name() << "]: loop initialization\n";
    return false;
  }

  UniqueFd local;
  UniqueFd peer;
  if (!MakeSocketPair(local, peer)) {
    return false;
  }

  auto stream = Harness::MakeStream(loop, local.Release());
  std::array<std::byte, 32> buffer{};
  ConcurrentReadObservation observation;
  constexpr std::string_view kPayload = "first-read";

  alyrn::coro::SpawnDetach(loop, ObserveFirstRead(stream, loop, buffer, observation));
  alyrn::coro::SpawnDetach(loop, ObserveCompetingEmptyRead(stream, loop, observation));
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(5),
                         [fd = peer.Get(), kPayload] { (void)WriteExactly(fd, kPayload); })) {
    return false;
  }
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(500), [&] {
        observation.timed_out = true;
        loop.RequestStop();
      })) {
    return false;
  }

  Harness::Run(loop);

  bool ok = true;
  ok &= Expect(!observation.timed_out, Harness::Name(), "concurrent read test timed out");
  ok &= Expect(observation.first.has_value() && observation.first->has_value() &&
                   **observation.first == kPayload.size(),
               Harness::Name(), "first pending read did not complete");
  ok &= Expect(observation.second.has_value() && !observation.second->has_value() &&
                   observation.second->error() == std::errc::device_or_resource_busy,
               Harness::Name(), "second read did not return EBUSY");
  ok &= Expect(observation.first_resume_count == 1 && observation.second_resume_count == 1,
               Harness::Name(), "a competing read resumed more than once");
  return ok;
}

template <alyrn::io::AsyncRecvStream Stream, class Loop>
auto ObserveOwnedReadDuringLoopStop(Stream& stream, Loop& loop, alyrn::io::Buffer buffer,
                                    OwnedReadObservation& observation)
    -> alyrn::coro::DetachedTask {
  observation.outcome.emplace(co_await stream.Recv(std::move(buffer), 32));
  ++observation.resume_count;
  observation.resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
}

template <class Harness>
bool CheckOwnedReadLoopStopContract() {
  typename Harness::Loop loop;
  if (!Harness::Init(loop)) {
    if (Harness::Skip(loop)) {
      return true;
    }
    std::cerr << "FAIL [" << Harness::Name() << "]: loop initialization\n";
    return false;
  }

  UniqueFd local;
  UniqueFd peer;
  if (!MakeSocketPair(local, peer)) {
    return false;
  }

  auto stream = Harness::MakeStream(loop, local.Release());
  alyrn::io::Buffer buffer;
  buffer.Append("preserved");
  OwnedReadObservation observation;

  alyrn::coro::SpawnDetach(
      loop, ObserveOwnedReadDuringLoopStop(stream, loop, std::move(buffer), observation));
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(5), [&loop] { loop.RequestStop(); })) {
    return false;
  }

  Harness::Run(loop);

  bool ok = true;
  ok &= Expect(observation.outcome.has_value() && !observation.outcome->result.has_value() &&
                   observation.outcome->result.error() == std::errc::operation_canceled,
               Harness::Name(), "loop stop did not cancel Recv");
  ok &=
      Expect(observation.outcome.has_value() && Gather(observation.outcome->buffer) == "preserved",
             Harness::Name(), "cancelled Recv did not return the original owner");
  ok &= Expect(observation.resume_count == 1, Harness::Name(),
               "loop-stop Recv resumed more than once");
  ok &= Expect(observation.resumed_with_scheduler, Harness::Name(),
               "loop-stop Recv lost scheduler affinity");
  if (observation.outcome.has_value()) {
    observation.outcome->buffer.Append(":reused");
    ok &= Expect(Gather(observation.outcome->buffer) == "preserved:reused", Harness::Name(),
                 "returned Recv buffer retained an active reservation");
  }
  return ok;
}

struct StopRequestObservation {
  std::optional<ReadResult> read;
  std::optional<VoidResult> write;
  bool resumed_with_scheduler{false};
};

template <alyrn::io::AsyncStream Stream, class Loop>
auto ObserveIoAfterStopRequest(Stream& stream, Loop& loop, StopRequestObservation& observation)
    -> alyrn::coro::DetachedTask {
  loop.RequestStop();
  observation.read.emplace(co_await stream.Read(std::span<std::byte>{}));
  observation.write.emplace(co_await stream.Write(std::span<const std::byte>{}));
  observation.resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
}

template <class Harness>
bool CheckIoAfterStopRequestContract() {
  typename Harness::Loop loop;
  if (!Harness::Init(loop)) {
    if (Harness::Skip(loop)) {
      return true;
    }
    std::cerr << "FAIL [" << Harness::Name() << "]: loop initialization\n";
    return false;
  }

  UniqueFd local;
  UniqueFd peer;
  if (!MakeSocketPair(local, peer)) {
    return false;
  }

  auto stream = Harness::MakeStream(loop, local.Release());
  StopRequestObservation observation;
  alyrn::coro::SpawnDetach(loop, ObserveIoAfterStopRequest(stream, loop, observation));

  Harness::Run(loop);

  return Expect(observation.read.has_value() && !observation.read->has_value() &&
                    observation.read->error() == std::errc::operation_canceled,
                Harness::Name(), "empty read succeeded after loop stop was requested") &&
         Expect(observation.write.has_value() && !observation.write->has_value() &&
                    observation.write->error() == std::errc::operation_canceled,
                Harness::Name(), "empty write succeeded after loop stop was requested") &&
         Expect(observation.resumed_with_scheduler, Harness::Name(),
                "post-stop I/O lost scheduler affinity");
}

struct ShutdownObservation {
  std::optional<VoidResult> first_shutdown;
  std::optional<VoidResult> second_shutdown;
  std::optional<VoidResult> rejected_write;
  std::optional<ReadResult> read;
  bool resumed_with_scheduler{false};
};

template <alyrn::io::AsyncStream Stream, class Loop>
auto ObserveShutdownContract(Stream& stream, Loop& loop, std::span<std::byte> read_buffer,
                             std::span<const std::byte> write_buffer,
                             ShutdownObservation& observation) -> alyrn::coro::DetachedTask {
  observation.first_shutdown.emplace(co_await stream.Shutdown());
  observation.second_shutdown.emplace(co_await stream.Shutdown());
  observation.rejected_write.emplace(co_await stream.Write(write_buffer));
  observation.read.emplace(co_await stream.Read(read_buffer));
  observation.resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
  loop.RequestStop();
}

template <class Harness>
bool CheckShutdownContract() {
  typename Harness::Loop loop;
  if (!Harness::Init(loop)) {
    if (Harness::Skip(loop)) {
      std::cout << "SKIP [" << Harness::Name() << "]: backend unavailable\n";
      return true;
    }
    std::cerr << "FAIL [" << Harness::Name() << "]: loop initialization\n";
    return false;
  }

  UniqueFd local;
  UniqueFd peer;
  if (!MakeSocketPair(local, peer)) {
    return false;
  }

  constexpr std::string_view kReply = "reply";
  if (!WriteExactly(peer.Get(), kReply)) {
    std::cerr << "FAIL [" << Harness::Name() << "]: peer write\n";
    return false;
  }

  auto stream = Harness::MakeStream(loop, local.Release());
  std::array<std::byte, 16> read_buffer{};
  constexpr std::string_view kRejected = "must-not-send";
  const auto write_buffer =
      std::as_bytes(std::span<const char>(kRejected.data(), kRejected.size()));
  ShutdownObservation observation;

  alyrn::coro::SpawnDetach(
      loop, ObserveShutdownContract(stream, loop, read_buffer, write_buffer, observation));
  Harness::Run(loop);

  std::array<char, 1> eof_probe{};
  const ssize_t peer_read = ::read(peer.Get(), eof_probe.data(), eof_probe.size());
  const std::string_view actual(reinterpret_cast<const char*>(read_buffer.data()), kReply.size());

  bool ok = true;
  ok &= Expect(observation.first_shutdown.has_value() && observation.first_shutdown->has_value(),
               Harness::Name(), "first Shutdown failed");
  ok &= Expect(observation.second_shutdown.has_value() && observation.second_shutdown->has_value(),
               Harness::Name(), "Shutdown was not idempotent");
  ok &= Expect(observation.rejected_write.has_value() && !observation.rejected_write->has_value() &&
                   observation.rejected_write->error() == std::errc::broken_pipe,
               Harness::Name(), "write after Shutdown did not return EPIPE");
  ok &= Expect(observation.read.has_value() && observation.read->has_value() &&
                   **observation.read == kReply.size() && actual == kReply,
               Harness::Name(), "read direction did not remain open after Shutdown");
  ok &= Expect(peer_read == 0, Harness::Name(), "peer did not observe write-half EOF");
  ok &= Expect(observation.resumed_with_scheduler, Harness::Name(),
               "continuation lost scheduler affinity");
  return ok;
}

struct CloseReadObservation {
  std::optional<VoidResult> first_close_read;
  std::optional<VoidResult> second_close_read;
  std::optional<ReadResult> read;
  std::optional<VoidResult> write;
  bool resumed_with_scheduler{false};
};

template <alyrn::io::AsyncStream Stream, class Loop>
auto ObserveCloseReadContract(Stream& stream, Loop& loop, std::span<std::byte> read_buffer,
                              std::span<const std::byte> write_buffer,
                              CloseReadObservation& observation) -> alyrn::coro::DetachedTask {
  observation.first_close_read.emplace(co_await stream.CloseRead());
  observation.second_close_read.emplace(co_await stream.CloseRead());
  observation.read.emplace(co_await stream.Read(read_buffer));
  observation.write.emplace(co_await stream.Write(write_buffer));
  observation.resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
  loop.RequestStop();
}

template <class Harness>
bool CheckCloseReadContract() {
  typename Harness::Loop loop;
  if (!Harness::Init(loop)) {
    if (Harness::Skip(loop)) {
      std::cout << "SKIP [" << Harness::Name() << "]: backend unavailable\n";
      return true;
    }
    std::cerr << "FAIL [" << Harness::Name() << "]: loop initialization\n";
    return false;
  }

  UniqueFd local;
  UniqueFd peer;
  if (!MakeSocketPair(local, peer)) {
    return false;
  }

  constexpr std::string_view kDiscarded = "discarded";
  if (!WriteExactly(peer.Get(), kDiscarded)) {
    std::cerr << "FAIL [" << Harness::Name() << "]: peer write\n";
    return false;
  }

  auto stream = Harness::MakeStream(loop, local.Release());
  std::array<std::byte, 16> read_buffer{};
  constexpr std::string_view kOutgoing = "still-writable";
  const auto write_buffer =
      std::as_bytes(std::span<const char>(kOutgoing.data(), kOutgoing.size()));
  CloseReadObservation observation;

  alyrn::coro::SpawnDetach(
      loop, ObserveCloseReadContract(stream, loop, read_buffer, write_buffer, observation));
  Harness::Run(loop);

  std::array<char, 32> peer_buffer{};
  const ssize_t peer_read = ::read(peer.Get(), peer_buffer.data(), peer_buffer.size());
  bool ok = true;
  ok &=
      Expect(observation.first_close_read.has_value() && observation.first_close_read->has_value(),
             Harness::Name(), "first CloseRead failed");
  ok &= Expect(
      observation.second_close_read.has_value() && observation.second_close_read->has_value(),
      Harness::Name(), "CloseRead was not idempotent");
  ok &= Expect(
      observation.read.has_value() && observation.read->has_value() && **observation.read == 0,
      Harness::Name(), "read after CloseRead did not return EOF");
  ok &= Expect(observation.write.has_value() && observation.write->has_value(), Harness::Name(),
               "write after CloseRead failed");
  ok &= Expect(
      peer_read == static_cast<ssize_t>(kOutgoing.size()) &&
          std::string_view(peer_buffer.data(), static_cast<std::size_t>(peer_read)) == kOutgoing,
      Harness::Name(), "CloseRead disabled the write direction");
  ok &= Expect(observation.resumed_with_scheduler, Harness::Name(),
               "CloseRead continuation lost scheduler affinity");
  return ok;
}

struct PendingReadCloseReadObservation {
  std::optional<ReadResult> read;
  std::optional<VoidResult> rejected_close_read;
  std::optional<VoidResult> close_read;
  int finished{0};
  bool timed_out{false};
};

template <alyrn::io::AsyncStream Stream, class Loop>
auto ObservePendingReadForCloseRead(Stream& stream, Loop& loop, std::span<std::byte> buffer,
                                    PendingReadCloseReadObservation& observation)
    -> alyrn::coro::DetachedTask {
  observation.read.emplace(co_await stream.Read(buffer));
  observation.close_read.emplace(co_await stream.CloseRead());
  if (++observation.finished == 2) {
    loop.RequestStop();
  }
}

template <alyrn::io::AsyncStream Stream, class Loop>
auto RejectCloseReadWhileReadPending(Stream& stream, Loop& loop, int peer_fd,
                                     PendingReadCloseReadObservation& observation)
    -> alyrn::coro::DetachedTask {
  observation.rejected_close_read.emplace(co_await stream.CloseRead());
  constexpr std::string_view kPayload = "ready";
  (void)WriteExactly(peer_fd, kPayload);
  if (++observation.finished == 2) {
    loop.RequestStop();
  }
}

template <class Harness>
bool CheckPendingReadCloseReadContract() {
  typename Harness::Loop loop;
  if (!Harness::Init(loop)) {
    if (Harness::Skip(loop)) {
      return true;
    }
    std::cerr << "FAIL [" << Harness::Name() << "]: loop initialization\n";
    return false;
  }

  UniqueFd local;
  UniqueFd peer;
  if (!MakeSocketPair(local, peer)) {
    return false;
  }

  auto stream = Harness::MakeStream(loop, local.Release());
  std::array<std::byte, 16> read_buffer{};
  PendingReadCloseReadObservation observation;
  alyrn::coro::SpawnDetach(loop,
                           ObservePendingReadForCloseRead(stream, loop, read_buffer, observation));
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(1), [&] {
        alyrn::coro::SpawnDetach(
            loop, RejectCloseReadWhileReadPending(stream, loop, peer.Get(), observation));
      })) {
    return false;
  }
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(500), [&] {
        observation.timed_out = true;
        loop.RequestStop();
      })) {
    return false;
  }

  Harness::Run(loop);

  bool ok = true;
  ok &= Expect(!observation.timed_out, Harness::Name(), "pending-read CloseRead timed out");
  ok &= Expect(observation.rejected_close_read.has_value() &&
                   !observation.rejected_close_read->has_value() &&
                   observation.rejected_close_read->error() == std::errc::device_or_resource_busy,
               Harness::Name(), "CloseRead did not return EBUSY for a pending read");
  ok &= Expect(
      observation.read.has_value() && observation.read->has_value() && **observation.read > 0,
      Harness::Name(), "pending read did not complete before CloseRead");
  ok &= Expect(observation.close_read.has_value() && observation.close_read->has_value(),
               Harness::Name(), "CloseRead failed after the pending read drained");
  return ok;
}

struct PendingWriteObservation {
  std::optional<VoidResult> write;
  std::optional<VoidResult> competing_write;
  std::optional<VoidResult> shutdown;
  std::optional<VoidResult> close;
  int write_resume_count{0};
  int finished{0};
  bool write_resumed_with_scheduler{false};
  bool control_resumed_with_scheduler{false};
  bool timed_out{false};
};

template <alyrn::io::AsyncWriteStream Stream, class Loop>
auto ObservePendingWrite(Stream& stream, Loop& loop, std::span<const std::byte> buffer,
                         PendingWriteObservation& observation) -> alyrn::coro::DetachedTask {
  observation.write.emplace(co_await stream.Write(buffer));
  ++observation.write_resume_count;
  observation.write_resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
  if (++observation.finished == 2) {
    loop.RequestStop();
  }
}

template <alyrn::io::AsyncStream Stream, class Loop>
auto ControlPendingWrite(Stream& stream, Loop& loop, PendingWriteObservation& observation)
    -> alyrn::coro::DetachedTask {
  observation.competing_write.emplace(co_await stream.Write(std::span<const std::byte>{}));
  observation.shutdown.emplace(co_await stream.Shutdown());
  observation.close.emplace(co_await stream.Close());
  observation.control_resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
  if (++observation.finished == 2) {
    loop.RequestStop();
  }
}

template <class Harness>
bool CheckPendingWriteCloseContract() {
  typename Harness::Loop loop;
  if (!Harness::Init(loop)) {
    if (Harness::Skip(loop)) {
      return true;
    }
    std::cerr << "FAIL [" << Harness::Name() << "]: loop initialization\n";
    return false;
  }

  UniqueFd local;
  UniqueFd peer;
  if (!MakeSocketPair(local, peer)) {
    return false;
  }
  if (!FillSendBuffer(local.Get())) {
    std::cerr << "FAIL [" << Harness::Name() << "]: failed to fill send buffer\n";
    return false;
  }
  if (!Harness::PreparePendingWriteFd(local.Get())) {
    return false;
  }

  auto stream = Harness::MakeStream(loop, local.Release());
  const std::array<std::byte, 1> payload{std::byte{'x'}};
  PendingWriteObservation observation;

  alyrn::coro::SpawnDetach(loop, ObservePendingWrite(stream, loop, payload, observation));
  alyrn::coro::SpawnDetach(loop, ControlPendingWrite(stream, loop, observation));
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(500), [&] {
        observation.timed_out = true;
        loop.RequestStop();
      })) {
    return false;
  }

  Harness::Run(loop);

  bool ok = true;
  ok &= Expect(!observation.timed_out, Harness::Name(), "pending-write Close timed out");
  ok &= Expect(observation.write.has_value() && !observation.write->has_value() &&
                   observation.write->error() == std::errc::operation_canceled,
               Harness::Name(), "Close did not cancel the pending write");
  ok &=
      Expect(observation.competing_write.has_value() && !observation.competing_write->has_value() &&
                 observation.competing_write->error() == std::errc::device_or_resource_busy,
             Harness::Name(), "empty competing write did not return EBUSY");
  ok &= Expect(observation.shutdown.has_value() && !observation.shutdown->has_value() &&
                   observation.shutdown->error() == std::errc::device_or_resource_busy,
               Harness::Name(), "Shutdown did not return EBUSY for a pending write");
  ok &= Expect(observation.close.has_value() && observation.close->has_value(), Harness::Name(),
               "Close did not converge after cancelling a pending write");
  ok &= Expect(observation.write_resume_count == 1, Harness::Name(),
               "cancelled write resumed more than once");
  ok &=
      Expect(observation.write_resumed_with_scheduler && observation.control_resumed_with_scheduler,
             Harness::Name(), "pending-write Close lost scheduler affinity");
  return ok;
}

struct ClosedStreamObservation {
  std::optional<VoidResult> first_close;
  std::optional<VoidResult> second_close;
  std::optional<ReadResult> read;
  std::optional<VoidResult> write;
  std::optional<VoidResult> shutdown;
  bool resumed_with_scheduler{false};
};

template <alyrn::io::AsyncStream Stream, class Loop>
auto ObserveClosedStream(Stream& stream, Loop& loop, ClosedStreamObservation& observation)
    -> alyrn::coro::DetachedTask {
  observation.first_close.emplace(co_await stream.Close());
  observation.second_close.emplace(co_await stream.Close());
  observation.read.emplace(co_await stream.Read(std::span<std::byte>{}));
  observation.write.emplace(co_await stream.Write(std::span<const std::byte>{}));
  observation.shutdown.emplace(co_await stream.Shutdown());
  observation.resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
  loop.RequestStop();
}

template <class Harness>
bool CheckClosedStreamContract() {
  typename Harness::Loop loop;
  if (!Harness::Init(loop)) {
    if (Harness::Skip(loop)) {
      return true;
    }
    std::cerr << "FAIL [" << Harness::Name() << "]: loop initialization\n";
    return false;
  }

  UniqueFd local;
  UniqueFd peer;
  if (!MakeSocketPair(local, peer)) {
    return false;
  }

  auto stream = Harness::MakeStream(loop, local.Release());
  ClosedStreamObservation observation;
  alyrn::coro::SpawnDetach(loop, ObserveClosedStream(stream, loop, observation));

  Harness::Run(loop);

  bool ok = true;
  ok &= Expect(observation.first_close.has_value() && observation.first_close->has_value(),
               Harness::Name(), "first Close failed");
  ok &= Expect(observation.second_close.has_value() && observation.second_close->has_value(),
               Harness::Name(), "Close was not idempotent");
  ok &= Expect(observation.read.has_value() && !observation.read->has_value() &&
                   observation.read->error() == std::errc::bad_file_descriptor,
               Harness::Name(), "empty read after Close did not return EBADF");
  ok &= Expect(observation.write.has_value() && !observation.write->has_value() &&
                   observation.write->error() == std::errc::bad_file_descriptor,
               Harness::Name(), "empty write after Close did not return EBADF");
  ok &= Expect(observation.shutdown.has_value() && !observation.shutdown->has_value() &&
                   observation.shutdown->error() == std::errc::bad_file_descriptor,
               Harness::Name(), "Shutdown after Close did not return EBADF");
  ok &= Expect(observation.resumed_with_scheduler, Harness::Name(),
               "closed stream operations lost scheduler affinity");
  return ok;
}

struct CloseObservation {
  std::optional<ReadResult> read;
  std::optional<VoidResult> close;
  int read_resume_count{0};
  int finished{0};
  bool resumed_with_scheduler{false};
  bool timed_out{false};
};

template <alyrn::io::AsyncStream Stream, class Loop>
auto ObserveCancelledRead(Stream& stream, Loop& loop, std::span<std::byte> buffer,
                          CloseObservation& observation) -> alyrn::coro::DetachedTask {
  observation.read.emplace(co_await stream.Read(buffer));
  ++observation.read_resume_count;
  observation.resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
  if (++observation.finished == 2) {
    loop.RequestStop();
  }
}

template <alyrn::io::AsyncStream Stream, class Loop>
auto ObserveClose(Stream& stream, Loop& loop, CloseObservation& observation)
    -> alyrn::coro::DetachedTask {
  observation.close.emplace(co_await stream.Close());
  if (++observation.finished == 2) {
    loop.RequestStop();
  }
}

template <class Harness>
bool CheckPendingReadCloseContract() {
  typename Harness::Loop loop;
  if (!Harness::Init(loop)) {
    if (Harness::Skip(loop)) {
      return true;
    }
    std::cerr << "FAIL [" << Harness::Name() << "]: loop initialization\n";
    return false;
  }

  UniqueFd local;
  UniqueFd peer;
  if (!MakeSocketPair(local, peer)) {
    return false;
  }

  auto stream = Harness::MakeStream(loop, local.Release());
  std::array<std::byte, 16> read_buffer{};
  CloseObservation observation;

  alyrn::coro::SpawnDetach(loop, ObserveCancelledRead(stream, loop, read_buffer, observation));
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(1), [&] {
        alyrn::coro::SpawnDetach(loop, ObserveClose(stream, loop, observation));
      })) {
    return false;
  }
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(500), [&] {
        observation.timed_out = true;
        loop.RequestStop();
      })) {
    return false;
  }

  Harness::Run(loop);

  bool ok = true;
  ok &= Expect(!observation.timed_out, Harness::Name(), "pending-read Close timed out");
  ok &= Expect(observation.close.has_value() && observation.close->has_value(), Harness::Name(),
               "Close did not converge successfully");
  ok &= Expect(observation.read.has_value() && !observation.read->has_value() &&
                   observation.read->error() == std::errc::operation_canceled,
               Harness::Name(), "pending read did not return ECANCELED");
  ok &= Expect(observation.read_resume_count == 1, Harness::Name(),
               "pending read resumed more than once");
  ok &= Expect(observation.resumed_with_scheduler, Harness::Name(),
               "cancelled read lost scheduler affinity");
  return ok;
}

template <alyrn::io::AsyncStream Stream, class Loop>
auto DropIdleStream([[maybe_unused]] Stream stream, Loop& loop, bool& dropped)
    -> alyrn::coro::DetachedTask {
  dropped = true;
  loop.RequestStop();
  co_return;
}

template <class Harness>
bool CheckIdleStreamDropContract() {
  typename Harness::Loop loop;
  if (!Harness::Init(loop)) {
    if (Harness::Skip(loop)) {
      return true;
    }
    std::cerr << "FAIL [" << Harness::Name() << "]: loop initialization\n";
    return false;
  }

  UniqueFd local;
  UniqueFd peer;
  if (!MakeSocketPair(local, peer)) {
    return false;
  }

  bool dropped = false;
  alyrn::coro::SpawnDetach(
      loop, DropIdleStream(Harness::MakeStream(loop, local.Release()), loop, dropped));
  Harness::Run(loop);

  bool ok = true;
  ok &= Expect(dropped, Harness::Name(), "idle stream drop task did not run");
  ok &= Expect(PeerSawEof(peer.Get()), Harness::Name(),
               "idle stream drop did not close the peer connection");
  return ok;
}

template <alyrn::io::AsyncStream Stream, class Loop>
auto ObserveDroppedRead(Stream& stream, Loop& loop, std::span<std::byte> buffer,
                        CloseObservation& observation) -> alyrn::coro::DetachedTask {
  observation.read.emplace(co_await stream.Read(buffer));
  ++observation.read_resume_count;
  observation.resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
  loop.RequestStop();
}

template <class Harness>
bool CheckPendingReadDropContract() {
  typename Harness::Loop loop;
  if (!Harness::Init(loop)) {
    if (Harness::Skip(loop)) {
      return true;
    }
    std::cerr << "FAIL [" << Harness::Name() << "]: loop initialization\n";
    return false;
  }

  UniqueFd local;
  UniqueFd peer;
  if (!MakeSocketPair(local, peer)) {
    return false;
  }

  std::optional<typename Harness::Stream> stream{Harness::MakeStream(loop, local.Release())};
  std::array<std::byte, 16> read_buffer{};
  CloseObservation observation;

  alyrn::coro::SpawnDetach(loop, ObserveDroppedRead(*stream, loop, read_buffer, observation));
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(1), [&] { stream.reset(); })) {
    return false;
  }
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(500), [&] {
        observation.timed_out = true;
        loop.RequestStop();
      })) {
    return false;
  }

  Harness::Run(loop);

  bool ok = true;
  ok &= Expect(!observation.timed_out, Harness::Name(), "pending-read drop timed out");
  ok &= Expect(!stream.has_value(), Harness::Name(), "stream was not dropped");
  ok &= Expect(observation.read.has_value() && !observation.read->has_value() &&
                   observation.read->error() == std::errc::operation_canceled,
               Harness::Name(), "pending read did not return ECANCELED after drop");
  ok &= Expect(observation.read_resume_count == 1, Harness::Name(),
               "pending read resumed more than once after drop");
  ok &= Expect(observation.resumed_with_scheduler, Harness::Name(),
               "dropped-stream read lost scheduler affinity");
  ok &= Expect(PeerSawEof(peer.Get()), Harness::Name(),
               "pending-read drop did not close the peer connection");
  return ok;
}

template <class Harness>
bool CheckBackend() {
  const bool pending_read = CheckPendingReadSuccessContract<Harness>();
  const bool sequential_read = CheckReadReleaseBeforeContinuationContract<Harness>();
  const bool owned_read = CheckPendingOwnedReadSuccessContract<Harness>();
  const bool read_lane = CheckReadLaneExclusivityContract<Harness>();
  const bool loop_stop = CheckOwnedReadLoopStopContract<Harness>();
  const bool stop_rejection = CheckIoAfterStopRequestContract<Harness>();
  const bool shutdown = CheckShutdownContract<Harness>();
  const bool close_read = CheckCloseReadContract<Harness>();
  const bool pending_close_read = CheckPendingReadCloseReadContract<Harness>();
  const bool pending_write_close = CheckPendingWriteCloseContract<Harness>();
  const bool closed_stream = CheckClosedStreamContract<Harness>();
  const bool pending_read_close = CheckPendingReadCloseContract<Harness>();
  const bool idle_drop = CheckIdleStreamDropContract<Harness>();
  bool pending_read_drop = true;
  if constexpr (Harness::DropCancelsPendingOperations()) {
    pending_read_drop = CheckPendingReadDropContract<Harness>();
  }
  if (pending_read && sequential_read && owned_read && read_lane && loop_stop && stop_rejection &&
      shutdown && close_read && pending_close_read && pending_write_close && closed_stream &&
      pending_read_close && idle_drop && pending_read_drop) {
    std::cout << "lifecycle conformance [" << Harness::Name() << "]: PASS\n";
  }
  return pending_read && sequential_read && owned_read && read_lane && loop_stop &&
         stop_rejection && shutdown && close_read && pending_close_read && pending_write_close &&
         closed_stream && pending_read_close && idle_drop && pending_read_drop;
}

}  // namespace

int main() {
  bool ok = CheckBackend<EpollHarness>();
#if defined(ALYRN_ENABLE_URING)
  ok &= CheckBackend<UringHarness>();
#endif
  return ok ? 0 : 1;
}
