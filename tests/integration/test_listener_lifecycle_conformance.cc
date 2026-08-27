// SPDX-License-Identifier: MIT
// Runs the same application-observable listener and AcceptSource lifecycle
// scenarios against every enabled network backend.

#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <expected>
#include <functional>
#include <iostream>
#include <optional>
#include <string_view>
#include <system_error>
#include <utility>

#include "alyrn/backend/accept_source.h"
#include "alyrn/result.h"
#include "alyrn/coro/awaitable.h"
#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/reactor/listener.h"
#include "alyrn/reactor/loop.h"

#if defined(ALYRN_ENABLE_URING)
#include "alyrn/luring/listener.h"
#include "alyrn/luring/loop.h"
#include "alyrn/luring/options.h"
#endif

namespace {

using VoidResult = alyrn::Result<void>;

static_assert(alyrn::backend::AsyncAcceptSource<alyrn::reactor::AcceptSource>);
static_assert(alyrn::coro::Awaiter<
              decltype(std::declval<alyrn::reactor::AcceptSource&>().Next())>);

#if defined(ALYRN_ENABLE_URING)
static_assert(alyrn::backend::AsyncAcceptSource<alyrn::luring::AcceptSource>);
static_assert(alyrn::coro::Awaiter<
              decltype(std::declval<alyrn::luring::AcceptSource&>().Next())>);
#endif

bool Expect(bool condition, std::string_view backend, std::string_view message) {
  if (condition) {
    return true;
  }
  std::cerr << "FAIL [" << backend << "]: " << message << '\n';
  return false;
}

struct EpollHarness {
  using Loop = alyrn::reactor::Loop;
  using Listener = alyrn::reactor::Listener;
  using Source = alyrn::reactor::AcceptSource;

  static constexpr std::string_view Name() noexcept { return "Reactor"; }
  static bool Init(Loop&) noexcept { return true; }
  static bool Skip() noexcept { return false; }

  static alyrn::Result<Listener> CreateListener(Loop& loop) noexcept {
    return Listener::Create(&loop, alyrn::net::Endpoint::Loopback(0));
  }

  static bool RunAfter(Loop& loop, std::chrono::milliseconds delay,
                       std::function<void()> callback) {
    loop.RunAfter(std::chrono::duration_cast<alyrn::time::Duration>(delay), std::move(callback));
    return true;
  }

  static void Run(Loop& loop) { loop.Run(); }
};

#if defined(ALYRN_ENABLE_URING)
struct UringHarness {
  using Loop = alyrn::luring::Loop;
  using Listener = alyrn::luring::Listener;
  using Source = alyrn::luring::AcceptSource;

  static constexpr std::string_view Name() noexcept { return "io_uring"; }

  static bool Init(Loop& loop) noexcept {
    alyrn::luring::Options options;
    options.entries = 32;
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

  static alyrn::Result<Listener> CreateListener(Loop& loop) noexcept {
    return Listener::Create(&loop, alyrn::net::Endpoint::Loopback(0));
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

  static inline alyrn::Error init_error{};
};
#endif

template <class Harness>
bool PrepareLoopAndListener(typename Harness::Loop& loop,
                            alyrn::Result<typename Harness::Listener>& listener) {
  if (!Harness::Init(loop)) {
    if (Harness::Skip()) {
      std::cout << "SKIP [" << Harness::Name() << "]: backend unavailable\n";
      return false;
    }
    std::cerr << "FAIL [" << Harness::Name() << "]: loop initialization\n";
    return false;
  }

  listener = Harness::CreateListener(loop);
  if (!listener.has_value()) {
    std::cerr << "FAIL [" << Harness::Name()
              << "]: listener creation: " << listener.error().message() << '\n';
    return false;
  }
  return true;
}

template <class Listener>
struct PendingAcceptCloseObservation {
  using AcceptResult = alyrn::Result<typename Listener::StreamType>;

  std::optional<AcceptResult> accept;
  std::optional<VoidResult> close;
  int accept_resume_count{0};
  int finished{0};
  bool accept_with_scheduler{false};
  bool close_with_scheduler{false};
  bool timed_out{false};
};

template <class Listener, class Loop>
auto ObservePendingAccept(Listener& listener, Loop& loop,
                          PendingAcceptCloseObservation<Listener>& observation)
    -> alyrn::coro::DetachedTask {
  observation.accept.emplace(co_await listener.Accept());
  ++observation.accept_resume_count;
  observation.accept_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
  if (++observation.finished == 2) {
    loop.RequestStop();
  }
}

template <class Listener, class Loop>
auto ClosePendingAccept(Listener& listener, Loop& loop,
                        PendingAcceptCloseObservation<Listener>& observation)
    -> alyrn::coro::DetachedTask {
  observation.close.emplace(co_await listener.Close());
  observation.close_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
  if (++observation.finished == 2) {
    loop.RequestStop();
  }
}

template <class Harness>
bool CheckPendingAcceptCloseContract() {
  typename Harness::Loop loop;
  alyrn::Result<typename Harness::Listener> listener =
      std::unexpected(alyrn::Errno(EINVAL));
  if (!PrepareLoopAndListener<Harness>(loop, listener)) {
    return Harness::Skip();
  }

  PendingAcceptCloseObservation<typename Harness::Listener> observation;
  alyrn::coro::SpawnDetach(loop, ObservePendingAccept(*listener, loop, observation));
  alyrn::coro::SpawnDetach(loop, ClosePendingAccept(*listener, loop, observation));
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(500), [&] {
        observation.timed_out = true;
        loop.RequestStop();
      })) {
    return false;
  }

  Harness::Run(loop);

  bool ok = true;
  ok &= Expect(!observation.timed_out, Harness::Name(), "pending Accept Close timed out");
  ok &= Expect(observation.accept.has_value() && !observation.accept->has_value() &&
                   observation.accept->error() == std::errc::operation_canceled,
               Harness::Name(), "Close did not cancel pending Accept with ECANCELED");
  ok &= Expect(observation.close.has_value() && observation.close->has_value(), Harness::Name(),
               "listener Close did not converge");
  ok &= Expect(observation.accept_resume_count == 1, Harness::Name(),
               "cancelled Accept resumed more than once");
  ok &= Expect(observation.accept_with_scheduler && observation.close_with_scheduler,
               Harness::Name(), "listener Close lost scheduler affinity");
  return ok;
}

struct ClosedListenerObservation {
  std::optional<VoidResult> first_close;
  std::optional<VoidResult> second_close;
  bool accept_rejected{false};
  bool source_rejected{false};
  bool address_rejected{false};
  bool resumed_with_scheduler{false};
};

template <class Listener, class Loop>
auto ObserveClosedListener(Listener& listener, Loop& loop, ClosedListenerObservation& observation)
    -> alyrn::coro::DetachedTask {
  observation.first_close.emplace(co_await listener.Close());
  observation.second_close.emplace(co_await listener.Close());

  auto accepted = co_await listener.Accept();
  observation.accept_rejected =
      !accepted.has_value() && accepted.error() == std::errc::bad_file_descriptor;

  auto source = listener.CreateAcceptSource();
  observation.source_rejected =
      !source.has_value() && source.error() == std::errc::bad_file_descriptor;

  auto address = listener.LocalAddress();
  observation.address_rejected =
      !address.has_value() && address.error() == std::errc::bad_file_descriptor;
  observation.resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
  loop.RequestStop();
}

template <class Harness>
bool CheckClosedListenerContract() {
  typename Harness::Loop loop;
  alyrn::Result<typename Harness::Listener> listener =
      std::unexpected(alyrn::Errno(EINVAL));
  if (!PrepareLoopAndListener<Harness>(loop, listener)) {
    return Harness::Skip();
  }

  ClosedListenerObservation observation;
  alyrn::coro::SpawnDetach(loop, ObserveClosedListener(*listener, loop, observation));
  Harness::Run(loop);

  return Expect(observation.first_close.has_value() && observation.first_close->has_value(),
                Harness::Name(), "first listener Close failed") &&
         Expect(observation.second_close.has_value() && observation.second_close->has_value(),
                Harness::Name(), "listener Close was not idempotent") &&
         Expect(observation.accept_rejected, Harness::Name(),
                "Accept after Close did not return EBADF") &&
         Expect(observation.source_rejected, Harness::Name(),
                "AcceptSource after Close did not return EBADF") &&
         Expect(observation.address_rejected, Harness::Name(),
                "LocalAddress after Close did not return EBADF") &&
         Expect(observation.resumed_with_scheduler, Harness::Name(),
                "closed-listener operations lost scheduler affinity");
}

struct StoppingListenerObservation {
  bool accept_rejected{false};
  bool source_rejected{false};
  bool resumed_with_scheduler{false};
};

template <class Listener, class Loop>
auto ObserveListenerAfterStopRequest(Listener& listener, Loop& loop,
                                     StoppingListenerObservation& observation)
    -> alyrn::coro::DetachedTask {
  loop.RequestStop();
  auto accepted = co_await listener.Accept();
  observation.accept_rejected =
      !accepted.has_value() && accepted.error() == std::errc::operation_canceled;

  auto source = listener.CreateAcceptSource();
  observation.source_rejected =
      !source.has_value() && source.error() == std::errc::operation_canceled;
  observation.resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
}

template <class Harness>
bool CheckListenerAfterStopRequestContract() {
  typename Harness::Loop loop;
  alyrn::Result<typename Harness::Listener> listener =
      std::unexpected(alyrn::Errno(EINVAL));
  if (!PrepareLoopAndListener<Harness>(loop, listener)) {
    return Harness::Skip();
  }

  StoppingListenerObservation observation;
  alyrn::coro::SpawnDetach(loop, ObserveListenerAfterStopRequest(*listener, loop, observation));
  Harness::Run(loop);

  return Expect(observation.accept_rejected, Harness::Name(),
                "Accept succeeded after loop stop was requested") &&
         Expect(observation.source_rejected, Harness::Name(),
                "AcceptSource succeeded after loop stop was requested") &&
         Expect(observation.resumed_with_scheduler, Harness::Name(),
                "post-stop listener operation lost scheduler affinity");
}

template <class Source>
struct SourceStopObservation {
  std::optional<typename Source::NextResult> pending_next;
  std::optional<typename Source::NextResult> sticky_next;
  std::optional<VoidResult> first_stop;
  std::optional<VoidResult> second_stop;
  int next_resume_count{0};
  int finished{0};
  bool next_with_scheduler{false};
  bool stop_with_scheduler{false};
  bool timed_out{false};
};

template <class Source, class Loop>
auto ObservePendingNext(Source& source, Loop& loop, SourceStopObservation<Source>& observation)
    -> alyrn::coro::DetachedTask {
  observation.pending_next.emplace(co_await source.Next());
  ++observation.next_resume_count;
  observation.next_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
  if (++observation.finished == 2) {
    loop.RequestStop();
  }
}

template <class Source, class Loop>
auto StopSource(Source& source, Loop& loop, SourceStopObservation<Source>& observation)
    -> alyrn::coro::DetachedTask {
  observation.first_stop.emplace(co_await source.Stop());
  observation.second_stop.emplace(co_await source.Stop());
  observation.sticky_next.emplace(co_await source.Next());
  observation.stop_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
  if (++observation.finished == 2) {
    loop.RequestStop();
  }
}

template <class Harness>
bool CheckSourceStopContract() {
  typename Harness::Loop loop;
  alyrn::Result<typename Harness::Listener> listener =
      std::unexpected(alyrn::Errno(EINVAL));
  if (!PrepareLoopAndListener<Harness>(loop, listener)) {
    return Harness::Skip();
  }

  auto source_result = listener->CreateAcceptSource();
  if (!source_result.has_value()) {
    std::cerr << "FAIL [" << Harness::Name()
              << "]: AcceptSource creation: " << source_result.error().message() << '\n';
    return false;
  }
  typename Harness::Source source = std::move(*source_result);
  SourceStopObservation<typename Harness::Source> observation;

  alyrn::coro::SpawnDetach(loop, ObservePendingNext(source, loop, observation));
  alyrn::coro::SpawnDetach(loop, StopSource(source, loop, observation));
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(500), [&] {
        observation.timed_out = true;
        loop.RequestStop();
      })) {
    return false;
  }
  Harness::Run(loop);

  bool ok = true;
  ok &= Expect(!observation.timed_out, Harness::Name(), "AcceptSource Stop timed out");
  ok &= Expect(observation.pending_next.has_value() && observation.pending_next->has_value() &&
                   !observation.pending_next->value().has_value(),
               Harness::Name(), "Stop did not end a pending Next normally");
  ok &= Expect(observation.sticky_next.has_value() && observation.sticky_next->has_value() &&
                   !observation.sticky_next->value().has_value(),
               Harness::Name(), "AcceptSource terminal result was not sticky");
  ok &= Expect(observation.first_stop.has_value() && observation.first_stop->has_value() &&
                   observation.second_stop.has_value() && observation.second_stop->has_value(),
               Harness::Name(), "AcceptSource Stop was not idempotent");
  ok &= Expect(observation.next_resume_count == 1, Harness::Name(),
               "stopped AcceptSource::Next resumed more than once");
  ok &= Expect(observation.next_with_scheduler && observation.stop_with_scheduler, Harness::Name(),
               "AcceptSource Stop lost scheduler affinity");
  return ok;
}

template <class Source>
struct TerminalAfterLoopStopObservation {
  std::optional<VoidResult> stop;
  std::optional<typename Source::NextResult> terminal;
  bool terminal_with_scheduler{false};
};

template <class Source, class Loop>
auto StopSourceThenObserveTerminalAfterLoopStop(
    Source& source, Loop& loop, TerminalAfterLoopStopObservation<Source>& observation)
    -> alyrn::coro::DetachedTask {
  observation.stop.emplace(co_await source.Stop());
  loop.RequestStop();
  observation.terminal.emplace(co_await source.Next());
  observation.terminal_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
}

// Stopping a loop prevents a source in Idle from opening new backend work,
// but it must not hide an already-terminal logical result. This is especially
// important for direct Next awaiters, which can complete inline after the
// loop has entered Stopping.
template <class Harness>
bool CheckTerminalNextAfterLoopStopContract() {
  typename Harness::Loop loop;
  alyrn::Result<typename Harness::Listener> listener =
      std::unexpected(alyrn::Errno(EINVAL));
  if (!PrepareLoopAndListener<Harness>(loop, listener)) {
    return Harness::Skip();
  }

  auto source_result = listener->CreateAcceptSource();
  if (!source_result.has_value()) {
    std::cerr << "FAIL [" << Harness::Name()
              << "]: AcceptSource creation: " << source_result.error().message() << '\n';
    return false;
  }
  typename Harness::Source source = std::move(*source_result);
  TerminalAfterLoopStopObservation<typename Harness::Source> observation;

  alyrn::coro::SpawnDetach(
      loop, StopSourceThenObserveTerminalAfterLoopStop(source, loop, observation));
  Harness::Run(loop);

  return Expect(observation.stop.has_value() && observation.stop->has_value(), Harness::Name(),
                "AcceptSource Stop failed before loop shutdown") &&
         Expect(observation.terminal.has_value() && observation.terminal->has_value() &&
                    !observation.terminal->value().has_value(),
                Harness::Name(), "terminal Next changed after loop stop was requested") &&
         Expect(observation.terminal_with_scheduler, Harness::Name(),
                "terminal Next after loop stop lost scheduler affinity");
}

template <class Source>
struct ListenerCloseSourceObservation {
  std::optional<typename Source::NextResult> pending_next;
  std::optional<typename Source::NextResult> sticky_next;
  std::optional<VoidResult> close;
  std::optional<VoidResult> stop;
  int next_resume_count{0};
  int finished{0};
  bool next_with_scheduler{false};
  bool close_with_scheduler{false};
  bool timed_out{false};
};

template <class Source, class Loop>
auto ObserveNextDuringListenerClose(Source& source, Loop& loop,
                                    ListenerCloseSourceObservation<Source>& observation)
    -> alyrn::coro::DetachedTask {
  observation.pending_next.emplace(co_await source.Next());
  ++observation.next_resume_count;
  observation.next_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
  if (++observation.finished == 2) {
    loop.RequestStop();
  }
}

template <class Listener, class Source, class Loop>
auto CloseListenerWithSource(Listener& listener, Source& source, Loop& loop,
                             ListenerCloseSourceObservation<Source>& observation)
    -> alyrn::coro::DetachedTask {
  observation.close.emplace(co_await listener.Close());
  observation.sticky_next.emplace(co_await source.Next());
  observation.stop.emplace(co_await source.Stop());
  observation.close_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
  if (++observation.finished == 2) {
    loop.RequestStop();
  }
}

template <class Harness>
bool CheckListenerCloseSourceContract() {
  typename Harness::Loop loop;
  alyrn::Result<typename Harness::Listener> listener =
      std::unexpected(alyrn::Errno(EINVAL));
  if (!PrepareLoopAndListener<Harness>(loop, listener)) {
    return Harness::Skip();
  }

  auto source_result = listener->CreateAcceptSource();
  if (!source_result.has_value()) {
    std::cerr << "FAIL [" << Harness::Name()
              << "]: AcceptSource creation: " << source_result.error().message() << '\n';
    return false;
  }
  typename Harness::Source source = std::move(*source_result);
  ListenerCloseSourceObservation<typename Harness::Source> observation;

  alyrn::coro::SpawnDetach(loop, ObserveNextDuringListenerClose(source, loop, observation));
  alyrn::coro::SpawnDetach(loop, CloseListenerWithSource(*listener, source, loop, observation));
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(500), [&] {
        observation.timed_out = true;
        loop.RequestStop();
      })) {
    return false;
  }
  Harness::Run(loop);

  bool ok = true;
  ok &= Expect(!observation.timed_out, Harness::Name(), "listener Close with source timed out");
  ok &= Expect(observation.close.has_value() && observation.close->has_value(), Harness::Name(),
               "listener Close with source failed");
  ok &= Expect(observation.pending_next.has_value() && observation.pending_next->has_value() &&
                   !observation.pending_next->value().has_value(),
               Harness::Name(), "listener Close did not end pending source Next");
  ok &= Expect(observation.sticky_next.has_value() && observation.sticky_next->has_value() &&
                   !observation.sticky_next->value().has_value(),
               Harness::Name(), "listener Close did not leave a sticky source terminal");
  ok &= Expect(observation.stop.has_value() && observation.stop->has_value(), Harness::Name(),
               "source Stop failed after listener Close");
  ok &= Expect(observation.next_resume_count == 1, Harness::Name(),
               "listener Close resumed source Next more than once");
  ok &= Expect(observation.next_with_scheduler && observation.close_with_scheduler, Harness::Name(),
               "listener Close with source lost scheduler affinity");
  return ok;
}

int ConnectNonBlocking(const alyrn::net::Endpoint& address) noexcept {
  const int fd = ::socket(address.NativeFamily(), SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    return -1;
  }

  const int connected = ::connect(fd, address.SockAddr(), address.SockAddrLen());
  if (connected == 0 || errno == EINPROGRESS) {
    return fd;
  }

  (void)::close(fd);
  return -1;
}

void CloseClients(std::array<int, 4>& clients) noexcept {
  for (int& fd : clients) {
    if (fd >= 0) {
      (void)::close(fd);
      fd = -1;
    }
  }
}

template <class Listener>
struct SequentialAcceptObservation {
  using AcceptResult = alyrn::Result<typename Listener::StreamType>;

  std::array<int, 4> clients{-1, -1, -1, -1};
  std::optional<AcceptResult> first;
  std::optional<VoidResult> first_close;
  std::optional<AcceptResult> second;
  bool first_stream_valid{false};
  bool second_stream_valid{false};
  bool resumed_with_scheduler{false};
  bool timed_out{false};
};

template <class Listener, class Loop>
auto ObserveSequentialAccept(Listener& listener, Loop& loop,
                             SequentialAcceptObservation<Listener>& observation)
    -> alyrn::coro::DetachedTask {
  observation.first.emplace(co_await listener.Accept());
  observation.first_stream_valid =
      observation.first->has_value() && observation.first->value().Fd() >= 0;
  if (observation.first_stream_valid) {
    observation.first_close.emplace(co_await observation.first->value().Close());
  }
  observation.second.emplace(co_await listener.Accept());
  observation.second_stream_valid =
      observation.second->has_value() && observation.second->value().Fd() >= 0;
  observation.resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
  // RequestStop synchronously asks owner-loop resources to close, so stream
  // validity must be observed before entering the loop shutdown boundary.
  loop.RequestStop();
}

template <class Harness>
bool CheckAcceptReleaseBeforeContinuationContract() {
  typename Harness::Loop loop;
  alyrn::Result<typename Harness::Listener> listener =
      std::unexpected(alyrn::Errno(EINVAL));
  if (!PrepareLoopAndListener<Harness>(loop, listener)) {
    return Harness::Skip();
  }

  auto address = listener->LocalAddress();
  if (!address.has_value()) {
    std::cerr << "FAIL [" << Harness::Name()
              << "]: listener address lookup: " << address.error().message() << '\n';
    return false;
  }

  SequentialAcceptObservation<typename Harness::Listener> observation;
  alyrn::coro::SpawnDetach(loop, ObserveSequentialAccept(*listener, loop, observation));
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(5),
                         [&] {
                           observation.clients[0] = ConnectNonBlocking(*address);
                           observation.clients[1] = ConnectNonBlocking(*address);
                         }) ||
      !Harness::RunAfter(loop, std::chrono::milliseconds(500), [&] {
        observation.timed_out = true;
        loop.RequestStop();
      })) {
    return false;
  }

  Harness::Run(loop);

  const bool clients_connected = observation.clients[0] >= 0 && observation.clients[1] >= 0;
  CloseClients(observation.clients);
  return Expect(!observation.timed_out, Harness::Name(), "sequential Accept timed out") &&
         Expect(clients_connected, Harness::Name(),
                "sequential Accept clients failed to connect") &&
         Expect(observation.first.has_value() && observation.first->has_value() &&
                    observation.first_stream_valid,
                Harness::Name(), "first Accept returned an invalid stream") &&
         Expect(observation.first_close.has_value() && observation.first_close->has_value(),
                Harness::Name(), "accepted stream Close failed immediately after Accept") &&
         Expect(observation.second.has_value() && observation.second->has_value() &&
                    observation.second_stream_valid,
                Harness::Name(),
                "follow-up Accept observed a stale listener reservation instead of a stream") &&
         Expect(observation.resumed_with_scheduler, Harness::Name(),
                "sequential Accept lost scheduler affinity");
}

template <class Source>
bool IsStreamEvent(const std::optional<typename Source::NextResult>& result) {
  return result.has_value() && result->has_value() && result->value().has_value();
}

template <class Source>
struct AcceptSourceAdmissionObservation {
  std::array<int, 4> clients{-1, -1, -1, -1};
  std::optional<typename Source::NextResult> first;
  std::optional<typename Source::NextResult> second;
  std::optional<typename Source::NextResult> third;
  std::optional<typename Source::NextResult> fourth;
  std::optional<typename Source::NextResult> terminal;
  std::optional<VoidResult> stop;
  bool first_with_scheduler{false};
  bool drain_with_scheduler{false};
  bool timed_out{false};
};

template <class Source, class Loop>
auto ObserveFirstAcceptSourceEvent(Source& source, Loop& loop,
                                   AcceptSourceAdmissionObservation<Source>& observation)
    -> alyrn::coro::DetachedTask {
  observation.first.emplace(co_await source.Next());
  observation.first_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
}

template <class Source, class Loop>
auto DrainAcceptSourceAtLowWater(Source& source, Loop& loop, alyrn::net::Endpoint address,
                                 AcceptSourceAdmissionObservation<Source>& observation)
    -> alyrn::coro::DetachedTask {
  observation.second.emplace(co_await source.Next());
  observation.third.emplace(co_await source.Next());
  observation.clients[3] = ConnectNonBlocking(address);
  observation.fourth.emplace(co_await source.Next());
  observation.stop.emplace(co_await source.Stop());
  observation.terminal.emplace(co_await source.Next());
  observation.drain_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
  loop.RequestStop();
}

template <class Source, class Loop>
auto StopAcceptSourceOnTimeout(Source& source, Loop& loop) -> alyrn::coro::DetachedTask {
  (void)(co_await source.Stop());
  loop.RequestStop();
}

template <class Source, class Loop>
auto StopAcceptSourceThenDrain(Source& source, Loop& loop,
                               AcceptSourceAdmissionObservation<Source>& observation)
    -> alyrn::coro::DetachedTask {
  observation.stop.emplace(co_await source.Stop());
  observation.second.emplace(co_await source.Next());
  observation.third.emplace(co_await source.Next());
  observation.terminal.emplace(co_await source.Next());
  observation.drain_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == &loop;
  loop.RequestStop();
}

template <class Harness>
bool CheckAcceptSourceAdmissionTrace() {
  typename Harness::Loop loop;
  alyrn::Result<typename Harness::Listener> listener =
      std::unexpected(alyrn::Errno(EINVAL));
  if (!PrepareLoopAndListener<Harness>(loop, listener)) {
    return Harness::Skip();
  }

  auto source_result = listener->CreateAcceptSource({
      .pending_depth = 1,
      .event_capacity = 2,
      .resume_threshold = 1,
  });
  if (!source_result.has_value()) {
    std::cerr << "FAIL [" << Harness::Name()
              << "]: AcceptSource creation: " << source_result.error().message() << '\n';
    return false;
  }
  typename Harness::Source source = std::move(*source_result);

  auto address = listener->LocalAddress();
  if (!address.has_value()) {
    std::cerr << "FAIL [" << Harness::Name()
              << "]: listener address lookup: " << address.error().message() << '\n';
    return false;
  }

  AcceptSourceAdmissionObservation<typename Harness::Source> observation;
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(5),
                         [&] {
                           for (std::size_t index = 0; index < 3; ++index) {
                             observation.clients[index] = ConnectNonBlocking(*address);
                           }
                         }) ||
      !Harness::RunAfter(loop, std::chrono::milliseconds(30),
                         [&] {
                           alyrn::coro::SpawnDetach(
                               loop,
                               DrainAcceptSourceAtLowWater(source, loop, *address, observation));
                         }) ||
      !Harness::RunAfter(loop, std::chrono::milliseconds(750), [&] {
        observation.timed_out = true;
        alyrn::coro::SpawnDetach(loop, StopAcceptSourceOnTimeout(source, loop));
      })) {
    return false;
  }

  alyrn::coro::SpawnDetach(loop, ObserveFirstAcceptSourceEvent(source, loop, observation));
  Harness::Run(loop);
  bool clients_connected = true;
  for (int fd : observation.clients) {
    clients_connected = clients_connected && fd >= 0;
  }
  CloseClients(observation.clients);

  bool ok = true;
  ok &= Expect(!observation.timed_out, Harness::Name(), "AcceptSource admission trace timed out");
  ok &= Expect(clients_connected, Harness::Name(), "AcceptSource clients failed to connect");
  ok &= Expect(IsStreamEvent<typename Harness::Source>(observation.first), Harness::Name(),
               "AcceptSource did not deliver the first burst event");
  ok &= Expect(IsStreamEvent<typename Harness::Source>(observation.second), Harness::Name(),
               "AcceptSource did not retain the first queued burst event");
  ok &= Expect(IsStreamEvent<typename Harness::Source>(observation.third), Harness::Name(),
               "AcceptSource did not retain the high-water burst event");
  ok &= Expect(IsStreamEvent<typename Harness::Source>(observation.fourth), Harness::Name(),
               "AcceptSource did not re-arm after reaching low-water");
  ok &= Expect(observation.stop.has_value() && observation.stop->has_value(), Harness::Name(),
               "AcceptSource Stop failed after the admission trace");
  ok &= Expect(observation.terminal.has_value() && observation.terminal->has_value() &&
                   !observation.terminal->value().has_value(),
               Harness::Name(), "AcceptSource did not produce its normal terminal result");
  ok &= Expect(observation.first_with_scheduler && observation.drain_with_scheduler,
               Harness::Name(), "AcceptSource admission trace lost scheduler affinity");
  return ok;
}

template <class Harness>
bool CheckAcceptSourceStopDrainsBurstContract() {
  typename Harness::Loop loop;
  alyrn::Result<typename Harness::Listener> listener =
      std::unexpected(alyrn::Errno(EINVAL));
  if (!PrepareLoopAndListener<Harness>(loop, listener)) {
    return Harness::Skip();
  }

  auto source_result = listener->CreateAcceptSource({
      .pending_depth = 1,
      .event_capacity = 2,
      .resume_threshold = 1,
  });
  if (!source_result.has_value()) {
    std::cerr << "FAIL [" << Harness::Name()
              << "]: AcceptSource creation: " << source_result.error().message() << '\n';
    return false;
  }
  typename Harness::Source source = std::move(*source_result);

  auto address = listener->LocalAddress();
  if (!address.has_value()) {
    std::cerr << "FAIL [" << Harness::Name()
              << "]: listener address lookup: " << address.error().message() << '\n';
    return false;
  }

  AcceptSourceAdmissionObservation<typename Harness::Source> observation;
  if (!Harness::RunAfter(loop, std::chrono::milliseconds(5),
                         [&] {
                           for (std::size_t index = 0; index < 3; ++index) {
                             observation.clients[index] = ConnectNonBlocking(*address);
                           }
                         }) ||
      !Harness::RunAfter(loop, std::chrono::milliseconds(30),
                         [&] {
                           alyrn::coro::SpawnDetach(
                               loop, StopAcceptSourceThenDrain(source, loop, observation));
                         }) ||
      !Harness::RunAfter(loop, std::chrono::milliseconds(750), [&] {
        observation.timed_out = true;
        alyrn::coro::SpawnDetach(loop, StopAcceptSourceOnTimeout(source, loop));
      })) {
    return false;
  }

  alyrn::coro::SpawnDetach(loop, ObserveFirstAcceptSourceEvent(source, loop, observation));
  Harness::Run(loop);
  bool clients_connected = true;
  for (std::size_t index = 0; index < 3; ++index) {
    clients_connected = clients_connected && observation.clients[index] >= 0;
  }
  CloseClients(observation.clients);

  bool ok = true;
  ok &= Expect(!observation.timed_out, Harness::Name(), "AcceptSource Stop during burst timed out");
  ok &= Expect(clients_connected, Harness::Name(), "AcceptSource burst clients failed to connect");
  ok &= Expect(IsStreamEvent<typename Harness::Source>(observation.first), Harness::Name(),
               "AcceptSource Stop discarded the first burst event");
  ok &= Expect(IsStreamEvent<typename Harness::Source>(observation.second), Harness::Name(),
               "AcceptSource Stop discarded the first queued burst event");
  ok &= Expect(IsStreamEvent<typename Harness::Source>(observation.third), Harness::Name(),
               "AcceptSource Stop discarded the second queued burst event");
  ok &= Expect(observation.stop.has_value() && observation.stop->has_value(), Harness::Name(),
               "AcceptSource Stop failed during the bounded burst");
  ok &= Expect(observation.terminal.has_value() && observation.terminal->has_value() &&
                   !observation.terminal->value().has_value(),
               Harness::Name(), "AcceptSource Stop did not terminate after draining the burst");
  ok &= Expect(observation.first_with_scheduler && observation.drain_with_scheduler,
               Harness::Name(), "AcceptSource Stop during burst lost scheduler affinity");
  return ok;
}

template <class Harness>
bool RunBackendSuite() {
  return CheckPendingAcceptCloseContract<Harness>() && CheckClosedListenerContract<Harness>() &&
         CheckListenerAfterStopRequestContract<Harness>() &&
         CheckAcceptReleaseBeforeContinuationContract<Harness>() &&
         CheckSourceStopContract<Harness>() && CheckTerminalNextAfterLoopStopContract<Harness>() &&
         CheckListenerCloseSourceContract<Harness>() &&
         CheckAcceptSourceAdmissionTrace<Harness>() &&
         CheckAcceptSourceStopDrainsBurstContract<Harness>();
}

}  // namespace

int main() {
  if (!RunBackendSuite<EpollHarness>()) {
    return 1;
  }
#if defined(ALYRN_ENABLE_URING)
  if (!RunBackendSuite<UringHarness>()) {
    return 1;
  }
#endif
  std::cout << "listener lifecycle conformance: PASS\n";
  return 0;
}
