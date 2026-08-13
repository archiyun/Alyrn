// SPDX-License-Identifier: MIT
#pragma once

#include <sys/uio.h>

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <span>

#include "coropact/result.h"
#include "coropact/coro/task.h"
#include "coropact/kqueue/detail/channel.h"
#include "coropact/kqueue/detail/loop_shutdown.h"
#include "coropact/kqueue/detail/op_hook.h"
#include "coropact/kqueue/detail/result_state.h"
#include "coropact/kqueue/loop.h"
#include "coropact/kqueue/options.h"
#include "coropact/net/buffer.h"
#include "coropact/net/detail/stream_lifecycle.h"
#include "coropact/net/endpoint.h"
#include "coropact/net/read_into.h"
#include "coropact/net/socket.h"
#include "coropact/operation/detail/scheduler_continuation.h"
#include "coropact/operation/detail/single_result_lifecycle.h"
#include "coropact/time/clock.h"
#include "coropact/time/timer_id.h"
#include "coropact/utils/macros.h"

namespace coropact::kqueue {

/*
 * One-shot readiness is the only supported stream mode for now: each await arms
 * a filter, delivery retires it, and the next await must re-arm. Timed reads
 * share the loop's user-space timer tree rather than a per-op kernel timer.
 */
struct KqueueStreamOptions {
  TriggerMode trigger_mode{TriggerMode::kOneShot};
};

class KqueueStream {
private:
  class ReadSomeAwaiter;
  class ReadIntoAwaiter;
  class ReadAwaiterState;
  class WriteAllAwaiter;

public:
  COROPACT_DELETE_COPY(KqueueStream);

  KqueueStream(KqueueLoop* loop, int fd, net::Endpoint peer = net::Endpoint(0),
               KqueueStreamOptions options = {});
  ~KqueueStream();

  KqueueStream(KqueueStream&& other) noexcept;
  KqueueStream& operator=(KqueueStream&& other) noexcept;

  [[nodiscard]]
  ReadSomeAwaiter ReadSome(std::span<std::byte> buffer) noexcept;
  [[nodiscard]]
  ReadSomeAwaiter ReadSomeFor(std::span<std::byte> buffer, time::Duration timeout) noexcept;
  [[nodiscard]]
  ReadIntoAwaiter ReadInto(net::Buffer buffer, std::size_t reserve = 4096) noexcept;
  [[nodiscard]]
  WriteAllAwaiter WriteAll(std::span<const std::byte> buffer) noexcept;
  coro::Task<Result<void>> Shutdown();
  coro::Task<Result<void>> Close();

  [[nodiscard]]
  const net::Endpoint& PeerAddress() const noexcept {
    return peer_;
  }

  [[nodiscard]]
  int Fd() const noexcept {
    return socket_.fd();
  }

  [[nodiscard]]
  KqueueLoop* Loop() const noexcept {
    return loop_;
  }

private:
  using Channel = detail::Channel;
  using LoopShutdownParticipant = detail::LoopShutdownParticipant;
  using SchedulerContinuation = operation::detail::SchedulerContinuation;
  using SingleResultLifecycle = operation::detail::SingleResultLifecycle;
  using IoResultState = detail::KqueueIoResultState;

  template <class Awaiter>
  using OperationHook = detail::KqueueOperationHook<Awaiter>;

  void HandleRead();
  void HandleWrite();
  void HandleClose();
  void HandleError();

  static void DispatchRead(void* context) noexcept;
  static void DispatchWrite(void* context) noexcept;
  static void DispatchClose(void* context) noexcept;
  static void DispatchError(void* context) noexcept;

  void CompleteRead(Result<std::size_t> result);
  void CompleteWrite(Result<std::size_t> result);
  void CloseNow() noexcept;
  void DetachChannel();
  void RequireOwnerLoop() const noexcept;
  void BindChannelCallbacks() noexcept;
  void ResetForMove() noexcept;
  static KqueueLoop* PrepareMove(KqueueStream& other) noexcept;
  static void DispatchLoopStop(void* context) noexcept;

  KqueueLoop* loop_;
  net::Socket socket_;
  Channel channel_;
  net::Endpoint peer_;
  enum class PendingReadKind : std::uint8_t {
    kNone,
    kReadSome,
    kReadInto,
  };
  void* pending_read_{nullptr};
  WriteAllAwaiter* pending_write_{nullptr};
  PendingReadKind pending_read_kind_{PendingReadKind::kNone};
  net::detail::StreamLifecycle lifecycle_;
  LoopShutdownParticipant shutdown_participant_{this, &DispatchLoopStop};
};

class KqueueStream::ReadAwaiterState {
public:
  [[nodiscard]]
  bool TryAuthorizeRelease() noexcept;
  [[nodiscard]]
  bool TryAuthorizeContinuation() noexcept;
  void ScheduleContinuation() noexcept;

protected:
  explicit ReadAwaiterState(KqueueStream& stream) noexcept : stream_(&stream) {}

  [[nodiscard]]
  bool BeginRead(std::coroutine_handle<> continuation) noexcept;
  void SuspendForRead(void* awaiter, PendingReadKind kind) noexcept;

  void ArmReadTimeout(time::Duration timeout, void* awaiter, time::TimerId& timer) noexcept;
  void CancelReadTimeout(time::TimerId& timer) noexcept;

  [[nodiscard]]
  bool TryAuthorizeResult() noexcept;
  void CompleteInline(Result<std::size_t> result) noexcept;
  void CompleteStoredInline() noexcept;
  void SetResult(Result<std::size_t> result) noexcept;

  [[nodiscard]]
  Result<std::size_t> TakeResult() noexcept;

  KqueueStream* stream_;
  SchedulerContinuation continuation_;
  SingleResultLifecycle lifecycle_;
  IoResultState result_;
};

class KqueueStream::ReadSomeAwaiter final : public ReadAwaiterState,
                                            public OperationHook<KqueueStream::ReadSomeAwaiter> {
public:
  COROPACT_DELETE_COPY_MOVE(ReadSomeAwaiter);

  ReadSomeAwaiter(KqueueStream& stream, std::span<std::byte> buffer,
                  time::Duration timeout = time::Duration::zero()) noexcept
      : ReadAwaiterState(stream), buffer_(buffer), timeout_(timeout) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }
  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  Result<std::size_t> await_resume() noexcept;

private:
  friend OperationHook<ReadSomeAwaiter>;

  bool CompleteResultImpl(Result<std::size_t> result) noexcept;
  void OnReadyImpl() noexcept;

  std::span<std::byte> buffer_;
  time::Duration timeout_{};
  time::TimerId timer_{};
};

class KqueueStream::WriteAllAwaiter final : public OperationHook<KqueueStream::WriteAllAwaiter> {
public:
  COROPACT_DELETE_COPY_MOVE(WriteAllAwaiter);

  WriteAllAwaiter(KqueueStream& stream, std::span<const std::byte> buffer) noexcept
      : stream_(&stream), buffer_(buffer) {}

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }
  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  Result<void> await_resume() noexcept;

private:
  friend class KqueueStream;
  friend OperationHook<WriteAllAwaiter>;

  void CompleteInline(Result<std::size_t> result) noexcept;
  [[nodiscard]] bool CompleteResultImpl(Result<std::size_t> result) noexcept;
  [[nodiscard]] bool TryAuthorizeRelease() noexcept;
  [[nodiscard]] bool TryAuthorizeContinuation() noexcept;
  void ScheduleContinuation() noexcept;
  void OnReadyImpl() noexcept;

  KqueueStream* stream_;
  std::span<const std::byte> buffer_;
  SchedulerContinuation continuation_;
  SingleResultLifecycle lifecycle_;
  IoResultState result_;
};

class KqueueStream::ReadIntoAwaiter : public ReadAwaiterState,
                                      public OperationHook<KqueueStream::ReadIntoAwaiter> {
public:
  COROPACT_DELETE_COPY_MOVE(ReadIntoAwaiter);

  ReadIntoAwaiter(KqueueStream& stream, net::Buffer buffer, std::size_t reserve) noexcept;

  [[nodiscard]]
  bool await_ready() const noexcept {
    return false;
  }
  [[nodiscard]]
  bool await_suspend(std::coroutine_handle<> continuation) noexcept;
  net::ReadIntoOutcome await_resume() noexcept;

private:
  friend OperationHook<ReadIntoAwaiter>;

  bool CompleteResultImpl(Result<std::size_t> result) noexcept;
  void OnReadyImpl() noexcept;
  bool PrepareReservation() noexcept;
  void FinishAttempt(Result<std::size_t> result) noexcept;

  net::Buffer buffer_;
  std::size_t reserve_;
  bool reservation_active_{false};
};

}  // namespace coropact::kqueue
