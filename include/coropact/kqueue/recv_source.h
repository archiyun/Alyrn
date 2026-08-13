// SPDX-License-Identifier: MIT
#pragma once

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <vector>

#include "coropact/backend/detail/value_result_state.h"
#include "coropact/backend/recv_source.h"
#include "coropact/result.h"
#include "coropact/coro/task.h"
#include "coropact/net/recv_source.h"
#include "coropact/operation/detail/completion_gate.h"
#include "coropact/operation/detail/scheduler_continuation.h"
#include "coropact/kqueue/detail/channel.h"
#include "coropact/kqueue/detail/loop_shutdown.h"
#include "coropact/kqueue/loop.h"
#include "coropact/utils/macros.h"

namespace coropact::kqueue {

struct KqueueRecvSourceOptions {
  net::RecvSourceOptions source{};
  std::size_t buffer_size{16 * 1024};

  [[nodiscard]]
  bool Valid() const noexcept;
};

// A loop-affine receive event source implemented with non-blocking recv and
// readiness-driven drain. The fd is borrowed and must outlive the source. A
// source must also outlive every BufferLease returned by Next().
class KqueueRecvSource final {
public:
  COROPACT_DELETE_COPY(KqueueRecvSource);

  using Event = net::RecvEvent;
  using NextResult = coropact::Result<std::optional<Event>>;

  // Direct awaiter for the single-consumer receive loop. It keeps Next() on
  // the caller's coroutine frame, matching the luring source path and
  // avoiding a child Task frame for every event.
  class NextAwaiter {
  public:
    explicit NextAwaiter(KqueueRecvSource& source) noexcept : source_(&source) {}

    [[nodiscard]]
    bool await_ready() const noexcept {
      return false;
    }

    bool await_suspend(std::coroutine_handle<> continuation) noexcept;

    NextResult await_resume() noexcept;

    void Complete(NextResult result) noexcept;

  private:
    KqueueRecvSource* source_;
    operation::detail::SchedulerContinuation continuation_;
    operation::detail::CompletionGate completion_gate_;
    backend::detail::ValueResultState<std::optional<Event>> result_;
  };

  [[nodiscard]]
  static Result<KqueueRecvSource> Create(KqueueLoop* loop, int fd,
                                                KqueueRecvSourceOptions options = {}) noexcept;

  ~KqueueRecvSource();

  KqueueRecvSource(KqueueRecvSource&& other) noexcept;
  KqueueRecvSource& operator=(KqueueRecvSource&& other) noexcept;

  [[nodiscard]]
  NextAwaiter Next() noexcept {
    return NextAwaiter(*this);
  }

  // Stops new readiness admission without waiting for queued events or
  // BufferLease instances. An owning consumer drains those events and then
  // awaits Stop() for the final ownership boundary.
  [[nodiscard]]
  Result<void> RequestStop() noexcept;

  coro::Task<Result<void>> Stop();

private:
  class StopAwaiter;

  KqueueRecvSource(KqueueLoop* loop, int fd, net::detail::RecvSourceStateMachine state,
                    std::size_t buffer_size, std::vector<std::byte> storage,
                    std::vector<std::uint32_t> available_buffers) noexcept;

  [[nodiscard]]
  Result<void> Start() noexcept;

  [[nodiscard]]
  Result<bool> BeginStop() noexcept;

  void EnsureAdmission() noexcept;
  void RequestBackendPause() noexcept;
  void RequestBackendStop(std::optional<Error> error = std::nullopt) noexcept;
  void CompleteReadiness() noexcept;
  void OnReady() noexcept;
  void OnClose() noexcept;
  void OnError() noexcept;

  void DeliverNextIfReady() noexcept;
  void CompleteStopIfReady() noexcept;
  bool TryTakeNext(NextResult& result) noexcept;

  void ReturnBuffer(std::uint32_t buffer_id) noexcept;
  static void ReclaimBuffer(void* context, std::uint32_t buffer_id) noexcept;

  void DetachChannel() noexcept;
  void BindChannelCallbacks() noexcept;
  static void DispatchLoopStop(void* context) noexcept;

  KqueueLoop* loop_{nullptr};
  int fd_{-1};
  net::detail::RecvSourceStateMachine state_;
  detail::Channel channel_;
  std::deque<Event> events_;
  std::optional<Error> terminal_error_;

  NextAwaiter* pending_next_{nullptr};
  StopAwaiter* pending_stop_{nullptr};

  std::size_t buffer_size_{0};
  std::vector<std::byte> storage_;
  std::vector<std::uint32_t> available_buffers_;
  detail::LoopShutdownParticipant shutdown_participant_{this, &DispatchLoopStop};
};

static_assert(backend::AsyncRecvSource<KqueueRecvSource>);

}  // namespace coropact::kqueue
