// SPDX-License-Identifier: MIT
#pragma once

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <vector>

#include "alyrn/detail/backend/value_result_state.h"
#include "alyrn/io/recv_source.h"
#include "alyrn/result.h"
#include "alyrn/task.h"
#include "alyrn/net/recv_source.h"
#include "alyrn/detail/operation/completion_gate.h"
#include "alyrn/detail/operation/scheduler_continuation.h"
#include "alyrn/detail/kqueue/channel.h"
#include "alyrn/detail/kqueue/loop_shutdown.h"
#include "alyrn/kqueue/loop.h"
#include "alyrn/detail/utils/macros.h"

namespace alyrn::kqueue {

struct RecvSourceOptions {
  net::RecvSourceOptions source{};
  std::size_t buffer_size{16 * 1024};

  [[nodiscard]]
  bool Valid() const noexcept;
};

// A loop-affine receive event source implemented with non-blocking recv and
// readiness-driven drain. The fd is borrowed and must outlive the source. A
// source must also outlive every BufferLease returned by Next().
class RecvSource final {
public:
  ALYRN_DELETE_COPY(RecvSource);

  using Event = net::RecvEvent;
  using NextResult = alyrn::Result<std::optional<Event>>;

  // Direct awaiter for the single-consumer receive loop. It keeps Next() on
  // the caller's coroutine frame, matching the uring source path and
  // avoiding a child Task frame for every event.
  class NextAwaiter {
  public:
    explicit NextAwaiter(RecvSource& source) noexcept : source_(&source) {}

    bool await_ready() const noexcept { return false; }

    bool await_suspend(std::coroutine_handle<> continuation) noexcept;

    NextResult await_resume() noexcept;

    void Complete(NextResult result) noexcept;

  private:
    RecvSource* source_;
    ::alyrn::detail::operation::SchedulerContinuation continuation_;
    ::alyrn::detail::operation::CompletionGate completion_gate_;
    ::alyrn::detail::backend::ValueResultState<std::optional<Event>> result_;
  };

  [[nodiscard]]
  static Result<RecvSource> Create(Loop* loop, int fd,
                                                RecvSourceOptions options = {}) noexcept;

  ~RecvSource();

  RecvSource(RecvSource&& other) noexcept;
  RecvSource& operator=(RecvSource&& other) noexcept;

  [[nodiscard]]
  NextAwaiter Next() noexcept {
    return NextAwaiter(*this);
  }

  // Stops new readiness admission without waiting for queued events or
  // BufferLease instances. An owning consumer drains those events and then
  // awaits Stop() for the final ownership boundary.
  [[nodiscard]]
  Result<void> RequestStop() noexcept;

  ::alyrn::Task<Result<void>> Stop();

private:
  class StopAwaiter;

  RecvSource(Loop* loop, int fd, net::detail::RecvSourceStateMachine state,
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

  Loop* loop_{nullptr};
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

static_assert(::alyrn::io::AsyncRecvSource<RecvSource>);

}  // namespace alyrn::kqueue
