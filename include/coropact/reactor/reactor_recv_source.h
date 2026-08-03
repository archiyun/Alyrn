// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <vector>

#include "coropact/base/error.h"
#include "coropact/coro/task.h"
#include "coropact/backend/recv_source.h"
#include "coropact/net/recv_source.h"
#include "coropact/reactor/channel.h"
#include "coropact/reactor/event_loop.h"
#include "coropact/utils/macros.h"

namespace coropact::reactor {

struct ReactorRecvSourceOptions {
  net::RecvSourceOptions source{};
  std::size_t buffer_size{16 * 1024};

  [[nodiscard]]
  bool Valid() const noexcept;
};

// A loop-affine receive event source implemented with non-blocking recv and
// readiness-driven drain. The fd is borrowed and must outlive the source. A
// source must also outlive every BufferLease returned by Next().
class ReactorRecvSource final {
public:
  COROPACT_DELETE_COPY(ReactorRecvSource);

  using Event = net::RecvEvent;
  using Result = base::Result<std::optional<Event>>;

  [[nodiscard]]
  static base::Result<ReactorRecvSource> Create(
      EventLoop* loop,
      int fd,
      ReactorRecvSourceOptions options = {}) noexcept;

  ~ReactorRecvSource();

  ReactorRecvSource(ReactorRecvSource&& other) noexcept;
  ReactorRecvSource& operator=(ReactorRecvSource&& other) noexcept;

  coro::Task<Result> Next();

  // Stops new readiness admission without waiting for queued events or
  // BufferLease instances. An owning consumer drains those events and then
  // awaits Stop() for the final ownership boundary.
  [[nodiscard]]
  base::Result<void> RequestStop() noexcept;

  coro::Task<base::Result<void>> Stop();

private:
  class NextAwaiter;
  class StopAwaiter;

  ReactorRecvSource(
      EventLoop* loop,
      int fd,
      net::detail::RecvSourceStateMachine state,
      std::size_t buffer_size,
      std::vector<std::byte> storage,
      std::vector<std::uint32_t> available_buffers) noexcept;

  [[nodiscard]]
  base::Result<void> Start() noexcept;

  [[nodiscard]]
  base::Result<bool> BeginStop() noexcept;

  void EnsureAdmission() noexcept;
  void RequestBackendStop(std::optional<base::Error> error = std::nullopt) noexcept;
  void CompleteReadiness() noexcept;
  void OnReady() noexcept;
  void OnClose() noexcept;
  void OnError() noexcept;

  void DeliverNextIfReady() noexcept;
  void CompleteStopIfReady() noexcept;
  bool TryTakeNext(Result& result) noexcept;

  void ReturnBuffer(std::uint32_t buffer_id) noexcept;
  static void ReclaimBuffer(void* context, std::uint32_t buffer_id) noexcept;

  void DetachChannel() noexcept;
  void BindChannelCallbacks() noexcept;

  EventLoop* loop_{nullptr};
  int fd_{-1};
  net::detail::RecvSourceStateMachine state_;
  Channel channel_;
  std::deque<Event> events_;
  std::optional<base::Error> terminal_error_;

  NextAwaiter* pending_next_{nullptr};
  StopAwaiter* pending_stop_{nullptr};

  std::size_t buffer_size_{0};
  std::vector<std::byte> storage_;
  std::vector<std::uint32_t> available_buffers_;
};

static_assert(backend::AsyncRecvSource<ReactorRecvSource>);

}  // namespace coropact::reactor
