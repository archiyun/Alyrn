// SPDX-License-Identifier: MIT
#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <stop_token>
#include <thread>

#include "alyrn/coro/detached_task.h"
#include "alyrn/detail/macros.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/result.h"
#include "alyrn/time/clock.h"
#include "alyrn/uring/connector.h"
#include "alyrn/uring/listener.h"
#include "alyrn/uring/loop.h"
#include "alyrn/uring/options.h"

namespace alyrn::uring::detail {

enum class AcceptMode : std::uint8_t {
  kSingleShot,
  kMultishot,
};

struct WorkerContext {
  WorkerContext(std::size_t index, Loop& loop, Listener& listener,
                      Connector& connector) noexcept
      : index(index), loop(loop), listener(listener), connector(connector) {}

  ALYRN_DELETE_COPY_MOVE(WorkerContext);

  const std::size_t index;
  Loop& loop;
  Listener& listener;
  Connector& connector;
  // A ThreadInitCallback may set an error here to fail worker startup without
  // throwing; Start() then reports this error.
  Result<void> start_result{};
  // Owner-thread drain state: connection handlers still running, and whether
  // a graceful shutdown began.
  std::size_t live_connections{0};
  bool draining{false};
};

struct WorkerOptions {
  Options loop_options{};
  ListenOptions listen_options{};

  // Selects the logical accept implementation used by the worker. The
  // multishot source preserves the same ConnectionCallback contract while
  // allowing one native accept request to produce multiple accepted streams.
  AcceptMode accept_mode{AcceptMode::kSingleShot};

  // Optional resource for coroutine frames created while this worker resumes
  // work. The resource must outlive the worker group.
  std::pmr::memory_resource* frame_resource{nullptr};

  // Optional CPU to which this worker thread is pinned. Leave unset to use
  // the process scheduler's normal placement policy.
  std::optional<unsigned> cpu_affinity;
  // How long a drain waits for connection handlers before the loop stops.
  time::Duration shutdown_grace{};
};

class Worker {
public:
  ALYRN_DELETE_COPY_MOVE(Worker);

  using ExitResult = Result<void, std::exception_ptr>;
  // Runs on the worker thread inside the Loop's scheduling context, after the
  // loop, listener, and connector exist and before connections are accepted.
  // It fails startup by throwing or by setting WorkerContext::start_result.
  using ThreadInitCallback = std::function<void(WorkerContext&)>;
  // Runs on the worker thread inside the Loop's scheduling context, after the
  // loop has drained and before loop-bound listener/connector resources are
  // destroyed; work it schedules is drained before those resources go away.
  // Also runs if ThreadInitCallback fails, so it must tolerate partial
  // initialization. Exceptions are retained and returned by Join(); the
  // callback is not retried.
  using ThreadExitCallback = std::function<void(WorkerContext&)>;
  using ConnectionCallback =
      std::function<coro::DetachedTask(WorkerContext&, Stream)>;
  // Runs on the worker thread inside the Loop's scheduling context when a
  // drain begins, after the listener closed.
  using ThreadDrainCallback = std::function<void(WorkerContext&)>;

  Worker(std::size_t index, net::Endpoint listen_addr, WorkerOptions options = {},
         ThreadInitCallback init_callback = {}, ConnectionCallback connection_callback = {},
         ThreadExitCallback exit_callback = {}, ThreadDrainCallback drain_callback = {});
  ~Worker() noexcept;

  Result<void> Start();
  // Requests shutdown without waiting for thread exit. Safe to call while
  // another thread joins.
  void Stop() noexcept;
  // Requests a graceful shutdown: the worker stops accepting, runs the drain
  // callback, and stops its loop once its connection handlers finished or
  // WorkerOptions::shutdown_grace elapsed. Stop() still stops at once.
  // Thread-safe.
  void RequestDrain() noexcept;

  // Waits for thread exit and returns any exception from ThreadExitCallback.
  // Call Stop() first to request shutdown. Join and lifecycle calls must be
  // serialized on a non-worker thread. Repeated joins preserve the result;
  // a new Start() attempt clears it. Destruction alone discards the result.
  [[nodiscard]] ExitResult Join() noexcept;

  std::size_t Index() const noexcept { return index_; }

private:
  void WorkLoop(std::stop_token token) noexcept;
  void BeginDrain(WorkerContext& context) noexcept;

  std::size_t index_;
  net::Endpoint listen_addr_;
  WorkerOptions options_;
  ThreadInitCallback init_callback_;
  ConnectionCallback connection_callback_;
  ThreadExitCallback exit_callback_;
  ThreadDrainCallback drain_callback_;
  std::stop_source drain_source_;

  std::mutex mutex_;
  std::condition_variable_any cv_;
  Result<void> start_result_;
  bool init_done_{false};
  ExitResult exit_result_;

  std::jthread thread_;
};

}  // namespace alyrn::uring::detail
