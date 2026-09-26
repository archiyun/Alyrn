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
#include <thread>

#include "alyrn/coro/detached_task.h"
#include "alyrn/detail/macros.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/result.h"
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
};

class Worker {
public:
  ALYRN_DELETE_COPY_MOVE(Worker);

  using ExitResult = Result<void, std::exception_ptr>;
  using ThreadInitCallback = std::function<void(WorkerContext&)>;
  // Runs on the worker thread after the loop has drained and before loop-bound
  // listener/connector resources are destroyed. Also runs if ThreadInitCallback
  // throws, so it must tolerate partial initialization. Exceptions are retained
  // and returned by Join(); the callback is not retried.
  using ThreadExitCallback = std::function<void(WorkerContext&)>;
  using ConnectionCallback =
      std::function<coro::DetachedTask(WorkerContext&, Stream)>;

  Worker(std::size_t index, net::Endpoint listen_addr, WorkerOptions options = {},
               ThreadInitCallback init_callback = {}, ConnectionCallback connection_callback = {},
               ThreadExitCallback exit_callback = {});
  ~Worker() noexcept;

  Result<void> Start();
  // Requests shutdown without waiting for thread exit.
  void Stop() noexcept;

  // Waits for thread exit and returns any exception from ThreadExitCallback.
  // Call Stop() first to request shutdown. Join and lifecycle calls must be
  // serialized on a non-worker thread. Repeated joins preserve the result;
  // a new Start() attempt clears it. Destruction alone discards the result.
  [[nodiscard]] ExitResult Join() noexcept;

  std::size_t Index() const noexcept { return index_; }

private:
  void WorkLoop(std::stop_token token) noexcept;

  std::size_t index_;
  net::Endpoint listen_addr_;
  WorkerOptions options_;
  ThreadInitCallback init_callback_;
  ConnectionCallback connection_callback_;
  ThreadExitCallback exit_callback_;

  std::mutex mutex_;
  std::condition_variable_any cv_;
  Result<void> start_result_;
  bool init_done_{false};
  ExitResult exit_result_;

  std::jthread thread_;
};

}  // namespace alyrn::uring::detail
