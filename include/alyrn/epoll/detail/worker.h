// SPDX-License-Identifier: MIT
#pragma once

#include <condition_variable>
#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <thread>

#include "alyrn/coro/detached_task.h"
#include "alyrn/detail/macros.h"
#include "alyrn/epoll/connector.h"
#include "alyrn/epoll/listener.h"
#include "alyrn/epoll/loop.h"
#include "alyrn/epoll/stream.h"
#include "alyrn/result.h"

namespace alyrn::epoll::detail {

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
  ListenerOptions listener_options{.reuse_addr = true, .reuse_port = true};

  // Must outlive the worker. It should be private to one worker when it is
  // unsynchronized.
  std::pmr::memory_resource* frame_resource{nullptr};

  ConnectorOptions connector_options{};
};

class Worker {
public:
  ALYRN_DELETE_COPY_MOVE(Worker);

  using ExitResult = Result<void, std::exception_ptr>;
  using ThreadInitCallback = std::function<void(WorkerContext&)>;
  // Runs on the worker thread after the loop stops and before loop-bound
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

  // Requests shutdown. The worker thread is joined by the destructor or by
  // the owning WorkerGroup.
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

}  // namespace alyrn::epoll::detail
