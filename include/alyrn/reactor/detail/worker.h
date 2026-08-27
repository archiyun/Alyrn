// SPDX-License-Identifier: MIT
#pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <thread>

#include "alyrn/result.h"
#include "alyrn/coro/detached_task.h"
#include "alyrn/reactor/connector.h"
#include "alyrn/reactor/listener.h"
#include "alyrn/reactor/loop.h"
#include "alyrn/reactor/stream.h"
#include "alyrn/utils/macros.h"

namespace alyrn::reactor::detail {

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

  using ThreadInitCallback = std::function<void(WorkerContext&)>;
  // Runs on the worker thread after the loop stops and before loop-bound
  // listener/connector resources are destroyed.
  using ThreadExitCallback = std::function<void(WorkerContext&)>;
  using ConnectionCallback =
      std::function<coro::DetachedTask(WorkerContext&, Stream)>;

  Worker(std::size_t index, net::Endpoint listen_addr, WorkerOptions options = {},
                ThreadInitCallback init_callback = {}, ConnectionCallback connection_callback = {},
                ThreadExitCallback exit_callback = {});
  ~Worker() noexcept;

  [[nodiscard]]
  Result<void> Start();

  // Requests shutdown. The worker thread is joined by the destructor or by
  // the owning WorkerGroup.
  void Stop() noexcept;

  [[nodiscard]]
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

  std::jthread thread_;
};

}  // namespace alyrn::reactor::detail
