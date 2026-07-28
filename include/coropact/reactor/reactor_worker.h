// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <thread>

#include "coropact/base/error.h"
#include "coropact/coro/detached_task.h"
#include "coropact/reactor/event_loop.h"
#include "coropact/reactor/event_loop_scheduler.h"
#include "coropact/reactor/reactor_connect.h"
#include "coropact/reactor/reactor_listener.h"
#include "coropact/reactor/reactor_stream.h"
#include "coropact/utils/macros.h"

namespace coropact::reactor {

struct ReactorWorkerContext {
  ReactorWorkerContext(std::size_t index, EventLoop& loop, EventLoopScheduler& scheduler,
                       ReactorListener& listener, ReactorConnector& connector) noexcept
      : index(index), loop(loop), scheduler(scheduler), listener(listener), connector(connector) {}

  COROPACT_DELETE_COPY_MOVE(ReactorWorkerContext);

  const std::size_t index;
  EventLoop& loop;
  EventLoopScheduler& scheduler;
  ReactorListener& listener;
  ReactorConnector& connector;
};

struct ReactorWorkerOptions {
  ReactorListenerOptions listener_options{.reuse_addr = true, .reuse_port = true};

  // Must outlive the worker. It should be private to one worker when it is
  // unsynchronized.
  std::pmr::memory_resource* frame_resource{nullptr};
};

class ReactorWorker {
public:
  COROPACT_DELETE_COPY_MOVE(ReactorWorker);

  using ThreadInitCallback = std::function<void(ReactorWorkerContext&)>;
  // Runs on the worker thread after the loop stops and before loop-bound
  // listener/connector resources are destroyed.
  using ThreadExitCallback = std::function<void(ReactorWorkerContext&)>;
  using ConnectionCallback =
      std::function<coro::DetachedTask(ReactorWorkerContext&, ReactorStream)>;

  ReactorWorker(std::size_t index, net::Endpoint listen_addr, ReactorWorkerOptions options = {},
                ThreadInitCallback init_callback = {}, ConnectionCallback connection_callback = {},
                ThreadExitCallback exit_callback = {});
  ~ReactorWorker() noexcept;

  [[nodiscard]]
  base::Result<void> Start();

  // Requests shutdown. The worker thread is joined by the destructor or by
  // the owning ReactorWorkerGroup.
  void Stop() noexcept;

  [[nodiscard]]
  std::size_t Index() const noexcept { return index_; }

private:
  void WorkLoop(std::stop_token token) noexcept;

  std::size_t index_;
  net::Endpoint listen_addr_;
  ReactorWorkerOptions options_;
  ThreadInitCallback init_callback_;
  ConnectionCallback connection_callback_;
  ThreadExitCallback exit_callback_;

  std::mutex mutex_;
  std::condition_variable_any cv_;
  base::Result<void> start_result_;
  bool init_done_{false};

  std::jthread thread_;
};

}  // namespace coropact::reactor
