// SPDX-License-Identifier: MIT
#pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <thread>

#include "coropact/result.h"
#include "coropact/coro/detached_task.h"
#include "coropact/kqueue/connector.h"
#include "coropact/kqueue/listener.h"
#include "coropact/kqueue/loop.h"
#include "coropact/kqueue/stream.h"
#include "coropact/utils/macros.h"

namespace coropact::kqueue::detail {

struct KqueueWorkerContext {
  KqueueWorkerContext(std::size_t index, KqueueLoop& loop, KqueueListener* listener,
                       KqueueConnector& connector) noexcept
      : index(index), loop(loop), listener(listener), connector(connector) {}

  COROPACT_DELETE_COPY_MOVE(KqueueWorkerContext);

  const std::size_t index;
  KqueueLoop& loop;
  // Null on I/O workers in the master-slave topology. Only the acceptor
  // worker owns a listener.
  KqueueListener* listener;
  KqueueConnector& connector;
};

struct KqueueWorkerOptions {
  KqueueListenerOptions listener_options{.reuse_addr = true, .reuse_port = false};

  // When true, this worker binds the listen socket and runs Accept(). The
  // worker group sets this on exactly one worker.
  bool accept{true};

  // Must outlive the worker. It should be private to one worker when it is
  // unsynchronized.
  std::pmr::memory_resource* frame_resource{nullptr};

  KqueueConnectorOptions connector_options{};
};

class KqueueWorker {
public:
  COROPACT_DELETE_COPY_MOVE(KqueueWorker);

  using ThreadInitCallback = std::function<void(KqueueWorkerContext&)>;
  // Runs on the worker thread after the loop stops and before loop-bound
  // listener/connector resources are destroyed.
  using ThreadExitCallback = std::function<void(KqueueWorkerContext&)>;
  using ConnectionCallback =
      std::function<coro::DetachedTask(KqueueWorkerContext&, KqueueStream)>;

  KqueueWorker(std::size_t index, net::Endpoint listen_addr, KqueueWorkerOptions options = {},
                ThreadInitCallback init_callback = {}, ConnectionCallback connection_callback = {},
                ThreadExitCallback exit_callback = {});
  ~KqueueWorker() noexcept;

  [[nodiscard]]
  Result<void> Start();

  // Requests shutdown. The worker thread is joined by the destructor or by
  // the owning KqueueWorkerGroup.
  void Stop() noexcept;

  [[nodiscard]]
  std::size_t Index() const noexcept { return index_; }

  [[nodiscard]]
  KqueueLoop* Loop() const noexcept { return loop_; }

  [[nodiscard]]
  KqueueWorkerContext* Context() const noexcept { return context_; }

private:
  void WorkLoop(std::stop_token token) noexcept;

  std::size_t index_;
  net::Endpoint listen_addr_;
  KqueueWorkerOptions options_;
  ThreadInitCallback init_callback_;
  ConnectionCallback connection_callback_;
  ThreadExitCallback exit_callback_;

  std::mutex mutex_;
  std::condition_variable_any cv_;
  Result<void> start_result_;
  bool init_done_{false};

  std::jthread thread_;
  KqueueLoop* loop_{nullptr};
  KqueueWorkerContext* context_{nullptr};
};

}  // namespace coropact::kqueue::detail
