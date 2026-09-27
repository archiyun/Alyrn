// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <memory_resource>
#include <vector>

#include "alyrn/result.h"
#include "alyrn/epoll/detail/worker.h"
#include "alyrn/detail/macros.h"

namespace alyrn::epoll::detail {

struct WorkerGroupOptions {
  std::size_t worker_num{1};
  WorkerOptions worker_options{};

  // The returned resource must outlive the worker group and must be private to
  // the selected worker when using an unsynchronized PMR resource.
  std::function<std::pmr::memory_resource*(std::size_t)> frame_resource_factory;
};

class WorkerGroup {
public:
  ALYRN_DELETE_COPY_MOVE(WorkerGroup);

  using ExitResult = Worker::ExitResult;
  using ThreadInitCallback = Worker::ThreadInitCallback;
  using ThreadExitCallback = Worker::ThreadExitCallback;
  using ConnectionCallback = Worker::ConnectionCallback;
  using ThreadDrainCallback = Worker::ThreadDrainCallback;

  WorkerGroup(net::Endpoint listen_addr, WorkerGroupOptions options = {},
              ThreadInitCallback init_callback = {}, ConnectionCallback connection_callback = {},
              ThreadExitCallback exit_callback = {}, ThreadDrainCallback drain_callback = {});

  ~WorkerGroup() noexcept;

  // Failed startup stops and joins all workers, leaving an empty group that
  // can be retried. Exceptions propagate after rollback. EALREADY preserves
  // the running group. Lifecycle calls must be serialized by the caller.
  Result<void> Start();

  // Asks every worker loop to stop without joining its thread. Safe to call
  // while another thread is inside Join().
  void RequestStop() noexcept;
  // Asks every worker to drain (see Worker::RequestDrain) without joining.
  void RequestDrain() noexcept;
  // Joins every worker without asking it to stop: call after RequestStop()
  // or RequestDrain(). Returns the first exit callback exception.
  ExitResult Join() noexcept;
  // Stops and joins every worker, returning the first exit callback exception
  // in worker-index order. The result survives repeated Stop() calls and startup
  // rollback until the next Start() attempt. Destruction discards the result;
  // call Stop() explicitly to observe it, from outside the worker threads.
  ExitResult Stop() noexcept;

  bool Started() const noexcept { return started_; }
  std::size_t Size() const noexcept { return workers_.size(); }

  Worker* At(std::size_t index) noexcept {
    return index < workers_.size() ? workers_[index].get() : nullptr;
  }
  const Worker* At(std::size_t index) const noexcept {
    return index < workers_.size() ? workers_[index].get() : nullptr;
  }

private:
  net::Endpoint listen_addr_;
  WorkerGroupOptions options_;
  ThreadInitCallback init_callback_;
  ConnectionCallback connection_callback_;
  ThreadExitCallback exit_callback_;
  ThreadDrainCallback drain_callback_;

  bool started_{false};
  ExitResult exit_result_;
  std::vector<std::unique_ptr<Worker>> workers_;
};

}  // namespace alyrn::epoll::detail
