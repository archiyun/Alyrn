// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <memory_resource>
#include <vector>

#include "alyrn/result.h"
#include "alyrn/detail/epoll/worker.h"
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

  using ThreadInitCallback = Worker::ThreadInitCallback;
  using ThreadExitCallback = Worker::ThreadExitCallback;
  using ConnectionCallback = Worker::ConnectionCallback;

  WorkerGroup(net::Endpoint listen_addr, WorkerGroupOptions options = {},
                     ThreadInitCallback init_callback = {},
                     ConnectionCallback connection_callback = {},
                     ThreadExitCallback exit_callback = {});

  ~WorkerGroup() noexcept;

  Result<void> Start();

  // Asks every worker loop to stop without joining its thread.
  void RequestStop() noexcept;
  void Stop() noexcept;

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

  bool started_{false};
  std::vector<std::unique_ptr<Worker>> workers_;
};

}  // namespace alyrn::epoll::detail
