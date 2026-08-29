// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <memory_resource>
#include <optional>
#include <vector>

#include "alyrn/result.h"
#include "alyrn/detail/uring/worker.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/detail/macros.h"

namespace alyrn::uring::detail {

struct WorkerGroupOptions {
  std::size_t worker_num{1};
  WorkerOptions worker_options{};

  // Optional per-worker frame resource selector. The returned resource must
  // outlive the worker group and must be private to the selected worker when
  // using an unsynchronized PMR resource.
  std::function<std::pmr::memory_resource*(std::size_t)> frame_resource_factory;

  // Optional per-worker CPU selector. The selected CPU is applied before the
  // worker initializes its ring and publishes successful startup.
  std::function<std::optional<unsigned>(std::size_t)> cpu_affinity_factory;
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

  bool Started() const noexcept {
    return started_;
  }

  std::size_t Size() const noexcept {
    return workers_.size();
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

}  // namespace alyrn::uring::detail
