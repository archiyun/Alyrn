// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <memory_resource>
#include <vector>

#include "coropact/base/error.h"
#include "coropact/reactor/reactor_worker.h"
#include "coropact/utils/macros.h"

namespace coropact::reactor {

struct ReactorWorkerGroupOptions {
  std::size_t worker_num{1};
  ReactorWorkerOptions worker_options{};

  // The returned resource must outlive the worker group and must be private to
  // the selected worker when using an unsynchronized PMR resource.
  std::function<std::pmr::memory_resource*(std::size_t)> frame_resource_factory;
};

class ReactorWorkerGroup {
public:
  COROPACT_DELETE_COPY_MOVE(ReactorWorkerGroup);

  using ThreadInitCallback = ReactorWorker::ThreadInitCallback;
  using ThreadExitCallback = ReactorWorker::ThreadExitCallback;
  using ConnectionCallback = ReactorWorker::ConnectionCallback;

  ReactorWorkerGroup(net::Endpoint listen_addr, ReactorWorkerGroupOptions options = {},
                     ThreadInitCallback init_callback = {},
                     ConnectionCallback connection_callback = {},
                     ThreadExitCallback exit_callback = {});

  ~ReactorWorkerGroup() noexcept;

  [[nodiscard]]
  base::Result<void> Start();
  void Stop() noexcept;

  [[nodiscard]]
  bool Started() const noexcept { return started_; }
  [[nodiscard]]
  std::size_t Size() const noexcept { return workers_.size(); }

  [[nodiscard]]
  ReactorWorker* Worker(std::size_t index) noexcept {
    return index < workers_.size() ? workers_[index].get() : nullptr;
  }
  [[nodiscard]]
  const ReactorWorker* Worker(std::size_t index) const noexcept {
    return index < workers_.size() ? workers_[index].get() : nullptr;
  }

private:
  net::Endpoint listen_addr_;
  ReactorWorkerGroupOptions options_;
  ThreadInitCallback init_callback_;
  ConnectionCallback connection_callback_;
  ThreadExitCallback exit_callback_;

  bool started_{false};
  std::vector<std::unique_ptr<ReactorWorker>> workers_;
};

}  // namespace coropact::reactor
