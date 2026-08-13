// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <cstddef>
#include <functional>
#include <memory>
#include <memory_resource>
#include <vector>

#include "coropact/result.h"
#include "coropact/kqueue/detail/kqueue_worker.h"
#include "coropact/utils/macros.h"

namespace coropact::kqueue::detail {

struct KqueueWorkerGroupOptions {
  std::size_t worker_num{1};
  KqueueWorkerOptions worker_options{};

  // The returned resource must outlive the worker group and must be private to
  // the selected worker when using an unsynchronized PMR resource.
  std::function<std::pmr::memory_resource*(std::size_t)> frame_resource_factory;
};

class KqueueWorkerGroup {
public:
  COROPACT_DELETE_COPY_MOVE(KqueueWorkerGroup);

  using ThreadInitCallback = KqueueWorker::ThreadInitCallback;
  using ThreadExitCallback = KqueueWorker::ThreadExitCallback;
  using ConnectionCallback = KqueueWorker::ConnectionCallback;

  KqueueWorkerGroup(net::Endpoint listen_addr, KqueueWorkerGroupOptions options = {},
                     ThreadInitCallback init_callback = {},
                     ConnectionCallback connection_callback = {},
                     ThreadExitCallback exit_callback = {});

  ~KqueueWorkerGroup() noexcept;

  [[nodiscard]]
  Result<void> Start();

  // Asks every worker loop to stop without joining its thread.
  void RequestStop() noexcept;
  void Stop() noexcept;

  [[nodiscard]]
  bool Started() const noexcept { return started_; }
  [[nodiscard]]
  std::size_t Size() const noexcept { return workers_.size(); }

  [[nodiscard]]
  KqueueWorker* Worker(std::size_t index) noexcept {
    return index < workers_.size() ? workers_[index].get() : nullptr;
  }
  [[nodiscard]]
  const KqueueWorker* Worker(std::size_t index) const noexcept {
    return index < workers_.size() ? workers_[index].get() : nullptr;
  }

  [[nodiscard]]
  std::size_t NextWorker() noexcept;

private:
  [[nodiscard]]
  Result<void> StartOne(std::size_t index, bool accept, ConnectionCallback connection_callback);

  net::Endpoint listen_addr_;
  KqueueWorkerGroupOptions options_;
  ThreadInitCallback init_callback_;
  ConnectionCallback connection_callback_;
  ThreadExitCallback exit_callback_;

  bool started_{false};
  std::atomic<std::size_t> next_worker_{0};
  std::vector<std::unique_ptr<KqueueWorker>> workers_;
};

}  // namespace coropact::kqueue::detail
