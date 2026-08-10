// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <memory_resource>
#include <optional>
#include <vector>

#include "coropact/base/error.h"
#include "coropact/luring/detail/worker.h"
#include "coropact/net/endpoint.h"
#include "coropact/utils/macros.h"

namespace coropact::luring::detail {

struct LUringWorkerGroupOptions {
  std::size_t worker_num{1};
  LUringWorkerOptions worker_options{};

  // Optional per-worker frame resource selector. The returned resource must
  // outlive the worker group and must be private to the selected worker when
  // using an unsynchronized PMR resource.
  std::function<std::pmr::memory_resource*(std::size_t)> frame_resource_factory;

  // Optional per-worker CPU selector. The selected CPU is applied before the
  // worker initializes its ring and publishes successful startup.
  std::function<std::optional<unsigned>(std::size_t)> cpu_affinity_factory;
};

class LUringWorkerGroup {
public:
  COROPACT_DELETE_COPY_MOVE(LUringWorkerGroup);

  using ThreadInitCallback = LUringWorker::ThreadInitCallback;
  using ThreadExitCallback = LUringWorker::ThreadExitCallback;
  using ConnectionCallback = LUringWorker::ConnectionCallback;

  LUringWorkerGroup(net::Endpoint listen_addr, LUringWorkerGroupOptions options = {},
                    ThreadInitCallback init_callback = {},
                    ConnectionCallback connection_callback = {},
                    ThreadExitCallback exit_callback = {});
  ~LUringWorkerGroup() noexcept;

  [[nodiscard]]
  base::Result<void> Start();

  // Asks every worker loop to stop without joining its thread.
  void RequestStop() noexcept;
  void Stop() noexcept;

  [[nodiscard]]
  bool Started() const noexcept {
    return started_;
  }

  [[nodiscard]]
  std::size_t Size() const noexcept {
    return workers_.size();
  }

private:
  net::Endpoint listen_addr_;
  LUringWorkerGroupOptions options_;
  ThreadInitCallback init_callback_;
  ConnectionCallback connection_callback_;
  ThreadExitCallback exit_callback_;

  bool started_{false};
  std::vector<std::unique_ptr<LUringWorker>> workers_;
};

}  // namespace coropact::luring::detail
