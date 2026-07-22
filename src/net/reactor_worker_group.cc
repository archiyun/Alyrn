// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/net/reactor_worker_group.h"

#include <cerrno>
#include <expected>
#include <memory>
#include <utility>

namespace vexo::net {

ReactorWorkerGroup::ReactorWorkerGroup(InetAddress listen_addr, ReactorWorkerGroupOptions options,
                                       ThreadInitCallback init_callback,
                                       ConnectionCallback connection_callback)
    : listen_addr_(listen_addr),
      options_(std::move(options)),
      init_callback_(std::move(init_callback)),
      connection_callback_(std::move(connection_callback)) {}

ReactorWorkerGroup::~ReactorWorkerGroup() noexcept { Stop(); }

base::Result<void> ReactorWorkerGroup::Start() {
  if (started_) {
    return std::unexpected(base::make_errno(EALREADY));
  }

  if (options_.worker_num == 0) {
    return std::unexpected(base::make_errno(EINVAL));
  }

  workers_.reserve(options_.worker_num);

  for (std::size_t i = 0; i < options_.worker_num; ++i) {
    ReactorWorkerOptions worker_options = options_.worker_options;
    if (options_.frame_resource_factory) {
      worker_options.frame_resource = options_.frame_resource_factory(i);
    }

    auto worker = std::make_unique<ReactorWorker>(i, listen_addr_, worker_options, init_callback_,
                                                  connection_callback_);
    auto result = worker->Start();
    if (!result.has_value()) {
      Stop();
      return std::unexpected(result.error());
    }

    workers_.push_back(std::move(worker));
  }

  started_ = true;
  return {};
}

void ReactorWorkerGroup::Stop() noexcept {
  for (auto& worker : workers_) {
    worker->Stop();
  }

  // ReactorWorker owns a jthread. Clearing the vector joins each worker after
  // its stop request has been delivered.
  workers_.clear();
  started_ = false;
}

}  // namespace vexo::net
