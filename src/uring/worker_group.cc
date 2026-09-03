// SPDX-License-Identifier: MIT
#include "alyrn/uring/detail/worker_group.h"

#include <cerrno>
#include <expected>
#include <memory>
#include <utility>

#include "alyrn/uring/detail/worker.h"
#include "alyrn/net/endpoint.h"

namespace alyrn::uring::detail {

WorkerGroup::WorkerGroup(net::Endpoint listen_addr, WorkerGroupOptions options,
                                     ThreadInitCallback init_callback,
                                     ConnectionCallback connection_callback,
                                     ThreadExitCallback exit_callback)
    : listen_addr_(listen_addr),
      options_(std::move(options)),
      init_callback_(std::move(init_callback)),
      connection_callback_(std::move(connection_callback)),
      exit_callback_(std::move(exit_callback)) {}

WorkerGroup::~WorkerGroup() noexcept { Stop(); }

Result<void> WorkerGroup::Start() {
  if (started_) {
    return std::unexpected(Errno(EALREADY));
  }

  if (options_.worker_num == 0) {
    return std::unexpected(Errno(EINVAL));
  }

  workers_.reserve(options_.worker_num);

  for (std::size_t i = 0; i < options_.worker_num; ++i) {
    WorkerOptions worker_options = options_.worker_options;
    if (options_.frame_resource_factory) {
      worker_options.frame_resource = options_.frame_resource_factory(i);
    }
    if (options_.cpu_affinity_factory) {
      worker_options.cpu_affinity = options_.cpu_affinity_factory(i);
    }

    auto worker = std::make_unique<Worker>(i, listen_addr_, std::move(worker_options),
                                                 init_callback_, connection_callback_, exit_callback_);
    auto started = worker->Start();
    if (!started.HasValue()) {
      Stop();
      return std::unexpected(started.Error());
    }

    workers_.push_back(std::move(worker));
  }

  started_ = true;
  return {};
}

void WorkerGroup::Stop() noexcept {
  RequestStop();

  workers_.clear();
  started_ = false;
}

void WorkerGroup::RequestStop() noexcept {
  for (auto& worker : workers_) {
    worker->Stop();
  }
}

}  // namespace alyrn::uring::detail
