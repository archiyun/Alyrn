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

WorkerGroup::~WorkerGroup() noexcept { (void)Stop(); }

Result<void> WorkerGroup::Start() {
  if (started_) {
    return std::unexpected(Errno(EALREADY));
  }

  if (options_.worker_num == 0) {
    return std::unexpected(Errno(EINVAL));
  }

  exit_result_ = ExitResult{};
  try {
    workers_.reserve(options_.worker_num);

    for (std::size_t i = 0; i < options_.worker_num; ++i) {
      WorkerOptions worker_options = options_.worker_options;
      if (options_.frame_resource_factory) {
        worker_options.frame_resource = options_.frame_resource_factory(i);
      }
      if (options_.cpu_affinity_factory) {
        worker_options.cpu_affinity = options_.cpu_affinity_factory(i);
      }

      // Own the worker before starting it so rollback requests every stop
      // before joining any thread, including a partially started worker.
      workers_.push_back(std::make_unique<Worker>(i, listen_addr_, std::move(worker_options),
                                                  init_callback_, connection_callback_,
                                                  exit_callback_));
      auto result = workers_.back()->Start();
      if (!result.HasValue()) {
        (void)Stop();
        return std::unexpected(result.Error());
      }
    }
  } catch (...) {
    (void)Stop();
    throw;
  }

  started_ = true;
  return {};
}

WorkerGroup::ExitResult WorkerGroup::Stop() noexcept {
  RequestStop();

  for (auto& worker : workers_) {
    auto result = worker->Join();
    if (exit_result_.HasValue() && !result.HasValue()) {
      exit_result_ = std::move(result);
    }
  }
  workers_.clear();
  started_ = false;
  return exit_result_;
}

void WorkerGroup::RequestStop() noexcept {
  for (auto& worker : workers_) {
    worker->Stop();
  }
}

}  // namespace alyrn::uring::detail
