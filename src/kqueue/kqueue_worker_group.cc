// SPDX-License-Identifier: MIT
#include "coropact/kqueue/detail/kqueue_worker_group.h"

#include <cerrno>
#include <expected>
#include <memory>
#include <utility>

#include "coropact/result.h"

namespace coropact::kqueue::detail {

KqueueWorkerGroup::KqueueWorkerGroup(net::Endpoint listen_addr, KqueueWorkerGroupOptions options,
                                       ThreadInitCallback init_callback,
                                       ConnectionCallback connection_callback,
                                       ThreadExitCallback exit_callback)
    : listen_addr_(listen_addr),
      options_(std::move(options)),
      init_callback_(std::move(init_callback)),
      connection_callback_(std::move(connection_callback)),
      exit_callback_(std::move(exit_callback)) {}

KqueueWorkerGroup::~KqueueWorkerGroup() noexcept { Stop(); }

Result<void> KqueueWorkerGroup::Start() {
  if (started_) {
    return std::unexpected(Errno(EALREADY));
  }

  if (options_.worker_num == 0) {
    return std::unexpected(Errno(EINVAL));
  }

  workers_.reserve(options_.worker_num);

  for (std::size_t i = 0; i < options_.worker_num; ++i) {
    KqueueWorkerOptions worker_options = options_.worker_options;
    if (options_.frame_resource_factory) {
      worker_options.frame_resource = options_.frame_resource_factory(i);
    }

    auto worker = std::make_unique<KqueueWorker>(i, listen_addr_, worker_options, init_callback_,
                                                  connection_callback_, exit_callback_);
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

void KqueueWorkerGroup::Stop() noexcept {
  RequestStop();

  // KqueueWorker owns a jthread. Clearing the vector joins each worker after
  // its stop request has been delivered.
  workers_.clear();
  started_ = false;
}

void KqueueWorkerGroup::RequestStop() noexcept {
  for (auto& worker : workers_) {
    worker->Stop();
  }
}

}  // namespace coropact::kqueue::detail
