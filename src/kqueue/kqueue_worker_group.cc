// SPDX-License-Identifier: MIT
#include "coropact/kqueue/detail/kqueue_worker_group.h"

#include <unistd.h>

#include <cerrno>
#include <expected>
#include <utility>

#include "coropact/result.h"
#include "coropact/coro/spawn.h"
#include "coropact/kqueue/stream.h"

namespace coropact::kqueue::detail {
namespace {

coro::DetachedTask HandoffAccepted(KqueueWorkerGroup* group,
                                   KqueueWorker::ConnectionCallback handler,
                                   KqueueStream stream) {
  const net::Endpoint peer = stream.PeerAddress();
  const int fd = stream.Release();
  const std::size_t count = group->Size();
  if (count == 0) {
    ::close(fd);
    co_return;
  }

  const std::size_t index = group->NextWorker() % count;
  KqueueWorker* target = group->Worker(index);
  KqueueLoop* loop = target != nullptr ? target->Loop() : nullptr;
  if (loop == nullptr) {
    ::close(fd);
    co_return;
  }

  loop->Post([target, fd, peer, handler = std::move(handler)]() mutable {
    KqueueLoop* owner = target->Loop();
    KqueueWorkerContext* context = target->Context();
    if (owner == nullptr || context == nullptr) {
      ::close(fd);
      return;
    }
    KqueueStream adopted(owner, fd, peer);
    if (handler) {
      coro::SpawnDetach(*owner, handler(*context, std::move(adopted)));
    }
  });
  co_return;
}

}  // namespace

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

std::size_t KqueueWorkerGroup::NextWorker() noexcept {
  return next_worker_.fetch_add(1, std::memory_order_relaxed);
}

Result<void> KqueueWorkerGroup::StartOne(std::size_t index, bool accept,
                                         ConnectionCallback connection_callback) {
  KqueueWorkerOptions worker_options = options_.worker_options;
  worker_options.accept = accept;
  worker_options.listener_options.reuse_port = false;
  if (options_.frame_resource_factory) {
    worker_options.frame_resource = options_.frame_resource_factory(index);
  }

  auto worker = std::make_unique<KqueueWorker>(index, listen_addr_, worker_options, init_callback_,
                                                std::move(connection_callback), exit_callback_);
  workers_[index] = std::move(worker);
  auto result = workers_[index]->Start();
  if (!result.has_value()) {
    Stop();
    return std::unexpected(result.error());
  }
  return {};
}

Result<void> KqueueWorkerGroup::Start() {
  if (started_) {
    return std::unexpected(Errno(EALREADY));
  }

  if (options_.worker_num == 0) {
    return std::unexpected(Errno(EINVAL));
  }

  workers_.clear();
  workers_.resize(options_.worker_num);
  next_worker_.store(0, std::memory_order_relaxed);

  /* I/O workers start first so their loops exist before the acceptor can
   * Post a handed-off descriptor. */
  for (std::size_t i = 1; i < options_.worker_num; ++i) {
    auto started = StartOne(i, false, ConnectionCallback{});
    if (!started.has_value()) {
      return started;
    }
  }

  ConnectionCallback acceptor_callback = connection_callback_;
  if (options_.worker_num > 1 && connection_callback_) {
    acceptor_callback = [this](KqueueWorkerContext&, KqueueStream stream) {
      return HandoffAccepted(this, connection_callback_, std::move(stream));
    };
  }

  auto started = StartOne(0, true, std::move(acceptor_callback));
  if (!started.has_value()) {
    return started;
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
    if (worker != nullptr) {
      worker->Stop();
    }
  }
}

}  // namespace coropact::kqueue::detail
