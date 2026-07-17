#include "vexo/luring/worker_group.h"

#include <cerrno>
#include <expected>
#include <memory>
#include <utility>

#include "vexo/luring/worker.h"
#include "vexo/net/inet_address.h"

namespace vexo::luring {

LUringWorkerGroup::LUringWorkerGroup(net::InetAddress listen_addr, LUringWorkerGroupOptions options,
                                     ThreadInitCallback init_callback,
                                     ConnectionCallback connection_callback)
    : listen_addr_(listen_addr),
      options_(std::move(options)),
      init_callback_(std::move(init_callback)),
      connection_callback_(std::move(connection_callback)) {}

LUringWorkerGroup::~LUringWorkerGroup() { Stop(); }

base::Result<void> LUringWorkerGroup::Start() {
  if (started_) {
    return std::unexpected(base::make_errno(EALREADY));
  }

  if (options_.worker_num == 0) {
    return std::unexpected(base::make_errno(EINVAL));
  }

  workers_.reserve(options_.worker_num);

  for (std::size_t i = 0; i < options_.worker_num; ++i) {
    auto worker = std::make_unique<LUringWorker>(listen_addr_, options_.worker_options,
                                                 init_callback_, connection_callback_);
    auto started = worker->Start();
    if (!started.has_value()) {
      Stop();
      return std::unexpected(started.error());
    }

    workers_.push_back(std::move(worker));
  }

  started_ = true;
  return {};
}

void LUringWorkerGroup::Stop() noexcept {
  for (auto& worker : workers_) {
    worker->Stop();
  }

  workers_.clear();
  started_ = false;
}

}  // namespace vexo::luring
