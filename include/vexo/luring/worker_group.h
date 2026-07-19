#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <memory_resource>
#include <vector>

#include "vexo/base/error.h"
#include "vexo/luring/worker.h"
#include "vexo/net/inet_address.h"
#include "vexo/utils/macros.h"

namespace vexo::luring {

struct LUringWorkerGroupOptions {
  std::size_t worker_num{1};
  LUringWorkerOptions worker_options{};

  // Optional per-worker frame resource selector. The returned resource must
  // outlive the worker group and must be private to the selected worker when
  // using an unsynchronized PMR resource.
  std::function<std::pmr::memory_resource*(std::size_t)> frame_resource_factory;
};

class LUringWorkerGroup {
public:
  VEXO_DELETE_COPY_MOVE(LUringWorkerGroup);

  using ThreadInitCallback = LUringWorker::ThreadInitCallback;
  using ConnectionCallback = LUringWorker::ConnectionCallback;

  LUringWorkerGroup(net::InetAddress listen_addr, LUringWorkerGroupOptions options = {},
                    ThreadInitCallback init_callback = {},
                    ConnectionCallback connection_callback = {});
  ~LUringWorkerGroup() noexcept;

  [[nodiscard]] base::Result<void> Start();
  void Stop() noexcept;

  [[nodiscard]] bool started() const noexcept { return started_; }
  [[nodiscard]] std::size_t size() const noexcept { return workers_.size(); }

  [[nodiscard]] LUringWorker* worker(std::size_t index) noexcept {
    return index < workers_.size() ? workers_[index].get() : nullptr;
  }
  [[nodiscard]] const LUringWorker* worker(std::size_t index) const noexcept {
    return index < workers_.size() ? workers_[index].get() : nullptr;
  }

private:
  net::InetAddress listen_addr_;
  LUringWorkerGroupOptions options_;
  ThreadInitCallback init_callback_;
  ConnectionCallback connection_callback_;

  bool started_{false};
  std::vector<std::unique_ptr<LUringWorker>> workers_;
};

}  // namespace vexo::luring
