// SPDX-License-Identifier: MIT
#include "coropact/kqueue/runtime.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stop_token>
#include <thread>
#include <utility>

#include "coropact/result.h"
#include "coropact/kqueue/detail/kqueue_worker_group.h"

namespace coropact::kqueue {

namespace {

using KqueueBuilder = Runtime::Builder<runtime::Kqueue>;

void WaitForStop(std::atomic_bool& stop_requested) noexcept {
  while (!stop_requested.load(std::memory_order_acquire)) {
    stop_requested.wait(false, std::memory_order_acquire);
  }
}

// Owns the kqueue worker group behind Runtime's cold lifecycle seam. The
// accepted stream remains KqueueStream all the way to ConnectionHandler.
class KqueueRuntimeControl final : public runtime::detail::RuntimeControl {
public:
  KqueueRuntimeControl(net::Endpoint listen_addr, std::size_t worker_count,
                        KqueueBuilder::ConnectionHandler connection_handler) noexcept
      : listen_addr_(listen_addr),
        worker_count_(worker_count),
        connection_handler_(std::move(connection_handler)) {}

  ~KqueueRuntimeControl() noexcept override { Stop(); }

  Result<void> Start() override {
    {
      std::lock_guard lock{lifecycle_mutex_};
      if (state_ != LifecycleState::kCreated) {
        return std::unexpected(Errno(EALREADY));
      }
      if (worker_count_ == 0 || !connection_handler_) {
        return std::unexpected(Errno(EINVAL));
      }
      stop_requested_.store(false, std::memory_order_release);
      state_ = LifecycleState::kStarting;
    }

    detail::KqueueWorkerGroupOptions options;
    options.worker_num = worker_count_;
    options.worker_options.listener_options.reuse_port = false;

    auto callback = [this](detail::KqueueWorkerContext&, KqueueStream stream) {
      return connection_handler_(std::move(stream));
    };
    auto workers = std::make_unique<detail::KqueueWorkerGroup>(
        listen_addr_, std::move(options), detail::KqueueWorkerGroup::ThreadInitCallback{},
        std::move(callback));
    auto started = workers->Start();
    if (!started.has_value()) {
      std::lock_guard lock{lifecycle_mutex_};
      stop_requested_.store(false, std::memory_order_release);
      state_ = LifecycleState::kCreated;
      return std::unexpected(started.error());
    }

    {
      std::lock_guard lock{lifecycle_mutex_};
      workers_ = std::move(workers);
      state_ = LifecycleState::kRunning;
    }
    if (stop_requested_.load(std::memory_order_acquire)) {
      RequestStop();
    }
    return {};
  }

  Result<void> Run(std::stop_token stop_token) override {
    auto started = Start();
    if (!started.has_value()) {
      return std::unexpected(started.error());
    }

    std::stop_callback on_stop{stop_token, [this] { RequestStop(); }};
    WaitForStop(stop_requested_);
    Stop();
    return {};
  }

  void RequestStop() noexcept override {
    stop_requested_.store(true, std::memory_order_release);
    stop_requested_.notify_all();

    std::lock_guard lock{lifecycle_mutex_};
    if (state_ != LifecycleState::kRunning) {
      return;
    }
    state_ = LifecycleState::kStopping;
    workers_->RequestStop();
  }

  void Stop() noexcept override {
    std::unique_ptr<detail::KqueueWorkerGroup> workers;
    {
      std::lock_guard lock{lifecycle_mutex_};
      if (state_ == LifecycleState::kCreated || state_ == LifecycleState::kStopped ||
          state_ == LifecycleState::kStarting) {
        return;
      }

      stop_requested_.store(true, std::memory_order_release);
      stop_requested_.notify_all();
      state_ = LifecycleState::kStopping;
      workers_->RequestStop();
      workers = std::move(workers_);
    }

    workers.reset();

    std::lock_guard lock{lifecycle_mutex_};
    state_ = LifecycleState::kStopped;
  }

  bool Started() const noexcept override {
    std::lock_guard lock{lifecycle_mutex_};
    return state_ == LifecycleState::kRunning || state_ == LifecycleState::kStopping;
  }

private:
  enum class LifecycleState : std::uint8_t {
    kCreated,
    kStarting,
    kRunning,
    kStopping,
    kStopped,
  };

  net::Endpoint listen_addr_;
  std::size_t worker_count_;
  KqueueBuilder::ConnectionHandler connection_handler_;
  mutable std::mutex lifecycle_mutex_;
  std::unique_ptr<detail::KqueueWorkerGroup> workers_;
  LifecycleState state_{LifecycleState::kCreated};
  std::atomic_bool stop_requested_{false};
};

}  // namespace

std::unique_ptr<runtime::detail::RuntimeControl> MakeRuntimeControl(
    net::Endpoint listen_addr, std::size_t worker_count,
    Runtime::Builder<runtime::Kqueue>::ConnectionHandler connection_handler) {
  return std::make_unique<KqueueRuntimeControl>(listen_addr, worker_count,
                                                 std::move(connection_handler));
}

}  // namespace coropact::kqueue

namespace coropact {

Runtime::Builder<runtime::Kqueue>::Builder(net::Endpoint listen_addr) noexcept
    : listen_addr_(listen_addr) {}

Runtime::Builder<runtime::Kqueue>& Runtime::Builder<runtime::Kqueue>::Workers(
    std::size_t count) noexcept {
  worker_count_ = count;
  return *this;
}

Runtime::Builder<runtime::Kqueue>& Runtime::Builder<runtime::Kqueue>::AutoWorkers() noexcept {
  worker_count_ = std::max<std::size_t>(std::thread::hardware_concurrency(), 1);
  return *this;
}

Runtime::Builder<runtime::Kqueue>& Runtime::Builder<runtime::Kqueue>::OnConnection(
    ConnectionHandler handler) {
  connection_handler_ = std::move(handler);
  return *this;
}

Runtime Runtime::Builder<runtime::Kqueue>::Build() {
  return Runtime{kqueue::MakeRuntimeControl(listen_addr_, worker_count_,
                                             std::move(connection_handler_))};
}

}  // namespace coropact
