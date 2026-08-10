// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/reactor/runtime.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stop_token>
#include <thread>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/reactor/detail/reactor_worker_group.h"

namespace coropact::reactor {

namespace {

using ReactorBuilder = Runtime::Builder<runtime::Reactor>;

void WaitForStop(std::atomic_bool& stop_requested) noexcept {
  while (!stop_requested.load(std::memory_order_acquire)) {
    stop_requested.wait(false, std::memory_order_acquire);
  }
}

// Owns the epoll worker group behind Runtime's cold lifecycle seam. The
// accepted stream remains ReactorStream all the way to ConnectionHandler.
class ReactorRuntimeControl final : public runtime::detail::RuntimeControl {
public:
  ReactorRuntimeControl(net::Endpoint listen_addr, std::size_t worker_count,
                        ReactorBuilder::ConnectionHandler connection_handler) noexcept
      : listen_addr_(listen_addr),
        worker_count_(worker_count),
        connection_handler_(std::move(connection_handler)) {}

  ~ReactorRuntimeControl() noexcept override { Stop(); }

  base::Result<void> Start() override {
    {
      std::lock_guard lock{lifecycle_mutex_};
      if (state_ != LifecycleState::kCreated) {
        return std::unexpected(base::MakeErrno(EALREADY));
      }
      if (worker_count_ == 0 || !connection_handler_) {
        return std::unexpected(base::MakeErrno(EINVAL));
      }
      stop_requested_.store(false, std::memory_order_release);
      state_ = LifecycleState::kStarting;
    }

    detail::ReactorWorkerGroupOptions options;
    options.worker_num = worker_count_;
    // A single listener does not need SO_REUSEPORT; independent workers do.
    options.worker_options.listener_options.reuse_port = worker_count_ > 1;

    auto callback = [this](detail::ReactorWorkerContext&, ReactorStream stream) {
      return connection_handler_(std::move(stream));
    };
    auto workers = std::make_unique<detail::ReactorWorkerGroup>(
        listen_addr_, std::move(options), detail::ReactorWorkerGroup::ThreadInitCallback{},
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

  base::Result<void> Run(std::stop_token stop_token) override {
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
    std::unique_ptr<detail::ReactorWorkerGroup> workers;
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
  ReactorBuilder::ConnectionHandler connection_handler_;
  mutable std::mutex lifecycle_mutex_;
  std::unique_ptr<detail::ReactorWorkerGroup> workers_;
  LifecycleState state_{LifecycleState::kCreated};
  std::atomic_bool stop_requested_{false};
};

}  // namespace

std::unique_ptr<runtime::detail::RuntimeControl> MakeRuntimeControl(
    net::Endpoint listen_addr, std::size_t worker_count,
    Runtime::Builder<runtime::Reactor>::ConnectionHandler connection_handler) {
  return std::make_unique<ReactorRuntimeControl>(listen_addr, worker_count,
                                                 std::move(connection_handler));
}

}  // namespace coropact::reactor

namespace coropact {

Runtime::Builder<runtime::Reactor>::Builder(net::Endpoint listen_addr) noexcept
    : listen_addr_(listen_addr) {}

Runtime::Builder<runtime::Reactor>& Runtime::Builder<runtime::Reactor>::Workers(
    std::size_t count) noexcept {
  worker_count_ = count;
  return *this;
}

Runtime::Builder<runtime::Reactor>& Runtime::Builder<runtime::Reactor>::AutoWorkers() noexcept {
  worker_count_ = std::max<std::size_t>(std::thread::hardware_concurrency(), 1);
  return *this;
}

Runtime::Builder<runtime::Reactor>& Runtime::Builder<runtime::Reactor>::OnConnection(
    ConnectionHandler handler) {
  connection_handler_ = std::move(handler);
  return *this;
}

Runtime Runtime::Builder<runtime::Reactor>::Build() {
  return Runtime{reactor::MakeRuntimeControl(listen_addr_, worker_count_,
                                             std::move(connection_handler_))};
}

}  // namespace coropact
