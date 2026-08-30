// SPDX-License-Identifier: MIT
#include "alyrn/uring/runtime.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stop_token>
#include <thread>
#include <utility>

#include "alyrn/result.h"
#include "alyrn/uring/detail/worker_group.h"

namespace alyrn::uring {

namespace {

using Builder = Runtime::Builder<runtime::Uring>;

void WaitForStop(std::atomic_bool& stop_requested) noexcept {
  while (!stop_requested.load(std::memory_order_acquire)) {
    stop_requested.wait(false, std::memory_order_acquire);
  }
}

// Owns the ring worker group behind Runtime's cold lifecycle seam. The
// accepted stream remains Stream all the way to ConnectionHandler.
class RuntimeControl final : public ::alyrn::detail::runtime::RuntimeControl {
public:
  RuntimeControl(net::Endpoint listen_addr, std::size_t worker_count,
                       Builder::ConnectionHandler connection_handler) noexcept
      : listen_addr_(listen_addr),
        worker_count_(worker_count),
        connection_handler_(std::move(connection_handler)) {}

  ~RuntimeControl() noexcept override { Stop(); }

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

    detail::WorkerGroupOptions options;
    options.worker_num = worker_count_;
    // Runtime tries native multishot accept; the source retains its capability
    // fallback when the active ring cannot use it.
    options.worker_options.accept_mode = detail::AcceptMode::kMultishot;
    options.worker_options.listen_options.reuse_port = worker_count_ > 1;

    auto callback = [this](detail::WorkerContext&, Stream stream) {
      return connection_handler_(std::move(stream));
    };
    auto workers = std::make_unique<detail::WorkerGroup>(
        listen_addr_, std::move(options), detail::WorkerGroup::ThreadInitCallback{},
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
    std::unique_ptr<detail::WorkerGroup> workers;
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
  Builder::ConnectionHandler connection_handler_;
  mutable std::mutex lifecycle_mutex_;
  std::unique_ptr<detail::WorkerGroup> workers_;
  LifecycleState state_{LifecycleState::kCreated};
  std::atomic_bool stop_requested_{false};
};

}  // namespace

std::unique_ptr<::alyrn::detail::runtime::RuntimeControl> MakeRuntimeControl(
    net::Endpoint listen_addr, std::size_t worker_count,
    Runtime::Builder<runtime::Uring>::ConnectionHandler connection_handler) {
  return std::make_unique<RuntimeControl>(listen_addr, worker_count,
                                                std::move(connection_handler));
}

}  // namespace alyrn::uring

namespace alyrn {

Runtime::Builder<runtime::Uring>::Builder(net::Endpoint listen_addr) noexcept
    : listen_addr_(listen_addr) {}

Runtime::Builder<runtime::Uring>& Runtime::Builder<runtime::Uring>::Workers(
    std::size_t count) noexcept {
  worker_count_ = count;
  return *this;
}

Runtime::Builder<runtime::Uring>& Runtime::Builder<runtime::Uring>::AutoWorkers() noexcept {
  worker_count_ = std::max<std::size_t>(std::thread::hardware_concurrency(), 1);
  return *this;
}

Runtime::Builder<runtime::Uring>& Runtime::Builder<runtime::Uring>::OnConnection(
    ConnectionHandler handler) {
  connection_handler_ = std::move(handler);
  return *this;
}

Runtime Runtime::Builder<runtime::Uring>::Build() {
  return Runtime{uring::MakeRuntimeControl(listen_addr_, worker_count_,
                                            std::move(connection_handler_))};
}

}  // namespace alyrn
