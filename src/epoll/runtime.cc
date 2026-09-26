// SPDX-License-Identifier: MIT
#include "alyrn/epoll/runtime.h"

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
#include "alyrn/epoll/detail/worker_group.h"

namespace alyrn::epoll {

namespace {

using Builder = Runtime::Builder<runtime::Epoll>;

void WaitForStop(std::atomic_bool& stop_requested) noexcept {
  while (!stop_requested.load(std::memory_order_acquire)) {
    stop_requested.wait(false, std::memory_order_acquire);
  }
}

// Owns the epoll worker group behind Runtime's cold lifecycle seam. The
// accepted stream remains Stream all the way to ConnectionHandler.
class RuntimeControl final : public ::alyrn::detail::runtime::RuntimeControl {
public:
  RuntimeControl(net::Endpoint listen_addr, std::size_t worker_count,
                 net::TcpOptions tcp_options, Builder::ConnectionHandler connection_handler,
                 Builder::WorkerStartHook start_hook, Builder::WorkerStopHook stop_hook) noexcept
      : listen_addr_(listen_addr),
        worker_count_(worker_count),
        tcp_options_(tcp_options),
        connection_handler_(std::move(connection_handler)),
        start_hook_(std::move(start_hook)),
        stop_hook_(std::move(stop_hook)) {}

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
    // A single listener does not need SO_REUSEPORT; independent workers do.
    options.worker_options.listener_options.reuse_port = worker_count_ > 1;
    options.worker_options.listener_options.tcp_options = tcp_options_;

    auto callback = [this](detail::WorkerContext&, Stream stream) {
      return connection_handler_(std::move(stream));
    };
    // Each flag is written by the start hook and read by the stop hook on the
    // same worker thread; worker threads start after this allocation.
    worker_started_ = std::make_unique<bool[]>(worker_count_);
    detail::WorkerGroup::ThreadInitCallback init_callback;
    if (start_hook_) {
      init_callback = [this](detail::WorkerContext& context) {
        auto started = start_hook_(context.loop, context.index);
        if (!started.HasValue()) {
          context.start_result = std::move(started);
          return;
        }
        worker_started_[context.index] = true;
      };
    }
    detail::WorkerGroup::ThreadExitCallback exit_callback;
    if (stop_hook_) {
      exit_callback = [this](detail::WorkerContext& context) {
        if (!start_hook_ || worker_started_[context.index]) {
          stop_hook_(context.loop, context.index);
        }
      };
    }
    auto workers = std::make_unique<detail::WorkerGroup>(
        listen_addr_, std::move(options), std::move(init_callback), std::move(callback),
        std::move(exit_callback));
    auto started = workers->Start();
    if (!started.HasValue()) {
      std::lock_guard lock{lifecycle_mutex_};
      stop_requested_.store(false, std::memory_order_release);
      state_ = LifecycleState::kCreated;
      return std::unexpected(started.Error());
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
    if (!started.HasValue()) {
      return std::unexpected(started.Error());
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
  net::TcpOptions tcp_options_;
  Builder::ConnectionHandler connection_handler_;
  Builder::WorkerStartHook start_hook_;
  Builder::WorkerStopHook stop_hook_;
  std::unique_ptr<bool[]> worker_started_;
  mutable std::mutex lifecycle_mutex_;
  std::unique_ptr<detail::WorkerGroup> workers_;
  LifecycleState state_{LifecycleState::kCreated};
  std::atomic_bool stop_requested_{false};
};

}  // namespace

std::unique_ptr<::alyrn::detail::runtime::RuntimeControl> MakeRuntimeControl(
    net::Endpoint listen_addr, std::size_t worker_count, net::TcpOptions tcp_options,
    Runtime::Builder<runtime::Epoll>::ConnectionHandler connection_handler,
    Runtime::Builder<runtime::Epoll>::WorkerStartHook start_hook,
    Runtime::Builder<runtime::Epoll>::WorkerStopHook stop_hook) {
  return std::make_unique<RuntimeControl>(listen_addr, worker_count, tcp_options,
                                          std::move(connection_handler), std::move(start_hook),
                                          std::move(stop_hook));
}

}  // namespace alyrn::epoll

namespace alyrn {

Runtime::Builder<runtime::Epoll>::Builder(net::Endpoint listen_addr) noexcept
    : listen_addr_(listen_addr) {}

Runtime::Builder<runtime::Epoll>& Runtime::Builder<runtime::Epoll>::Workers(
    std::size_t count) noexcept {
  worker_count_ = count;
  return *this;
}

Runtime::Builder<runtime::Epoll>& Runtime::Builder<runtime::Epoll>::AutoWorkers() noexcept {
  worker_count_ = std::max<std::size_t>(std::thread::hardware_concurrency(), 1);
  return *this;
}

Runtime::Builder<runtime::Epoll>& Runtime::Builder<runtime::Epoll>::Tcp(
    net::TcpOptions options) noexcept {
  tcp_options_ = options;
  return *this;
}

Runtime::Builder<runtime::Epoll>& Runtime::Builder<runtime::Epoll>::OnConnection(
    ConnectionHandler handler) {
  connection_handler_ = std::move(handler);
  return *this;
}

Runtime::Builder<runtime::Epoll>& Runtime::Builder<runtime::Epoll>::OnWorkerStart(
    WorkerStartHook hook) {
  worker_start_hook_ = std::move(hook);
  return *this;
}

Runtime::Builder<runtime::Epoll>& Runtime::Builder<runtime::Epoll>::OnWorkerStop(
    WorkerStopHook hook) {
  worker_stop_hook_ = std::move(hook);
  return *this;
}

Runtime Runtime::Builder<runtime::Epoll>::Build() {
  return Runtime{epoll::MakeRuntimeControl(listen_addr_, worker_count_, tcp_options_,
                                           std::move(connection_handler_),
                                           std::move(worker_start_hook_),
                                           std::move(worker_stop_hook_))};
}

}  // namespace alyrn
