#pragma once

#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

#include "vexo/base/error.h"
#include "vexo/luring/listener.h"
#include "vexo/luring/loop.h"
#include "vexo/luring/options.h"
#include "vexo/net/inet_address.h"
#include "vexo/utils/macros.h"

namespace vexo::luring {

struct LUringWorkerOptions {
  LUringOptions loop_options{};
  LUringListenOptions listen_options{};
};

class LUringWorker {
public:
  VEXO_DELETE_COPY_MOVE(LUringWorker);

  using ThreadInitCallback = std::function<void(LUringLoop*, LUringListener*)>;
  using ConnectionCallback = std::function<void(LUringLoop&, std::unique_ptr<LUringStream>)>;

  LUringWorker(net::InetAddress listen_addr, LUringWorkerOptions options = {},
               ThreadInitCallback init_callback = {}, ConnectionCallback connection_callback = {});
  ~LUringWorker();

  [[nodiscard]] base::Result<void> Start();
  void Stop() noexcept;

  LUringLoop* loop() noexcept { return loop_; }
  LUringListener* listener() noexcept { return listener_; }

private:
  void WorkLoop(std::stop_token token) noexcept;

  net::InetAddress listen_addr_;
  LUringWorkerOptions options_;
  ThreadInitCallback init_callback_;
  ConnectionCallback connection_callback_;

  std::jthread thread_;

  std::mutex mutex_;
  std::condition_variable_any cv_;
  base::Result<void> start_result_{};
  bool started_{false};

  LUringLoop* loop_{nullptr};
  LUringListener* listener_{nullptr};
};

}  // namespace vexo::luring
