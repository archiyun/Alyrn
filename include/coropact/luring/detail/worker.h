// SPDX-License-Identifier: MIT
#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <thread>

#include "coropact/result.h"
#include "coropact/coro/detached_task.h"
#include "coropact/luring/connector.h"
#include "coropact/luring/listener.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/options.h"
#include "coropact/net/endpoint.h"
#include "coropact/utils/macros.h"

namespace coropact::luring::detail {

enum class AcceptMode : std::uint8_t {
  kSingleShot,
  kMultishot,
};

struct LUringWorkerContext {
  LUringWorkerContext(std::size_t index, LUringLoop& loop, LUringListener& listener,
                      LUringConnector& connector) noexcept
      : index(index), loop(loop), listener(listener), connector(connector) {}

  COROPACT_DELETE_COPY_MOVE(LUringWorkerContext);

  const std::size_t index;
  LUringLoop& loop;
  LUringListener& listener;
  LUringConnector& connector;
};

struct LUringWorkerOptions {
  LUringOptions loop_options{};
  LUringListenOptions listen_options{};

  // Selects the logical accept implementation used by the worker. The
  // multishot source preserves the same ConnectionCallback contract while
  // allowing one native accept request to produce multiple accepted streams.
  AcceptMode accept_mode{AcceptMode::kSingleShot};

  // Optional resource for coroutine frames created while this worker resumes
  // work. The resource must outlive the worker group.
  std::pmr::memory_resource* frame_resource{nullptr};

  // Optional CPU to which this worker thread is pinned. Leave unset to use
  // the process scheduler's normal placement policy.
  std::optional<unsigned> cpu_affinity;
};

class LUringWorker {
public:
  COROPACT_DELETE_COPY_MOVE(LUringWorker);

  using ThreadInitCallback = std::function<void(LUringWorkerContext&)>;
  // Runs on the worker thread after the loop has drained and before loop-bound
  // listener/connector resources are destroyed.
  using ThreadExitCallback = std::function<void(LUringWorkerContext&)>;
  using ConnectionCallback =
      std::function<coro::DetachedTask(LUringWorkerContext&, LUringStream)>;

  LUringWorker(std::size_t index, net::Endpoint listen_addr, LUringWorkerOptions options = {},
               ThreadInitCallback init_callback = {}, ConnectionCallback connection_callback = {},
               ThreadExitCallback exit_callback = {});
  ~LUringWorker() noexcept;

  [[nodiscard]]
  Result<void> Start();
  void Stop() noexcept;

  [[nodiscard]]
  std::size_t Index() const noexcept { return index_; }

private:
  void WorkLoop(std::stop_token token) noexcept;

  std::size_t index_;
  net::Endpoint listen_addr_;
  LUringWorkerOptions options_;
  ThreadInitCallback init_callback_;
  ConnectionCallback connection_callback_;
  ThreadExitCallback exit_callback_;

  std::mutex mutex_;
  std::condition_variable_any cv_;
  Result<void> start_result_;
  bool init_done_{false};

  std::jthread thread_;
};

}  // namespace coropact::luring::detail
