// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <thread>

#include "coropact/base/error.h"
#include "coropact/coro/task.h"
#include "coropact/luring/connector.h"
#include "coropact/luring/listener.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/options.h"
#include "coropact/net/inet_address.h"
#include "coropact/utils/macros.h"

namespace coropact::luring {

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
  using ConnectionCallback = std::function<coro::Task<void>(LUringWorkerContext&, LUringStream)>;

  LUringWorker(std::size_t index, net::InetAddress listen_addr, LUringWorkerOptions options = {},
               ThreadInitCallback init_callback = {}, ConnectionCallback connection_callback = {});
  ~LUringWorker() noexcept;

  [[nodiscard]] base::Result<void> Start();
  void Stop() noexcept;

  [[nodiscard]] std::size_t index() const noexcept { return index_; }

private:
  void WorkLoop(std::stop_token token) noexcept;

  std::size_t index_;
  net::InetAddress listen_addr_;
  LUringWorkerOptions options_;
  ThreadInitCallback init_callback_;
  ConnectionCallback connection_callback_;

  std::mutex mutex_;
  std::condition_variable_any cv_;
  base::Result<void> start_result_;
  bool init_done_{false};

  std::jthread thread_;
};

}  // namespace coropact::luring
