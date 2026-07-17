// Copyright (c) 2026 Arsenova. All Rights Reserved.
#include "vexo/luring/server.h"

#include <cerrno>
#include <expected>
#include <memory>
#include <utility>

#include "vexo/base/error.h"
#include "vexo/coro/spawn.h"
#include "vexo/luring/stream.h"

namespace vexo::luring {

LUringServer::LUringServer(net::InetAddress listen_addr, LUringServerOptions options)
    : listen_addr_(listen_addr), options_(std::move(options)) {}

base::Result<void> LUringServer::Start() {
  if (started_) {
    return std::unexpected(base::make_errno(EALREADY));
  }

  workers_ = std::make_unique<LUringWorkerGroup>(
      listen_addr_, options_.worker_group_options, thread_init_callback_,
      [this](LUringLoop& loop, std::unique_ptr<LUringStream> stream) {
        if (session_handler_) {
          coro::Spawn(loop, session_handler_(loop, std::move(stream))).Detach();
        }
      });
  auto started = workers_->Start();
  if (!started.has_value()) {
    workers_.reset();
    return std::unexpected(started.error());
  }
  started_ = true;
  return {};
}

void LUringServer::Stop() noexcept {
  if (workers_) {
    workers_->Stop();
    workers_.reset();
  }
  started_ = false;
}

}  // namespace vexo::luring
