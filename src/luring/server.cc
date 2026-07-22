// Copyright (c) 2026 Arsenova. All Rights Reserved.
#include "vexo/luring/server.h"

#include <cerrno>
#include <expected>
#include <memory>
#include <utility>

#include "vexo/base/error.h"
#include "vexo/luring/stream.h"

namespace vexo::luring {

LUringServer::LUringServer(net::InetAddress listen_addr, LUringServerOptions options)
    : listen_addr_(listen_addr), options_(std::move(options)) {}

LUringServer::~LUringServer() noexcept { Stop(); }

base::Result<void> LUringServer::Start() {
  if (started_) {
    return std::unexpected(base::make_errno(EALREADY));
  }

  LUringWorkerGroup::ConnectionCallback connection_callback;
  if (session_handler_) {
    connection_callback = [this](LUringWorkerContext& context, LUringStream stream) {
      return session_handler_(context, std::move(stream));
    };
  }

  workers_ =
      std::make_unique<LUringWorkerGroup>(listen_addr_, options_.worker_group_options,
                                          thread_init_callback_, std::move(connection_callback));
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
