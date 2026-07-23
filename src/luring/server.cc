// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/luring/server.h"

#include <cerrno>
#include <expected>
#include <memory>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/luring/stream.h"

namespace coropact::luring {

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

}  // namespace coropact::luring
