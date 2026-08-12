// SPDX-License-Identifier: MIT
#include "coropact/luring/detail/server.h"

#include <cerrno>
#include <expected>
#include <memory>
#include <utility>

#include "coropact/result.h"
#include "coropact/luring/stream.h"

namespace coropact::luring::detail {

LUringServer::LUringServer(net::Endpoint listen_addr, LUringServerOptions options)
    : listen_addr_(listen_addr), options_(std::move(options)) {}

LUringServer::~LUringServer() noexcept { Stop(); }

Result<void> LUringServer::Start() {
  if (started_) {
    return std::unexpected(Errno(EALREADY));
  }

  LUringWorkerGroup::ConnectionCallback connection_callback;
  if (session_handler_) {
    connection_callback = [this](LUringWorkerContext& context, LUringStream stream) {
      return session_handler_(context, std::move(stream));
    };
  }

  workers_ =
      std::make_unique<LUringWorkerGroup>(listen_addr_, options_.worker_group_options,
                                          thread_init_callback_, std::move(connection_callback),
                                          thread_exit_callback_);
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

}  // namespace coropact::luring::detail
