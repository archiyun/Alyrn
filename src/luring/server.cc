// SPDX-License-Identifier: MIT
#include "alyrn/luring/detail/server.h"

#include <cerrno>
#include <expected>
#include <memory>
#include <utility>

#include "alyrn/result.h"
#include "alyrn/luring/stream.h"

namespace alyrn::luring::detail {

Server::Server(net::Endpoint listen_addr, ServerOptions options)
    : listen_addr_(listen_addr), options_(std::move(options)) {}

Server::~Server() noexcept { Stop(); }

Result<void> Server::Start() {
  if (started_) {
    return std::unexpected(Errno(EALREADY));
  }

  WorkerGroup::ConnectionCallback connection_callback;
  if (session_handler_) {
    connection_callback = [this](WorkerContext& context, Stream stream) {
      return session_handler_(context, std::move(stream));
    };
  }

  workers_ =
      std::make_unique<WorkerGroup>(listen_addr_, options_.worker_group_options,
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

void Server::Stop() noexcept {
  if (workers_) {
    workers_->Stop();
    workers_.reset();
  }
  started_ = false;
}

}  // namespace alyrn::luring::detail
