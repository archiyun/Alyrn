// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "runtime/base/noncopyable.h"
#include "runtime/gateway/health_check_config.h"
#include "runtime/gateway/upstream_registry.h"
#include "runtime/net/event_loop.h"
#include "runtime/time/timer_id.h"

namespace runtime::gateway {

// Active health checker for upstream peers.
//
// Periodically sends an HTTP GET probe to each registered backend. A peer is
// marked down after `unhealthy_threshold` consecutive failures and marked up
// again after `healthy_threshold` consecutive successes.
//
// Passive failure handling is performed by the proxy path when upstream
// connections fail during request forwarding.
class HealthChecker : public runtime::base::NonCopyable {
public:
  HealthChecker(runtime::net::EventLoop* loop, UpstreamRegistry& registry, HealthCheckConfig cfg = {});
  void Start();
  void Stop();
private:
  void CheckAll();
  void CheckOne(std::shared_ptr<UpstreamPeer> peer);

  runtime::net::EventLoop* loop_;
  UpstreamRegistry& registry_;
  HealthCheckConfig cfg_;
  runtime::time::TimerId timer_id_;
  bool running_{false};

  // Consecutive successful probes per peer. Not persisted across restarts.
  std::unordered_map<std::string, int> consecutive_ok_;

  // Consecutive failed probes per peer.
  std::unordered_map<std::string, int> consecutive_fail_;
};
}  // namespace runtime::gateway
