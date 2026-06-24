// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "vexo/gateway/health_check_config.h"
#include "vexo/gateway/upstream_registry.h"
#include "vexo/net/event_loop.h"
#include "vexo/time/timer_id.h"
#include "vexo/utils/macros.h"

namespace vexo::gateway {

// Active health checker for upstream peers.
//
// Periodically sends an HTTP GET probe to each registered backend. A peer is
// marked down after `unhealthy_threshold` consecutive failures and marked up
// again after `healthy_threshold` consecutive successes.
//
// Passive failure handling is performed by the proxy path when upstream
// connections fail during request forwarding.
class HealthChecker {
public:
  HealthChecker(vexo::net::EventLoop* loop, UpstreamRegistry& registry, HealthCheckConfig cfg = {});
  ~HealthChecker();

  VEXO_DELETE_COPY_MOVE(HealthChecker);

  void Start();
  void Stop();
private:
  struct Probe;
  struct State;

  static void CheckAll(const std::shared_ptr<State>& state,
                       uint64_t generation);
  static void CheckOne(const std::shared_ptr<State>& state,
                       uint64_t generation,
                       std::shared_ptr<UpstreamPeer> peer);
  static bool CompleteProbe(
      const std::shared_ptr<State>& state,
      uint64_t generation,
      const std::shared_ptr<UpstreamPeer>& peer,
      const std::string& name,
      const std::shared_ptr<Probe>& probe,
      bool success);

  vexo::net::EventLoop* loop_;
  std::shared_ptr<State> state_;
  vexo::time::TimerId timer_id_;
  std::atomic<bool> running_{false};
};
}  // namespace vexo::gateway
