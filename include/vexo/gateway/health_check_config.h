// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <string>

namespace vexo::gateway {

struct HealthCheckConfig {
  // Request path used by active health probes.
  std::string path{"/health"};

  // Interval between probe rounds, in seconds.
  double interval_sec{10.0};

  // Per-probe timeout, in seconds.
  double timeout_sec{3.0};

  // Consecutive failed probes required before marking a peer down.
  int unhealthy_threshold{3};

  // Consecutive successful probes required before marking a peer up.
  int healthy_threshold{2};
};

}  // namespace vexo::gateway
