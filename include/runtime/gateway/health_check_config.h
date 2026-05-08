#pragma once

#include <string>

namespace runtime::gateway {

struct HealthCheckConfig {
  std::string path{"/health"};
  double interval_sec{10.0};
  double timeout_sec{3.0};
  int unhealthy_threshold{3};
  int healthy_threshold{2};
};

}  // namespace runtime::gateway
