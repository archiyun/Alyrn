#pragma once

#include <string>

namespace runtime::gateway {

struct HealthCheckConfig {
  std::string path{"/health"};  // 健康检查请求路径
  double interval_sec{10.0};  // 检查间隔(单位: 秒)
  double timeout_sec{3.0};  // 单次检查超时
  int unhealthy_threshold{3}; // 连续失败 N 次 -> 标记 Down
  int healthy_threshold{2}; // 连续成功N次 -> 标记 Up
};

}  // namespace runtime::gateway
