#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/gateway/service_registry.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/timer_id.h"

#include <memory>
#include <string>

namespace runtime::gateway {

struct HealthCheckConfig {
  std::string path{"/health"};
  double interval_sec{10.0};
  double timeout_sec{3.0};
  int unhealthy_threshold{3};
  int healthy_threshold{2};
};


// 主动健康检查：定时对每个 backend 发 GET /health，
// 连续失败 unhealthy_threshold 次 → healthy=false；
// 连续成功 healthy_threshold 次   → healthy=true, fail_count=0。
// 被动摘除由 ProxySession::OnUpstreamConnChange 负责。
class HealthChecker : public runtime::base::NonCopyable {
public:
  HealthChecker(runtime::net::EventLoop* loop, ServiceRegistry& registry, HealthCheckConfig cfg = {});
  void Start();
  void Stop();
private:
  void CheckAll();
  void CheckOne(std::shared_ptr<Backend> backend);

  runtime::net::EventLoop* loop_;
  ServiceRegistry& registry_;
  HealthCheckConfig cfg_;
  runtime::net::TimerId timer_id_;
  bool running_{false};

  // per-backend 连续成功计数 (不持久化， 重启归零)
  std::unordered_map<std::string, int> consecutive_ok_;
};
} // namespace runtime::gateway