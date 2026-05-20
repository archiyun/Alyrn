// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/gateway/health_check_config.h"
#include "runtime/gateway/upstream_registry.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/timer_id.h"

#include <memory>
#include <string>
#include <unordered_map>

namespace runtime::gateway {

// 主动健康检查：定时对每个 backend 发 GET /health，
// 连续失败 unhealthy_threshold 次 → healthy=false；
// 连续成功 healthy_threshold 次   → healthy=true, fail_count=0。
// 被动摘除由 ProxySession::OnUpstreamConnChange 负责。
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
  runtime::net::TimerId timer_id_;
  bool running_{false};

  // per-stream 连续成功计数 (不持久化， 重启归零)
  std::unordered_map<std::string, int> consecutive_ok_;
  // per-upstream 连续失败次数 
  std::unordered_map<std::string, int> consecutive_fail_;
};
} // namespace runtime::gateway
