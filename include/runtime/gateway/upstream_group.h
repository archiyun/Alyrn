#pragma once

#include "runtime/gateway/backend.h"

#include <memory>
#include <utility>
#include <atomic>
#include <string>
#include <vector>

namespace runtime::gateway {


class UpStreamGroup {
public:
  explicit UpStreamGroup(std::string name) : name_(std::move(name)) {}
  void AddBackend(std::shared_ptr<Backend> b) {
    backends_.push_back(std::move(b));
  }
  std::vector<std::shared_ptr<Backend>> HealthyBackends() const {
    std::vector<std::shared_ptr<Backend>> res;
    res.reserve(backends_.size());
    for (auto& backend : backends_) {
      if (backend && backend->state_.healthy.load(std::memory_order_relaxed)) {
        res.push_back(backend);
      }
    }
    return res;
  }

  const std::vector<std::shared_ptr<Backend>>& Backends() const {
    return backends_;
  }

  const std::string& Name() const {
    return name_;
  }
private:
  std::string name_;
  std::vector<std::shared_ptr<Backend>> backends_;
};

}  // namespace runtime::gateway
