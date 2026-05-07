#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/gateway/upstream_group.h"

#include <memory>
#include <utility>
#include <string>
#include <unordered_map>

namespace runtime::gateway {

// Built during startup. After startup, it is read-only and can be resolved without locks.
class ServiceRegistry : public runtime::base::NonCopyable {
public:
  using ServiceRegistryMap = std::unordered_map<std::string, std::shared_ptr<UpStreamGroup>>;

  static ServiceRegistry& Instance() {
    static ServiceRegistry instance;
    return instance;
  }
  void Register(std::string name, std::shared_ptr<UpStreamGroup> group) {
    registry_[std::move(name)] = std::move(group);
  }
  
  std::shared_ptr<UpStreamGroup> Resolve(const std::string& service_name) const {
    auto it = registry_.find(service_name);
    return it == registry_.end() ? nullptr : it->second;
  }

  const ServiceRegistryMap& All() const {
    return registry_;
  }
private:
  ServiceRegistry() = default;
  ServiceRegistryMap registry_;
};
}  // namespace runtime::gateway
