#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/gateway/upstream.h"

#include <memory>
#include <utility>
#include <string>
#include <string_view>
#include <unordered_map>

namespace runtime::gateway {

// Built during startup. After startup, it is read-only and can be resolved without locks.
class UpstreamRegistry : public runtime::base::NonCopyable {
public:
  using UpstreamRegistryMap = std::unordered_map<std::string, std::shared_ptr<Upstream>>;

  void Add(std::shared_ptr<Upstream> upstream) {
    registry_.emplace(upstream->Name(), std::move(upstream));
  }

  std::shared_ptr<Upstream> Find(std::string_view name) const {
    if (auto it = registry_.find(std::string(name));
        it != registry_.end()) {
      return it->second;
    }
    return nullptr;
  }
  const UpstreamRegistryMap& All() const {
    return registry_;
  }
private:
  UpstreamRegistryMap registry_;
};
}  // namespace runtime::gateway
