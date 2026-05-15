#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/gateway/upstream.h"

#include <memory>
#include <utility>
#include <string>
#include <string_view>
#include <unordered_map>

namespace runtime::gateway {

struct StringHash {
  using is_transparent = void;
  std::size_t operator()(std::string_view sv) const noexcept {
    return std::hash<std::string_view>{}(sv);
  }
};

// Built during startup. After startup, it is read-only and can be resolved without locks.
class UpstreamRegistry : public runtime::base::NonCopyable {
public:
  using UpstreamRegistryMap = std::unordered_map<
      std::string, 
      std::shared_ptr<Upstream>,
      StringHash,
      std::equal_to<>>;

  void Add(std::shared_ptr<Upstream> upstream) {
    registry_.emplace(upstream->Name(), std::move(upstream));
  }

  std::shared_ptr<Upstream> Find(std::string_view name) const {
    auto it = registry_.find(name);
    return it != registry_.end() ? it->second : nullptr;
  }
  
  const UpstreamRegistryMap& All() const {
    return registry_;
  }

private:
  UpstreamRegistryMap registry_;
};

}  // namespace runtime::gateway
