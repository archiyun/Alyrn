// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <cerrno>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "coropact/gateway/upstream.h"
#include "coropact/base/error.h"
#include "coropact/utils/macros.h"

namespace coropact::gateway {

struct StringHash {
  using is_transparent = void;
  std::size_t operator()(std::string_view sv) const noexcept {
    return std::hash<std::string_view>{}(sv);
  }
};

// Built during startup. After startup, it is read-only and can be resolved without locks.
class UpstreamRegistry {
public:
  COROPACT_DELETE_COPY_MOVE(UpstreamRegistry);

  UpstreamRegistry() noexcept = default;
  ~UpstreamRegistry() noexcept = default;

  using UpstreamRegistryMap = std::unordered_map<
      std::string,
      std::shared_ptr<Upstream>,
      StringHash,
      std::equal_to<>>;

  [[nodiscard]]
  base::Result<void> Register(std::shared_ptr<Upstream> upstream) {
    if (upstream == nullptr) {
      return std::unexpected(base::MakeErrno(EINVAL));
    }

    const std::string name = upstream->name();
    if (name.empty()) {
      return std::unexpected(base::MakeErrno(EINVAL));
    }

    const bool inserted = registry_.emplace(name, std::move(upstream)).second;
    if (!inserted) {
      return std::unexpected(base::MakeErrno(EEXIST));
    }

    return {};
  }

  [[nodiscard]]
  base::Result<std::shared_ptr<Upstream>> Resolve(std::string_view name) const {
    if (name.empty()) {
      return std::unexpected(base::MakeErrno(EINVAL));
    }

    auto iterator = registry_.find(name);
    if (iterator == registry_.end()) {
      return std::unexpected(base::MakeErrno(ENOENT));
    }
    return iterator->second;
  }

  const UpstreamRegistryMap& all() const noexcept {
    return registry_;
  }

private:
  UpstreamRegistryMap registry_;
};

}  // namespace coropact::gateway
