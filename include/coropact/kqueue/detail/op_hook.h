// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <utility>

#include "coropact/result.h"

namespace coropact::kqueue::detail {

// Vtable-free operation hook. KqueueStream stores the erased awaiter address
// plus a kind; this CRTP hook restores the concrete awaiter at the dispatch
// site.
template <typename TOwner>
class KqueueOperationHook {
public:
  [[nodiscard]]
  bool CompleteResult(Result<std::size_t> result) noexcept {
    return static_cast<TOwner*>(this)->CompleteResultImpl(std::move(result));
  }

  void OnReady() noexcept { static_cast<TOwner*>(this)->OnReadyImpl(); }
};

static_assert(sizeof(KqueueOperationHook<void>) == 1);

}  // namespace coropact::kqueue::detail
