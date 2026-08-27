// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <utility>

#include "alyrn/result.h"

namespace alyrn::kqueue::detail {

// Vtable-free operation hook. Stream stores the erased awaiter address
// plus a kind; this CRTP hook restores the concrete awaiter at the dispatch
// site.
template <typename TOwner>
class OperationHook {
public:
  [[nodiscard]]
  bool CompleteResult(Result<std::size_t> result) noexcept {
    return static_cast<TOwner*>(this)->CompleteResultImpl(std::move(result));
  }

  void OnReady() noexcept { static_cast<TOwner*>(this)->OnReadyImpl(); }
};

static_assert(sizeof(OperationHook<void>) == 1);

}  // namespace alyrn::kqueue::detail
