// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <utility>

#include "alyrn/result.h"

namespace alyrn::epoll::detail {

// A vtable-free operation hook. Stream stores the erased awaiter
// address plus a kind, while this CRTP hook restores the concrete awaiter type
// at the dispatch site.
template <typename TOwner>
class OperationHook {
public:
  bool CompleteResult(Result<std::size_t> result) noexcept {
    return static_cast<TOwner*>(this)->CompleteResultImpl(std::move(result));
  }

  void OnReady() noexcept { static_cast<TOwner*>(this)->OnReadyImpl(); }
};

static_assert(sizeof(OperationHook<void>) == 1);

}  // namespace alyrn::epoll::detail
