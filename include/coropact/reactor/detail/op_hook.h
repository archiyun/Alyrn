// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <utility>

#include "coropact/result.h"

namespace coropact::reactor::detail {

// A vtable-free operation hook. ReactorStream stores the erased awaiter
// address plus a kind, while this CRTP hook restores the concrete awaiter type
// at the dispatch site.
template <typename TOwner>
class ReactorOperationHook {
public:
  [[nodiscard]]
  bool CompleteResult(Result<std::size_t> result) noexcept {
    return static_cast<TOwner*>(this)->CompleteResultImpl(std::move(result));
  }

  void OnReady() noexcept { static_cast<TOwner*>(this)->OnReadyImpl(); }
};

static_assert(sizeof(ReactorOperationHook<void>) == 1);

}  // namespace coropact::reactor::detail
