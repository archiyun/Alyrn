// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <utility>

#include "coropact/base/error.h"

namespace coropact::reactor::detail {

// A vtable-free operation hook. ReactorStream stores the erased awaiter
// address plus a kind, while this CRTP hook restores the concrete awaiter type
// at the dispatch site.
template <typename TOwner>
class ReactorOperationHook {
public:
  void Complete(base::Result<std::size_t> result) noexcept {
    static_cast<TOwner*>(this)->CompleteImpl(std::move(result));
  }

  void OnReady() noexcept {
    static_cast<TOwner*>(this)->OnReadyImpl();
  }
};

static_assert(sizeof(ReactorOperationHook<void>) == 1);

}  // namespace coropact::reactor::detail
