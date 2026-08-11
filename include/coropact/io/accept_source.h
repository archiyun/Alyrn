// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/backend/accept_source.h"

namespace coropact::io {

// Public facade spelling. The admission primitive itself belongs to the
// lower networking layer and is intentionally not owned by this facade.
using AcceptSourceOptions = backend::AcceptSourceOptions;

template <class T>
concept AsyncAcceptSource = backend::AsyncAcceptSource<T>;

}  // namespace coropact::io
