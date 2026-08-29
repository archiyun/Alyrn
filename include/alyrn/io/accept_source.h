// SPDX-License-Identifier: MIT
#pragma once

#include "alyrn/backend/accept_source.h"

namespace alyrn::io {

using AcceptSourceOptions = backend::AcceptSourceOptions;

template <class T>
concept AsyncAcceptSource = backend::AsyncAcceptSource<T>;

}  // namespace alyrn::io
