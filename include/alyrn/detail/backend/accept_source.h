// SPDX-License-Identifier: MIT
#pragma once

#include "alyrn/io/accept_source.h"

namespace alyrn::detail::backend {

using AcceptSourceOptions = io::AcceptSourceOptions;

template <class T>
concept AsyncAcceptSource = io::AsyncAcceptSource<T>;

}  // namespace alyrn::detail::backend
