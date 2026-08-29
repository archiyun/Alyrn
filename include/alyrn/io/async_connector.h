// SPDX-License-Identifier: MIT
#pragma once

#include "alyrn/backend/async_connector.h"

namespace alyrn::io {

template <class T>
concept AsyncConnector = backend::AsyncConnector<T>;

}  // namespace alyrn::io
