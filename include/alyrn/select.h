// SPDX-License-Identifier: MIT
#pragma once

// Application-facing channel selection operations.
#include "alyrn/coro/select.h"  // IWYU pragma: export

namespace alyrn {

using coro::Select;
using coro::SelectDefault;
using coro::SelectReceive;
using coro::SelectSend;

}  // namespace alyrn
