// SPDX-License-Identifier: MIT
#pragma once

// Application-facing spelling for Alyrn's lazy coroutine result.
//
// The implementation remains physically grouped with coroutine machinery,
// but Task is part of the root Alyrn vocabulary.  Keeping this forwarding
// header also gives us one public seam while the implementation is split into
// smaller internal modules.
#include "alyrn/coro/task.h"

namespace alyrn {

template <class T>
concept Returnable = coro::Returnable<T>;

template <Returnable T = void>
using Task = coro::Task<T>;

}  // namespace alyrn
