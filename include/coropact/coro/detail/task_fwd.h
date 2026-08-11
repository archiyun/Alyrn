// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>
#include <type_traits>

namespace coropact::coro {

// A Task yields void or a movable object. Values live in the coroutine frame,
// so non-void results must be move-constructible.
template <class T>
concept Returnable =
    std::is_void_v<T> || (std::is_object_v<T> && !std::is_array_v<T> && std::move_constructible<T>);

template <Returnable T = void>
class Task;

}  // namespace coropact::coro
