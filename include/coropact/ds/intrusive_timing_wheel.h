// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cassert>
#include <concepts>
#include <cstdint>
#include <vector>

namespace coropact::ds {

template <class T, auto kLess, class Tag = void>
class IntrusiveTimingWheel;
}  // namespace coropact::ds
