// SPDX-License-Identifier: MIT
#pragma once

namespace alyrn::detail::time {

// Selects the ordered store used by a backend TimerQueue. This is an
// implementation policy, not an application-facing scheduling abstraction.
enum class TimerIndexKind { kRbTree };

}  // namespace alyrn::detail::time
