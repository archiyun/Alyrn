// SPDX-License-Identifier: MIT
#pragma once

namespace alyrn::time {

// Selects the ordered store used by a Loop's TimerQueue. The store is fixed
// at Loop construction. Applications pass this to Loop; they do not construct
// Timer, TimerTree, or TimerIndex themselves.
enum class TimerIndexKind { kRbTree };

}  // namespace alyrn::time
