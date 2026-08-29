// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>
#include <cstddef>
#include <utility>
#include <vector>

#include "alyrn/detail/time/timer_tree.h"
#include "alyrn/detail/time/timer_index_kind.h"
#include "alyrn/detail/macros.h"

namespace alyrn::detail::time {

// Ordered timer index used by backend TimerQueues.
class TimerIndex {
public:
  ALYRN_DELETE_COPY_MOVE(TimerIndex);

  explicit TimerIndex(TimerIndexKind = TimerIndexKind::kRbTree) noexcept {}

  bool Empty() const {
    return store_.Empty();
  }

  std::size_t Size() const {
    return store_.Size();
  }

  bool Insert(Timer* timer) {
    return store_.Insert(timer);
  }

  bool Erase(Timer* timer) {
    return store_.Erase(timer);
  }

  Timer* Earliest() {
    return store_.Earliest();
  }

  const Timer* Earliest() const {
    return store_.Earliest();
  }

  void Clear() {
    store_.Clear();
  }

  template <typename Pred>
  std::vector<Timer*> ExtractPrefix(Pred pred) {
    return store_.ExtractPrefix(pred);
  }

  template <typename Pred, typename OnPop>
  std::size_t ExtractPrefix(Pred pred, OnPop on_pop) {
    return store_.ExtractPrefix(pred, on_pop);
  }

  template <typename Pred>
  std::vector<Timer*> PopWhile(Pred pred) {
    return ExtractPrefix(std::move(pred));
  }

  template <typename Pred, typename OnPop>
  std::size_t PopWhile(Pred pred, OnPop on_pop) {
    return ExtractPrefix(std::move(pred), std::move(on_pop));
  }

private:
  TimerTree store_;
};

}  // namespace alyrn::detail::time
