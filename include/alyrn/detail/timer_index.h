// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <utility>
#include <vector>

#include "alyrn/detail/intrusive_rbtree.h"
#include "alyrn/detail/macros.h"
#include "alyrn/detail/timer.h"

namespace alyrn::detail {

// Selects the ordered store used by a backend TimerQueue. This is an
// implementation policy, not an application-facing scheduling abstraction.
enum class TimerIndexKind { kRbTree };

inline bool TimerLess(const Timer* a, const Timer* b) {
  if (a->expiration() < b->expiration()) {
    return true;
  }
  if (a->expiration() > b->expiration()) {
    return false;
  }
  return a->sequence() < b->sequence();
}

// rbtree adapter inside TimerIndex. Applications do not construct or insert
// into it; Loop::RunAfter owns that path.
using TimerTree = ::alyrn::detail::IntrusiveTree<Timer, TimerLess>;

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

}  // namespace alyrn::detail
