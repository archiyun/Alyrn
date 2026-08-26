// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>
#include <cstddef>
#include <utility>
#include <variant>
#include <vector>

#include "coropact/ds/intrusive_quadheap.h"
#include "coropact/time/timer_tree.h"
#include "coropact/utils/macros.h"

namespace coropact::time {

enum class TimerIndexKind { kRbTree, kQuadHeap };

using TimerHeap = ds::IntrusiveQuadHeap<Timer, TimerLess>;

template <class Index>
concept TimerStore = requires(Index& index, const Index& cindex, Timer* timer) {
  { cindex.Empty() } -> std::convertible_to<bool>;
  { cindex.Size() } -> std::convertible_to<std::size_t>;
  { index.Insert(timer) } -> std::convertible_to<bool>;
  { index.Erase(timer) } -> std::convertible_to<bool>;
  { cindex.Earliest() } -> std::same_as<Timer*>;
  index.Clear();
};

static_assert(TimerStore<TimerTree>);
static_assert(TimerStore<TimerHeap>);

// Ordered timer index injected into backend TimerQueues. The two adapters are
// IntrusiveRBTree (default) and IntrusiveQuadHeap. The chosen store is fixed
// at construction; the containers themselves are not moved.
class TimerIndex {
public:
  COROPACT_DELETE_COPY_MOVE(TimerIndex);

  explicit TimerIndex(TimerIndexKind kind = TimerIndexKind::kRbTree) noexcept
      : store_(MakeStore(kind)) {}

  [[nodiscard]]
  TimerIndexKind Kind() const noexcept {
    return store_.index() == 1 ? TimerIndexKind::kQuadHeap : TimerIndexKind::kRbTree;
  }

  [[nodiscard]]
  bool Empty() const {
    return std::visit([](const auto& store) { return store.Empty(); }, store_);
  }

  [[nodiscard]]
  std::size_t Size() const {
    return std::visit([](const auto& store) { return store.Size(); }, store_);
  }

  bool Insert(Timer* timer) {
    return std::visit([timer](auto& store) { return store.Insert(timer); }, store_);
  }

  bool Erase(Timer* timer) {
    return std::visit([timer](auto& store) { return store.Erase(timer); }, store_);
  }

  [[nodiscard]]
  Timer* Earliest() const {
    return std::visit([](const auto& store) { return store.Earliest(); }, store_);
  }

  void Clear() {
    std::visit([](auto& store) { store.Clear(); }, store_);
  }

  template <typename Pred>
  std::vector<Timer*> PopWhile(Pred pred) {
    return std::visit([&](auto& store) { return store.PopWhile(pred); }, store_);
  }

  template <typename Pred, typename OnPop>
  std::size_t PopWhile(Pred pred, OnPop on_pop) {
    return std::visit([&](auto& store) { return store.PopWhile(pred, on_pop); }, store_);
  }

private:
  using Store = std::variant<TimerTree, TimerHeap>;

  static Store MakeStore(TimerIndexKind kind) noexcept {
    if (kind == TimerIndexKind::kQuadHeap) {
      return Store(std::in_place_type<TimerHeap>);
    }
    return Store(std::in_place_type<TimerTree>);
  }

  Store store_;
};

}  // namespace coropact::time
