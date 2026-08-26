// SPDX-License-Identifier: MIT
#pragma once

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <vector>

namespace coropact::ds {

inline constexpr std::size_t kNotInHeap = static_cast<std::size_t>(-1);

// Intrusive 4-ary min-heap. Node storage lives in T through a base hook:
// T must publicly inherit HeapNode<T, Tag>, so the heap can recover T* from a
// node with static_cast.
//
// Each linked element stores its slot in HeapNode::heap_index_, so Erase is
// O(log n) without a side table. The heap vector holds node pointers only;
// element storage stays with the caller.
//
// Template parameters:
//   T     - element type; must publicly inherit HeapNode<T>
//   kLess - total order on T; must be irreflexive and transitive
//   Tag   - optional hook tag for objects that inherit multiple HeapNode hooks
//
// This heap is one of the two timer-index adapters behind time::TimerIndex.
template <class T, auto kLess, class Tag = void>
class IntrusiveQuadHeap;

template <typename T, class Tag = void>
class HeapNode {
  template <class, auto, class>
  friend class IntrusiveQuadHeap;

public:
  [[nodiscard]]
  bool InHeap() const noexcept {
    return linked();
  }

protected:
  HeapNode(const HeapNode&) = delete;
  HeapNode& operator=(const HeapNode&) = delete;
  HeapNode(HeapNode&&) = delete;
  HeapNode& operator=(HeapNode&&) = delete;

  HeapNode() = default;
  ~HeapNode() = default;

private:
  using Node = HeapNode<T, Tag>;

  [[nodiscard]]
  std::size_t heap_index() const noexcept {
    return heap_index_;
  }
  void set_heap_index(std::size_t index) noexcept { heap_index_ = index; }

  [[nodiscard]]
  bool linked() const noexcept {
    return heap_index_ != kNotInHeap;
  }
  void clear_hook() noexcept { heap_index_ = kNotInHeap; }

  std::size_t heap_index_{kNotInHeap};  // slot in heap_, or kNotInHeap
};

template <class T, class Tag = void>
concept HeapNodeBaseHook =
    std::derived_from<T, HeapNode<T, Tag>> && requires(T* elem, HeapNode<T, Tag>* node) {
      { static_cast<HeapNode<T, Tag>*>(elem) } -> std::same_as<HeapNode<T, Tag>*>;
      { static_cast<T*>(node) } -> std::same_as<T*>;
    };

template <class T, auto kLess, class Tag>
class IntrusiveQuadHeap {
public:
  IntrusiveQuadHeap(const IntrusiveQuadHeap&) = delete;
  IntrusiveQuadHeap& operator=(const IntrusiveQuadHeap&) = delete;
  IntrusiveQuadHeap(IntrusiveQuadHeap&&) = delete;
  IntrusiveQuadHeap& operator=(IntrusiveQuadHeap&&) = delete;

  using Node = HeapNode<T, Tag>;
  static_assert(HeapNodeBaseHook<T, Tag>,
                "T must publicly and non-virtually inherit HeapNode<T, Tag>");

  IntrusiveQuadHeap() noexcept = default;
  ~IntrusiveQuadHeap() noexcept { Clear(); }

  // O(1)
  [[nodiscard]]
  bool Empty() const noexcept {
    return heap_.empty();
  }
  [[nodiscard]]
  std::size_t Size() const noexcept {
    return heap_.size();
  }

  // O(log n); returns false if elem is already linked in this heap.
  bool Insert(T* elem);

  // O(log n); returns false if elem was not linked in any heap.
  //
  // Precondition: if elem is linked, it must be linked in this exact heap.
  // Passing an element linked in another IntrusiveQuadHeap is undefined
  // behavior. InHeap() only says whether the element is linked somewhere.
  bool Erase(T* elem);

  // O(1)
  T* Earliest() const { return Empty() ? nullptr : ElemOf(heap_.front()); }

  void Clear() noexcept;

  // O(k log n) where k is the number of extracted elements.
  // Extracts (and erases) the earliest elements satisfying pred in key order.
  // Stops at the first element that fails the predicate.
  template <typename Pred>
  std::vector<T*> PopWhile(Pred pred);

  // O(k log n). Same traversal, but avoids building a result vector.
  // Each element is erased before on_pop(elem) runs.
  template <typename Pred, typename OnPop>
  std::size_t PopWhile(Pred pred, OnPop on_pop);

private:
  static constexpr std::size_t kArity = 4;

  static std::size_t Parent(std::size_t child) { return (child - 1) >> 2; }
  static std::size_t FirstChild(std::size_t parent) { return (parent << 2) | 1; }

  static Node* NodeOf(T* elem) { return static_cast<Node*>(elem); }
  static T* ElemOf(Node* node) { return static_cast<T*>(node); }

  void SwapNodes(std::size_t i, std::size_t j);
  void SiftUp(std::size_t child);
  void SiftDown(std::size_t parent);

  std::vector<Node*> heap_;
};

#define IQH_TMPL template <typename T, auto kLess, class Tag>
#define IQH_TYPE IntrusiveQuadHeap<T, kLess, Tag>

IQH_TMPL
void IQH_TYPE::SwapNodes(std::size_t i, std::size_t j) {
  std::swap(heap_[i], heap_[j]);
  heap_[i]->set_heap_index(i);
  heap_[j]->set_heap_index(j);
}

IQH_TMPL
void IQH_TYPE::SiftUp(std::size_t child) {
  while (child > 0) {
    std::size_t parent = Parent(child);
    if (!kLess(ElemOf(heap_[child]), ElemOf(heap_[parent]))) break;
    SwapNodes(child, parent);
    child = parent;
  }
}
IQH_TMPL
bool IQH_TYPE::Insert(T* elem) {
  auto* node = NodeOf(elem);
  if (node->InHeap()) return false;
  heap_.push_back(node);
  node->set_heap_index(heap_.size() - 1);
  SiftUp(heap_.size() - 1);
  return true;
}

IQH_TMPL
void IQH_TYPE::SiftDown(std::size_t parent) {
  std::size_t size = heap_.size();
  while (true) {
    std::size_t first_child = FirstChild(parent);
    if (first_child >= size) break;
    std::size_t end = std::min(first_child + kArity, size);
    std::size_t smallest = first_child;
    for (std::size_t next_child = first_child + 1; next_child < end; ++next_child) {
      if (kLess(ElemOf(heap_[next_child]), ElemOf(heap_[smallest]))) {
        smallest = next_child;
      }
    }
    if (!kLess(ElemOf(heap_[smallest]), ElemOf(heap_[parent]))) break;

    SwapNodes(parent, smallest);
    parent = smallest;
  }
}

IQH_TMPL
bool IQH_TYPE::Erase(T* elem) {
  auto* node = NodeOf(elem);
  if (!node->InHeap()) return false;
  auto* last = heap_.back();
  std::size_t index = node->heap_index();
  heap_.pop_back();
  node->clear_hook();
  if (index >= heap_.size()) return true;

  heap_[index] = last;
  last->set_heap_index(index);
  if (index > 0 && kLess(ElemOf(heap_[index]), ElemOf(heap_[Parent(index)]))) {
    SiftUp(index);
  } else {
    SiftDown(index);
  }
  return true;
}

IQH_TMPL
void IQH_TYPE::Clear() noexcept {
  for (Node* node : heap_) {
    node->clear_hook();
  }
  heap_.clear();
}

IQH_TMPL
template <typename Pred>
std::vector<T*> IQH_TYPE::PopWhile(Pred pred) {
  std::vector<T*> result;
  while (!Empty()) {
    T* top = Earliest();
    if (!pred(top)) break;
    result.push_back(top);
    Erase(top);
  }
  return result;
}

IQH_TMPL
template <typename Pred, typename OnPop>
std::size_t IQH_TYPE::PopWhile(Pred pred, OnPop on_pop) {
  std::size_t popped = 0;
  while (!Empty()) {
    T* elem = Earliest();
    if (!pred(elem)) break;
    Erase(elem);
    on_pop(elem);
    ++popped;
  }
  return popped;
}

#undef IQH_TYPE
#undef IQH_TMPL

}  // namespace coropact::ds
