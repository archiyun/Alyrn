// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <algorithm>
#include <cstddef>
#include <vector>
#include <concepts>

#include "runtime/base/noncopyable.h"

namespace runtime::ds {

inline constexpr std::size_t kNotInHeap = static_cast<std::size_t>(-1);

template <class T, auto kLess, class Tag = void>
class IntrusiveQuadHeap;

template <typename T, class Tag = void>
class HeapNode {
  template <class, auto, class>
  friend class IntrusiveQuadHeap;

public:
  bool InHeap() const noexcept { return linked(); }

protected:
  HeapNode() = default;
  ~HeapNode() = default;
  HeapNode(const HeapNode&) = delete;
  HeapNode& operator=(const HeapNode&) = delete;

private:
  using Node = HeapNode<T, Tag>;

  std::size_t heap_index() const noexcept { return heap_index_; }
  void set_heap_index(std::size_t index) noexcept { heap_index_ = index; }

  bool linked() const noexcept { return heap_index_ != kNotInHeap; }
  void clear_hook() noexcept { heap_index_ = kNotInHeap; }

  std::size_t heap_index_{kNotInHeap};  // slot in heap_, or kNotInHeap
};

template <class T, class Tag = void>
concept HeapNodeBaseHook =
    std::derived_from<T, HeapNode<T, Tag>> &&
    requires(T* elem, HeapNode<T, Tag>* node) {
      {
        static_cast<HeapNode<T, Tag>*>(elem)
      } -> std::same_as<HeapNode<T, Tag>*>;
      { static_cast<T*>(node) } -> std::same_as<T*>;
    };

template <class T, auto kLess, class Tag>
class IntrusiveQuadHeap : public runtime::base::NonCopyable {
public:
  using Node = HeapNode<T, Tag>;
  static_assert(HeapNodeBaseHook<T, Tag>,
                "T must publicly and non-virtually inherit HeapNode<T, Tag>");

  IntrusiveQuadHeap() = default;
  ~IntrusiveQuadHeap() = default;

  // O(1)
  bool empty() const noexcept { return heap_.empty(); }
  std::size_t size() const noexcept { return heap_.size(); }

  // O(log n); returns false if elem is already linked in this heap.
  bool Insert(T* elem);

  // O(log n); returns false if elem was not linked in any heap.
  //
  // Precondition: if elem is linked, it must be linked in this exact heap.
  // Passing an element linked in another IntrusiveQuadHeap is undefined
  // behavior. InHeap() only says whether the element is linked somewhere.
  bool Erase(T* elem);

  // O(1)
  T* earliest() const { return empty() ? nullptr : elem_of(heap_.front()); }

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

  static Node* node_of(T* elem) { return static_cast<Node*>(elem); }
  static T* elem_of(Node* node) { return static_cast<T*>(node); }

  void SwapNodes(std::size_t i, std::size_t j);
  void SiftUp(std::size_t index);
  void SiftDown(std::size_t index);

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
    if (!kLess(elem_of(heap_[child]), elem_of(heap_[parent]))) break;
    SwapNodes(child, parent);
    child = parent;
  }
}
IQH_TMPL
bool IQH_TYPE::Insert(T* elem) {
  auto* node = node_of(elem);
  if (node->InHeap()) return false;
  heap_.push_back(node);
  node->set_heap_index(heap_.size() - 1);
  SiftUp(heap_.size() - 1);
  return true;
}

IQH_TMPL
void IQH_TYPE::SiftDown(std::size_t parent) {
  std::size_t sz = heap_.size();
  while (true) {
    std::size_t first_child = FirstChild(parent);
    if (first_child >= sz) break;
    std::size_t end = std::min(first_child + kArity, sz);
    std::size_t smallest = first_child;
    for (std::size_t next_child = first_child + 1; next_child < end; ++next_child) {
      if (kLess(elem_of(heap_[next_child]), elem_of(heap_[smallest]))) {
        smallest = next_child;
      }
    }
    if (!kLess(elem_of(heap_[smallest]), elem_of(heap_[parent]))) break;

    SwapNodes(parent, smallest);
    parent = smallest;
  }
}

IQH_TMPL
bool IQH_TYPE::Erase(T* elem) {
  auto* node = node_of(elem);
  if (!node->InHeap()) return false;
  auto* last = heap_.back();

  heap_.pop_back();
  std::size_t index = node->heap_index();

  node->clear_hook();
  if (index >= heap_.size()) return true;

  heap_[index] = last;
  last->set_heap_index(index);
  if (index > 0 && kLess(elem_of(heap_[index]), elem_of(heap_[Parent(index)]))) {
    SiftUp(index);
  } else {
    SiftDown(index);
  }
  return true;
}

IQH_TMPL
template <typename Pred>
std::vector<T*> IQH_TYPE::PopWhile(Pred pred) {
  std::vector<T*> result;
  while (!empty()) {
    T* top = earliest();
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
  while (!empty()) {
    T* elem = earliest();
    if (!pred(elem)) break;
    Erase(elem);
    on_pop(elem);
    ++popped;
  }
  return popped;
}

#undef IQH_TYPE
#undef IQH_TMPL

}  // namespace runtime::ds
