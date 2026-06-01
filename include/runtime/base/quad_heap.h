// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <algorithm>
#include <cstddef>
#include <vector>

#include "runtime/base/noncopyable.h"

namespace runtime::base {

static constexpr std::size_t kNotInHeap = static_cast<std::size_t>(-1);

template <typename T>
struct HeapNode {
  T* owner{nullptr};
  std::size_t heap_index{kNotInHeap};
  const void* heap{nullptr};
};

template <typename T,
          HeapNode<T> T::*kMember,           // Member pointer of Intrusive Node
          bool (*kLess)(const T*, const T*)  // Comparator
          >
class IntrusiveQuadHeap : public NonCopyable {
public:
  IntrusiveQuadHeap() = default;
  ~IntrusiveQuadHeap() = default;

  // O(1)
  bool empty() const { return heap_.empty(); }
  std::size_t size() const { return heap_.size(); }

  // O(log n) amortized; no-op if elem is already in heap.
  void Insert(T* elem);

  // O(logn) amortized; return false if elem was not in heap.
  bool Erase(T* elem);

  // O(1)
  T* earliest() const { return empty() ? nullptr : ElemOf(heap_[0]); }

  // O(k log n) where k is the number of extracted elements.
  // Extracts and erases the earliest elements satisfying pred in key order.
  // Stops at the first element that fails the predicate.
  template <typename Pred>
  std::vector<T*> PopWhile(Pred pred);

private:
  inline static std::size_t Parent(std::size_t child) { return (child - 1) >> 2; }
  inline static std::size_t FirstChild(std::size_t parent) { return parent << 2 | 1; }
  static HeapNode<T>* NodeOf(T* elem) { return &(elem->*kMember); }
  static T* ElemOf(HeapNode<T>* node) { return node->owner; }

  void SwapNodes(std::size_t i, std::size_t j);
  void SiftUp(std::size_t index);
  void SiftDown(std::size_t index);

  std::vector<HeapNode<T>*> heap_;
};

#define IQAH_TMPL template <typename T, HeapNode<T> T::*kMember, bool (*kLess)(const T*, const T*)>
#define IQAH_TYPE IntrusiveQuadHeap<T, kMember, kLess>

IQAH_TMPL
void IQAH_TYPE::SwapNodes(std::size_t i, std::size_t j) {
  std::swap(heap_[i], heap_[j]);
  heap_[i]->heap_index = i;
  heap_[j]->heap_index = j;
}

IQAH_TMPL
void IQAH_TYPE::SiftUp(std::size_t child) {
  while (child > 0) {
    std::size_t parent = Parent(child);
    if (!kLess(ElemOf(heap_[child]), ElemOf(heap_[parent]))) break;
    SwapNodes(child, parent);
    child = parent;
  }
}
IQAH_TMPL
void IQAH_TYPE::Insert(T* elem) {
  auto* node = NodeOf(elem);
  if (node->heap != nullptr) return;
  node->owner = elem;
  node->heap = this;
  heap_.push_back(node);
  node->heap_index = heap_.size() - 1;
  SiftUp(heap_.size() - 1);
}

IQAH_TMPL
void IQAH_TYPE::SiftDown(std::size_t parent) {
  std::size_t sz = heap_.size();
  while (true) {
    std::size_t first_child = FirstChild(parent);
    if (first_child >= sz) break;
    std::size_t end = std::min(first_child + 4, sz);
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

IQAH_TMPL
bool IQAH_TYPE::Erase(T* elem) {
  auto* node = NodeOf(elem);
  if (node->heap != this) return false;
  auto* last = heap_.back();
  heap_.pop_back();
  std::size_t index = node->heap_index;
  node->heap_index = kNotInHeap;
  node->heap = nullptr;
  if (index < heap_.size()) {
    heap_[index] = last;
    last->heap_index = index;
    if (index > 0 && kLess(ElemOf(heap_[index]), ElemOf(heap_[Parent(index)]))) {
      SiftUp(index);
    } else {
      SiftDown(index);
    }
  }
  return true;
}

IQAH_TMPL
template <typename Pred>
std::vector<T*> IQAH_TYPE::PopWhile(Pred pred) {
  std::vector<T*> result;
  while (!empty()) {
    T* top = earliest();
    if (!pred(top)) break;
    result.push_back(top);
    Erase(top);
  }
  return result;
}

#undef IQAH_TYPE
#undef IQAH_TMPL
}  // namespace runtime::base
