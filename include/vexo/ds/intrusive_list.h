// Copyright (c) 2026 RomenJens. All Rights Reserved.
// SPDX-License-Identifier: MIT
#pragma once

#include <cassert>
#include <concepts>
#include <cstddef>

#include "vexo/utils/macros.h"

namespace vexo::ds {

template <typename T, class Tag>
class IntrusiveList;

template <typename T, class Tag = void>
class ListNode {
  template <class, class>
  friend class IntrusiveList;

public:
  bool InList() const noexcept { return next_ != nullptr; }

protected:
  ListNode() = default;
  VEXO_DELETE_COPY(ListNode);

private:
  using Node = ListNode<T, Tag>;
  Node* prev_{nullptr};
  Node* next_{nullptr};

  void clear_hook() noexcept {
    prev_ = nullptr;
    next_ = nullptr;
  }
};

template <class T, class Tag = void>
concept ListNodeBaseHook =
    std::derived_from<T, ListNode<T, Tag>> && requires(T* elem, ListNode<T, Tag>* node) {
      { static_cast<ListNode<T, Tag>*>(elem) } -> std::same_as<ListNode<T, Tag>*>;
      { static_cast<T*>(node) } -> std::same_as<T*>;
    };

template <class T, class Tag = void>
class IntrusiveList {
public:
  VEXO_DELETE_COPY_MOVE(IntrusiveList);

  using Node = ListNode<T, Tag>;
  static_assert(ListNodeBaseHook<T, Tag>,
                "T must publicly and non-virutally inherit ListNode<T, Tag>");
  IntrusiveList() {
    head_.next_ = &head_;
    head_.prev_ = &head_;
  }
  // Resets every linked node's hook so a stale Erase after destruction is safe.
  ~IntrusiveList() { Clear(); }

  class iterator {
  public:
    using value_type = T;
    T& operator*() const { return *elem_of(node_); }
    T* operator->() const { return elem_of(node_); }
    iterator& operator++() {
      node_ = Next(node_);
      return *this;
    }
    iterator& operator--() {
      node_ = Prev(node_);
      return *this;
    }
    bool operator==(const iterator& o) const { return node_ == o.node_; }
    bool operator!=(const iterator& o) const { return node_ != o.node_; }

  private:
    friend class IntrusiveList;
    explicit iterator(Node* node) : node_(node) {}
    Node* node_;
  };

  iterator begin() { return iterator(Next(&head_)); }
  iterator end() { return iterator(&head_); }

  static Node* Next(Node* node) { return node->next_; }
  static Node* Prev(Node* node) { return node->prev_; }
  static T* elem_of(Node* node) { return static_cast<T*>(node); }
  static Node* node_of(T* elem) { return static_cast<Node*>(elem); }

  bool empty() const { return head_.next_ == &head_; }
  std::size_t size() const { return size_; }

  T* front() const;
  T* back() const;

  // Insert. Idempotent: no-op if elem is already linked (in this or any list).
  void PushFront(T*);
  void PushBack(T*);
  // Link elem adjacent to pos. Precondition: pos is linked in *this*; elem is
  // not linked (no-op if it already is).
  void InsertBefore(T* pos, T* elem);
  void InsertAfter(T* pos, T* elem);

  // Pop. Returns nullptr when empty.
  T* PopFront();
  T* PopBack();

  // Erase by element. O(1). Returns false if elem was not linked.
  // Precondition: if linked, elem is linked in *this* (cross-list erase is UB,
  // same caveat as IntrusiveRBTree::Erase). InList() only says "linked somewhere".
  bool Erase(T* elem);

  // Erase by iterator. O(1). Returns the iterator following the erased element,
  // which stays valid for continued traversal. Precondition: it != end().
  iterator Erase(iterator it);

  // Unlink every element (resetting its hook) and reset the sentinel. O(n).
  void Clear();

  // Reposition an already-linked element to the back of *this* in O(1). Size unchanged.
  // Precondition: elem is linked in *this*.
  void MoveToBack(T*);
  void MoveToFront(T*);

  // Move all of other's elements to the back of *this* in O(1). other ends empty.
  void Splice(IntrusiveList& other);

  template <typename Fn>
  void ForEachSafe(Fn fn);

private:
  // -- Link primitives (the shared base for every modifier) --
  // Splice node out of the ring without touching its hook or size_ (for MoveTo).
  static void SpliceOut(Node* node) {
    node->prev_->next_ = node->next_;
    node->next_->prev_ = node->prev_;
  }
  // Insert node between prev and next.
  static void LinkBetween(Node* node, Node* prev, Node* next) {
    node->prev_ = prev;
    node->next_ = next;
    prev->next_ = node;
    next->prev_ = node;
  }

  void Unlink(Node* node) {
    SpliceOut(node);
    node->clear_hook();
    --size_;
  }

  Node head_{};  // sentinel; head_.next_ = first, head_.prev_ = last
  std::size_t size_{0};
};

#define ILIST_TMPL template <class T, class Tag>
#define ILIST_TYPE IntrusiveList<T, Tag>

ILIST_TMPL
T* ILIST_TYPE::front() const { return empty() ? nullptr : elem_of(head_.next_); }

ILIST_TMPL
T* ILIST_TYPE::back() const { return empty() ? nullptr : elem_of(head_.prev_); }

ILIST_TMPL
void ILIST_TYPE::PushFront(T* elem) {
  Node* node = node_of(elem);
  if (node->InList()) return;
  LinkBetween(node, &head_, head_.next_);
  ++size_;
}

ILIST_TMPL
void ILIST_TYPE::PushBack(T* elem) {
  Node* node = node_of(elem);
  if (node->InList()) return;
  LinkBetween(node, head_.prev_, &head_);
  ++size_;
}

ILIST_TMPL
void ILIST_TYPE::InsertBefore(T* pos, T* elem) {
  Node* anchor = node_of(pos);
  Node* node = node_of(elem);
  assert(anchor->InList());
  if (node->InList()) return;
  LinkBetween(node, anchor->prev_, anchor);
  ++size_;
}

ILIST_TMPL
void ILIST_TYPE::InsertAfter(T* pos, T* elem) {
  Node* anchor = node_of(pos);
  Node* node = node_of(elem);
  assert(anchor->InList());
  if (node->InList()) return;
  LinkBetween(node, anchor, anchor->next_);
  ++size_;
}

ILIST_TMPL
T* ILIST_TYPE::PopFront() {
  if (empty()) return nullptr;
  Node* node = head_.next_;
  Unlink(node);
  return elem_of(node);
}

ILIST_TMPL
T* ILIST_TYPE::PopBack() {
  if (empty()) return nullptr;
  Node* node = head_.prev_;
  Unlink(node);
  return elem_of(node);
}

ILIST_TMPL
bool ILIST_TYPE::Erase(T* elem) {
  Node* node = node_of(elem);
  if (!node->InList()) return false;
  Unlink(node);
  return true;
}

ILIST_TMPL
auto ILIST_TYPE::Erase(iterator it) -> iterator {
  Node* node = it.node_;
  assert(node != &head_);
  Node* next = node->next_;
  Unlink(node);
  return iterator(next);
}

ILIST_TMPL
void ILIST_TYPE::Clear() {
  for (Node* cur = head_.next_; cur != &head_;) {
    Node* next = cur->next_;
    cur->clear_hook();
    cur = next;
  }
  head_.next_ = &head_;
  head_.prev_ = &head_;
  size_ = 0;
}

ILIST_TMPL
void ILIST_TYPE::MoveToBack(T* elem) {
  Node* node = node_of(elem);
  assert(node->InList());
  SpliceOut(node);
  LinkBetween(node, head_.prev_, &head_);
}

ILIST_TMPL
void ILIST_TYPE::MoveToFront(T* elem) {
  Node* node = node_of(elem);
  assert(node->InList());
  SpliceOut(node);
  LinkBetween(node, &head_, head_.next_);
}

ILIST_TMPL
void ILIST_TYPE::Splice(IntrusiveList& other) {
  assert(&other != this);
  if (other.empty()) return;
  Node* first = other.head_.next_;
  Node* last = other.head_.prev_;
  Node* tail = head_.prev_;

  tail->next_ = first;
  first->prev_ = tail;
  last->next_ = &head_;
  head_.prev_ = last;
  size_ += other.size_;

  other.head_.next_ = &other.head_;
  other.head_.prev_ = &other.head_;
  other.size_ = 0;
}

ILIST_TMPL
template <typename Fn>
void ILIST_TYPE::ForEachSafe(Fn fn) {
  for (Node* cur = head_.next_; cur != &head_;) {
    Node* next = cur->next_;  // capture before a possible Unlink
    if (fn(*elem_of(cur))) Unlink(cur);
    cur = next;
  }
}

#undef ILIST_TMPL
#undef ILIST_TYPE

}  // namespace vexo::ds
