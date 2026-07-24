// Copyright (c) 2026 RomenJens, Arsenova.
// SPDX-License-Identifier: MIT
#pragma once

#include <cassert>
#include <concepts>
#include <cstddef>
#include <utility>

#include "coropact/utils/macros.h"

namespace coropact::ds {

template <typename T, class Tag>
class IntrusiveList;

template <typename T, class Tag = void>
class ListNode {
  template <class, class>
  friend class IntrusiveList;

public:
  [[nodiscard]] bool InList() const noexcept { return next_ != nullptr; }

protected:
  COROPACT_DELETE_COPY_MOVE(ListNode);

  ListNode() noexcept = default;
  ListNode(ListNode* prev, ListNode* next) noexcept : prev_(prev), next_(next) {}
  ~ListNode() noexcept = default;

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
  COROPACT_DELETE_COPY(IntrusiveList);

  using Node = ListNode<T, Tag>;
  static_assert(ListNodeBaseHook<T, Tag>,
                "T must publicly and non-virtually inherit ListNode<T, Tag>");

  IntrusiveList() noexcept : head_(&head_, &head_) {}

  IntrusiveList(IntrusiveList&& other) noexcept : IntrusiveList() {
    TakeFrom(other);
  }

  IntrusiveList& operator=(IntrusiveList&& other) noexcept {
    if (this != &other) {
      Clear();
      TakeFrom(other);
    }
    return *this;
  }

  // Resets every linked node's hook so a stale Erase after destruction is safe.
  ~IntrusiveList() noexcept { Clear(); }

  class iterator {
  public:
    using value_type = T;
    using reference = T&;
    using pointer = T*;

    T& operator*() const noexcept { return *elem_of(node_); }
    T* operator->() const noexcept { return elem_of(node_); }
    iterator& operator++() noexcept {
      node_ = Next(node_);
      return *this;
    }
    iterator& operator--() noexcept {
      node_ = Prev(node_);
      return *this;
    }
    bool operator==(const iterator& other) const noexcept { return node_ == other.node_; }
    bool operator!=(const iterator& other) const noexcept { return node_ != other.node_; }

  private:
    friend class IntrusiveList;
    friend class const_iterator;
    explicit iterator(Node* node) noexcept : node_(node) {}
    Node* node_{nullptr};
  };

  class const_iterator {
  public:
    using value_type = T;
    using reference = const T&;
    using pointer = const T*;

    const_iterator() noexcept = default;
    const_iterator(const iterator& it) noexcept : node_(it.node_) {}

    const T& operator*() const noexcept { return *elem_of(node_); }
    const T* operator->() const noexcept { return elem_of(node_); }
    const_iterator& operator++() noexcept {
      node_ = Next(node_);
      return *this;
    }
    const_iterator& operator--() noexcept {
      node_ = Prev(node_);
      return *this;
    }
    bool operator==(const const_iterator& o) const noexcept { return node_ == o.node_; }
    bool operator!=(const const_iterator& o) const noexcept { return node_ != o.node_; }

  private:
    friend class IntrusiveList;
    explicit const_iterator(const Node* node) noexcept : node_(node) {}
    const Node* node_{nullptr};
  };

  iterator begin() noexcept { return iterator(Next(&head_)); }
  iterator end() noexcept { return iterator(&head_); }
  const_iterator begin() const noexcept { return const_iterator(Next(&head_)); }
  const_iterator end() const noexcept { return const_iterator(&head_); }
  const_iterator cbegin() const noexcept { return begin(); }
  const_iterator cend() const noexcept { return end(); }

  static Node* Next(Node* node) noexcept { return node->next_; }
  static const Node* Next(const Node* node) noexcept { return node->next_; }
  static Node* Prev(Node* node) noexcept { return node->prev_; }
  static const Node* Prev(const Node* node) noexcept { return node->prev_; }
  static T* elem_of(Node* node) noexcept { return static_cast<T*>(node); }
  static const T* elem_of(const Node* node) noexcept { return static_cast<const T*>(node); }
  static Node* node_of(T* elem) noexcept { return static_cast<Node*>(elem); }

  [[nodiscard]] bool empty() const noexcept { return head_.next_ == &head_; }
  [[nodiscard]] std::size_t size() const noexcept { return size_; }

  T* front() noexcept;
  const T* front() const noexcept;
  T* back() noexcept;
  const T* back() const noexcept;

  // Insert. Returns false for nullptr or when elem is already linked (in this
  // or any list).
  [[nodiscard]] bool PushFront(T*) noexcept;
  [[nodiscard]] bool PushBack(T*) noexcept;
  // Link elem adjacent to pos. Precondition: pos is linked in *this*; elem is
  // not linked. Returns false for nullptr or when elem is already linked.
  [[nodiscard]] bool InsertBefore(T* pos, T* elem) noexcept;
  [[nodiscard]] bool InsertAfter(T* pos, T* elem) noexcept;

  // Pop. Returns nullptr when empty.
  T* PopFront() noexcept;
  T* PopBack() noexcept;

  // Erase by element. O(1). Returns false if elem was not linked.
  // Precondition: if linked, elem is linked in *this* (cross-list erase is UB,
  // same caveat as IntrusiveRBTree::Erase). InList() only says "linked somewhere".
  [[nodiscard]] bool Erase(T* elem) noexcept;

  // Erase by iterator. O(1). Returns the iterator following the erased element,
  // which stays valid for continued traversal. Precondition: it != end().
  iterator Erase(iterator it) noexcept;
  iterator Erase(const_iterator it) noexcept;

  // Unlink every element (resetting its hook) and reset the sentinel. O(n).
  void Clear() noexcept;

  // Unlinks every element satisfying pred and returns the number removed.
  // pred runs while the element is still linked and must not mutate this list.
  template <typename Pred>
  std::size_t RemoveIf(Pred pred);

  // Reposition an already-linked element to the back of *this* in O(1). Size unchanged.
  // Precondition: elem is linked in *this*.
  void MoveToBack(T*) noexcept;
  void MoveToFront(T*) noexcept;

  // Move all of other's elements to the back of *this* in O(1). other ends empty.
  void Splice(IntrusiveList& other) noexcept;

  // Exchanges the two list rings without moving any element.
  void Swap(IntrusiveList& other) noexcept;

  friend void swap(IntrusiveList& lhs, IntrusiveList& rhs) noexcept { lhs.Swap(rhs); }

  template <typename Fn>
  void ForEachSafe(Fn fn);

private:
  void reset() noexcept {
    head_.next_ = &head_;
    head_.prev_ = &head_;
    size_ = 0;
  }

  void TakeFrom(IntrusiveList& other) noexcept {
    if (other.empty()) return;

    head_.next_ = other.head_.next_;
    head_.prev_ = other.head_.prev_;
    size_ = other.size_;

    head_.next_->prev_ = &head_;
    head_.prev_->next_ = &head_;
    other.reset();
  }
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
T* ILIST_TYPE::front() noexcept { return empty() ? nullptr : elem_of(head_.next_); }

ILIST_TMPL
const T* ILIST_TYPE::front() const noexcept {
  return empty() ? nullptr : elem_of(head_.next_);
}

ILIST_TMPL
T* ILIST_TYPE::back() noexcept { return empty() ? nullptr : elem_of(head_.prev_); }

ILIST_TMPL
const T* ILIST_TYPE::back() const noexcept {
  return empty() ? nullptr : elem_of(head_.prev_);
}

ILIST_TMPL
bool ILIST_TYPE::PushFront(T* elem) noexcept {
  if (elem == nullptr) return false;
  Node* node = node_of(elem);
  if (node->InList()) return false;
  LinkBetween(node, &head_, head_.next_);
  ++size_;
  return true;
}

ILIST_TMPL
bool ILIST_TYPE::PushBack(T* elem) noexcept {
  if (elem == nullptr) return false;
  Node* node = node_of(elem);
  if (node->InList()) return false;
  LinkBetween(node, head_.prev_, &head_);
  ++size_;
  return true;
}

ILIST_TMPL
bool ILIST_TYPE::InsertBefore(T* pos, T* elem) noexcept {
  if (pos == nullptr || elem == nullptr) return false;
  Node* anchor = node_of(pos);
  Node* node = node_of(elem);
  assert(anchor->InList());
  if (!anchor->InList() || node->InList()) return false;
  LinkBetween(node, anchor->prev_, anchor);
  ++size_;
  return true;
}

ILIST_TMPL
bool ILIST_TYPE::InsertAfter(T* pos, T* elem) noexcept {
  if (pos == nullptr || elem == nullptr) return false;
  Node* anchor = node_of(pos);
  Node* node = node_of(elem);
  assert(anchor->InList());
  if (!anchor->InList() || node->InList()) return false;
  LinkBetween(node, anchor, anchor->next_);
  ++size_;
  return true;
}

ILIST_TMPL
T* ILIST_TYPE::PopFront() noexcept {
  if (empty()) return nullptr;
  Node* node = head_.next_;
  Unlink(node);
  return elem_of(node);
}

ILIST_TMPL
T* ILIST_TYPE::PopBack() noexcept {
  if (empty()) return nullptr;
  Node* node = head_.prev_;
  Unlink(node);
  return elem_of(node);
}

ILIST_TMPL
bool ILIST_TYPE::Erase(T* elem) noexcept {
  if (elem == nullptr) return false;
  Node* node = node_of(elem);
  if (!node->InList()) return false;
  Unlink(node);
  return true;
}

ILIST_TMPL
auto ILIST_TYPE::Erase(iterator it) noexcept -> iterator {
  Node* node = it.node_;
  assert(node != &head_);
  Node* next = node->next_;
  Unlink(node);
  return iterator(next);
}

ILIST_TMPL
auto ILIST_TYPE::Erase(const_iterator it) noexcept -> iterator {
  Node* node = const_cast<Node*>(it.node_);
  assert(node != &head_);
  Node* next = node->next_;
  Unlink(node);
  return iterator(next);
}

ILIST_TMPL
void ILIST_TYPE::Clear() noexcept {
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
template <typename Pred>
std::size_t ILIST_TYPE::RemoveIf(Pred pred) {
  std::size_t removed = 0;
  for (Node* cur = head_.next_; cur != &head_;) {
    Node* next = cur->next_;
    if (pred(*elem_of(cur))) {
      Unlink(cur);
      ++removed;
    }
    cur = next;
  }
  return removed;
}

ILIST_TMPL
void ILIST_TYPE::MoveToBack(T* elem) noexcept {
  Node* node = node_of(elem);
  assert(node->InList());
  SpliceOut(node);
  LinkBetween(node, head_.prev_, &head_);
}

ILIST_TMPL
void ILIST_TYPE::MoveToFront(T* elem) noexcept {
  Node* node = node_of(elem);
  assert(node->InList());
  SpliceOut(node);
  LinkBetween(node, &head_, head_.next_);
}

ILIST_TMPL
void ILIST_TYPE::Splice(IntrusiveList& other) noexcept {
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

  other.reset();
}

ILIST_TMPL
void ILIST_TYPE::Swap(IntrusiveList& other) noexcept {
  if (this == &other) return;

  using std::swap;
  swap(head_.next_, other.head_.next_);
  swap(head_.prev_, other.head_.prev_);
  swap(size_, other.size_);

  if (size_ == 0) {
    reset();
  } else {
    head_.next_->prev_ = &head_;
    head_.prev_->next_ = &head_;
  }

  if (other.size_ == 0) {
    other.reset();
  } else {
    other.head_.next_->prev_ = &other.head_;
    other.head_.prev_->next_ = &other.head_;
  }
}

ILIST_TMPL
template <typename Fn>
void ILIST_TYPE::ForEachSafe(Fn fn) {
  (void)RemoveIf(std::move(fn));
}

#undef ILIST_TMPL
#undef ILIST_TYPE

}  // namespace coropact::ds
