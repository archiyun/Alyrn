// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>
#include <utility>

namespace coropact::ds {

template <typename T, class Tag>
class IntrusiveList;

template <typename T, class Tag = void>
class ListNode {
  template <class, class>
  friend class IntrusiveList;

public:
  ListNode(const ListNode&) = delete;
  ListNode& operator=(const ListNode&) = delete;
  ListNode(ListNode&&) = delete;
  ListNode& operator=(ListNode&&) = delete;

  [[nodiscard]]
  bool InList() const noexcept {
    return next_ != nullptr;
  }

protected:
  ListNode() noexcept = default;
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
  IntrusiveList(const IntrusiveList&) = delete;
  IntrusiveList& operator=(const IntrusiveList&) = delete;

  using Node = ListNode<T, Tag>;
  static_assert(ListNodeBaseHook<T, Tag>,
                "T must publicly and non-virtually inherit ListNode<T, Tag>");

  IntrusiveList() noexcept { Reset(); }

  IntrusiveList(IntrusiveList&& other) noexcept : IntrusiveList() { TakeFrom(other); }

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

    T& operator*() const noexcept { return *ElemOf(node_); }
    T* operator->() const noexcept { return ElemOf(node_); }
    iterator& operator++() noexcept {
      node_ = Next(node_);
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

    const T& operator*() const noexcept { return *ElemOf(node_); }
    const T* operator->() const noexcept { return ElemOf(node_); }
    const_iterator& operator++() noexcept {
      node_ = Next(node_);
      return *this;
    }
    bool operator==(const const_iterator& o) const noexcept { return node_ == o.node_; }
    bool operator!=(const const_iterator& o) const noexcept { return node_ != o.node_; }

  private:
    friend class IntrusiveList;
    explicit const_iterator(const Node* node) noexcept : node_(node) {}
    const Node* node_{nullptr};
  };

  iterator Begin() noexcept { return iterator(Next(&head_)); }
  iterator End() noexcept { return iterator(&head_); }
  const_iterator Begin() const noexcept { return const_iterator(Next(&head_)); }
  const_iterator End() const noexcept { return const_iterator(&head_); }

  [[nodiscard]]
  bool Empty() const noexcept {
    return head_.next_ == &head_;
  }
  T* Front() noexcept;
  const T* Front() const noexcept;
  T* Back() noexcept;
  const T* Back() const noexcept;

  // Insert at either end. Returns false for nullptr or when elem is already
  // linked (in this or any list).
  [[nodiscard]]
  bool PushFront(T*) noexcept;
  [[nodiscard]]
  bool PushBack(T*) noexcept;
  // Pop. Returns nullptr when empty.
  T* PopFront() noexcept;

  // Erase by element. O(1). Returns false if elem was not linked.
  // Precondition: if linked, elem is linked in *this* (cross-list erase is UB,
  // same caveat as IntrusiveTree::Erase). InList() only says "linked somewhere".
  [[nodiscard]]
  bool Erase(T* elem) noexcept;

  // Unlink every element (resetting its hook) and reset the sentinel. O(n).
  void Clear() noexcept;

  // Move all of other's elements to the back of *this* in O(1). other ends empty.
  // Self-splice is a no-op.
  void Splice(IntrusiveList& other) noexcept;

  template <typename Fn>
  void ForEachSafe(Fn fn);

private:
  // Unlinks every element satisfying pred; used by the safe traversal helper.
  template <typename Pred>
  void RemoveIf(Pred pred);

  static Node* Next(Node* node) noexcept { return node->next_; }
  static const Node* Next(const Node* node) noexcept { return node->next_; }
  static T* ElemOf(Node* node) noexcept { return static_cast<T*>(node); }
  static const T* ElemOf(const Node* node) noexcept { return static_cast<const T*>(node); }
  static Node* NodeOf(T* elem) noexcept { return static_cast<Node*>(elem); }

  void Reset() noexcept {
    head_.next_ = &head_;
    head_.prev_ = &head_;
  }

  void TakeFrom(IntrusiveList& other) noexcept {
    if (other.Empty()) return;

    head_.next_ = other.head_.next_;
    head_.prev_ = other.head_.prev_;

    head_.next_->prev_ = &head_;
    head_.prev_->next_ = &head_;
    other.Reset();
  }
  // -- Link primitives --
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
  }

  Node head_{};  // sentinel; head_.next_ = first, head_.prev_ = last
};

// Range-for customization points. The container API itself uses Begin/End;
// these lowercase overloads remain only for the language range protocol.
template <class T, class Tag>
auto begin(IntrusiveList<T, Tag>& list) noexcept {
  return list.Begin();
}

template <class T, class Tag>
auto end(IntrusiveList<T, Tag>& list) noexcept {
  return list.End();
}

template <class T, class Tag>
auto begin(const IntrusiveList<T, Tag>& list) noexcept {
  return list.Begin();
}

template <class T, class Tag>
auto end(const IntrusiveList<T, Tag>& list) noexcept {
  return list.End();
}

#define ILIST_TMPL template <class T, class Tag>
#define ILIST_TYPE IntrusiveList<T, Tag>

ILIST_TMPL
T* ILIST_TYPE::Front() noexcept { return Empty() ? nullptr : ElemOf(head_.next_); }

ILIST_TMPL
const T* ILIST_TYPE::Front() const noexcept { return Empty() ? nullptr : ElemOf(head_.next_); }

ILIST_TMPL
T* ILIST_TYPE::Back() noexcept { return Empty() ? nullptr : ElemOf(head_.prev_); }

ILIST_TMPL
const T* ILIST_TYPE::Back() const noexcept { return Empty() ? nullptr : ElemOf(head_.prev_); }

ILIST_TMPL
bool ILIST_TYPE::PushFront(T* elem) noexcept {
  if (elem == nullptr) return false;
  Node* node = NodeOf(elem);
  if (node->InList()) return false;
  LinkBetween(node, &head_, head_.next_);
  return true;
}

ILIST_TMPL
bool ILIST_TYPE::PushBack(T* elem) noexcept {
  if (elem == nullptr) return false;
  Node* node = NodeOf(elem);
  if (node->InList()) return false;
  LinkBetween(node, head_.prev_, &head_);
  return true;
}

ILIST_TMPL
T* ILIST_TYPE::PopFront() noexcept {
  if (Empty()) return nullptr;
  Node* node = head_.next_;
  Unlink(node);
  return ElemOf(node);
}

ILIST_TMPL
bool ILIST_TYPE::Erase(T* elem) noexcept {
  if (elem == nullptr) return false;
  Node* node = NodeOf(elem);
  if (!node->InList()) return false;
  Unlink(node);
  return true;
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
}

ILIST_TMPL
template <typename Pred>
void ILIST_TYPE::RemoveIf(Pred pred) {
  for (Node* cur = head_.next_; cur != &head_;) {
    Node* next = cur->next_;
    if (pred(*ElemOf(cur))) {
      Unlink(cur);
    }
    cur = next;
  }
}

ILIST_TMPL
void ILIST_TYPE::Splice(IntrusiveList& other) noexcept {
  if (&other == this) return;
  if (other.Empty()) return;
  Node* first = other.head_.next_;
  Node* last = other.head_.prev_;
  Node* tail = head_.prev_;

  tail->next_ = first;
  first->prev_ = tail;
  last->next_ = &head_;
  head_.prev_ = last;

  other.Reset();
}

ILIST_TMPL
template <typename Fn>
void ILIST_TYPE::ForEachSafe(Fn fn) {
  (void)RemoveIf(std::move(fn));
}

#undef ILIST_TMPL
#undef ILIST_TYPE

}  // namespace coropact::ds
