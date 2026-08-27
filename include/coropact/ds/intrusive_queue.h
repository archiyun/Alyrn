// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>

namespace coropact::ds {

template <typename T, class Tag>
class IntrusiveQueue;

template <typename T, class Tag = void>
class QueueNode {
  template <class, class>
  friend class IntrusiveQueue;

public:
  QueueNode(const QueueNode&) = delete;
  QueueNode& operator=(const QueueNode&) = delete;
  QueueNode(QueueNode&&) = delete;
  QueueNode& operator=(QueueNode&&) = delete;

  [[nodiscard]]
  bool InQueue() const noexcept {
    return next_ != nullptr;
  }

protected:
  QueueNode() = default;
  ~QueueNode() = default;

private:
  using Node = QueueNode<T, Tag>;
  Node* next_{nullptr};

  void clear_hook() noexcept { next_ = nullptr; }
};

template <class T, class Tag = void>
concept QueueNodeBaseHook =
    std::derived_from<T, QueueNode<T, Tag>> && requires(T* elem, QueueNode<T, Tag>* node) {
      { static_cast<QueueNode<T, Tag>*>(elem) } -> std::same_as<QueueNode<T, Tag>*>;
      { static_cast<T*>(node) } -> std::same_as<T*>;
    };
template <class T, class Tag = void>
class IntrusiveQueue {
public:
  IntrusiveQueue(const IntrusiveQueue&) = delete;
  IntrusiveQueue& operator=(const IntrusiveQueue&) = delete;

  static_assert(QueueNodeBaseHook<T, Tag>,
                "T must publicly and non-virtually inherit QueueNode<T, Tag>");

  IntrusiveQueue() noexcept { Reset(); }

  IntrusiveQueue(IntrusiveQueue&& other) noexcept {
    Reset();
    TakeFrom(other);
  }

  IntrusiveQueue& operator=(IntrusiveQueue&& other) noexcept {
    if (this != &other) {
      Clear();
      TakeFrom(other);
    }
    return *this;
  }

  ~IntrusiveQueue() { Clear(); }

  [[nodiscard]]
  bool Empty() const noexcept {
    return head_.next_ == &head_;
  }
  T* Front() const noexcept { return Empty() ? nullptr : ElemOf(head_.next_); }

  // Returns false for nullptr or when elem is already queued.
  [[maybe_unused]] bool PushBack(T* elem) noexcept {
    if (elem == nullptr) return false;
    Node* node = NodeOf(elem);
    if (node->InQueue()) return false;

    tail_->next_ = node;
    node->next_ = &head_;
    tail_ = node;
    return true;
  }

  T* PopFront() noexcept {
    Node* node = head_.next_;
    if (node == &head_) return nullptr;

    Node* next = node->next_;
    head_.next_ = next;
    node->clear_hook();
    if (next == &head_) {
      tail_ = &head_;
    }
    return ElemOf(node);
  }

  void Clear() noexcept {
    for (Node* cur = head_.next_; cur != &head_;) {
      Node* next = cur->next_;
      cur->clear_hook();
      cur = next;
    }
    Reset();
  }

  // Moves all nodes from other to this queue. Self-splice is a no-op.
  void Splice(IntrusiveQueue& other) noexcept {
    if (&other == this) return;
    if (other.Empty()) return;
    Node* first = other.head_.next_;
    Node* last = other.tail_;
    tail_->next_ = first;
    last->next_ = &head_;
    tail_ = last;

    other.Reset();
  }

  template <typename Fn>
  void ForEachSafe(Fn fn) {
    Node* prev = &head_;
    for (Node* cur = head_.next_; cur != &head_;) {
      Node* next = cur->next_;
      if (fn(*ElemOf(cur))) {
        prev->next_ = next;
        if (tail_ == cur) {
          tail_ = prev;
        }
        cur->clear_hook();
      } else {
        prev = cur;
      }
      cur = next;
    }
  }

private:
  using Node = QueueNode<T, Tag>;
  static T* ElemOf(Node* node) { return static_cast<T*>(node); }
  static Node* NodeOf(T* elem) { return static_cast<Node*>(elem); }

  void Reset() noexcept {
    head_.next_ = &head_;
    tail_ = &head_;
  }

  void TakeFrom(IntrusiveQueue& other) noexcept {
    if (other.Empty()) return;

    head_.next_ = other.head_.next_;
    tail_ = other.tail_;
    tail_->next_ = &head_;
    other.Reset();
  }

  Node head_{};
  Node* tail_{&head_};
};

}  // namespace coropact::ds
