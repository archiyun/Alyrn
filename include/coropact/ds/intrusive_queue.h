// Copyright (c) 2026 Arsenova.
// SPDX-License-Identifier: MIT
#pragma once

#include <cassert>
#include <concepts>
#include <cstddef>

#include "coropact/utils/macros.h"

namespace coropact::ds {

template <typename T, class Tag>
class IntrusiveQueue;

template <typename T, class Tag = void>
class QueueNode {
  template <class, class>
  friend class IntrusiveQueue;

public:
  [[nodiscard]] bool InQueue() const noexcept { return next_ != nullptr; }

protected:
  QueueNode() = default;
  COROPACT_DELETE_COPY(QueueNode);

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
  COROPACT_DELETE_COPY(IntrusiveQueue);

  using Node = QueueNode<T, Tag>;
  static_assert(QueueNodeBaseHook<T, Tag>,
                "T must publicly and non-virtually inherit QueueNode<T, Tag>");

  IntrusiveQueue() noexcept { reset(); }

  IntrusiveQueue(IntrusiveQueue&& other) noexcept {
    reset();
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

  static Node* Next(Node* node) { return node->next_; }
  static T* elem_of(Node* node) { return static_cast<T*>(node); }
  static Node* node_of(T* elem) { return static_cast<Node*>(elem); }

  [[nodiscard]] bool empty() const { return head_.next_ == &head_; }
  [[nodiscard]] std::size_t size() const { return size_; }

  T* front() const { return empty() ? nullptr : elem_of(head_.next_); }
  T* back() const { return empty() ? nullptr : elem_of(tail_); }

  [[maybe_unused]] bool PushBack(T* elem) {
    Node* node = node_of(elem);
    if (node->InQueue()) return false;

    tail_->next_ = node;
    node->next_ = &head_;
    tail_ = node;
    size_++;
    return true;
  }

  [[maybe_unused]] bool PushFront(T* elem) {
    Node* node = node_of(elem);
    if (node->InQueue()) return false;

    const bool was_empty = empty();
    node->next_ = head_.next_;
    head_.next_ = node;
    if (was_empty) {
      tail_ = node;
    }
    ++size_;
    return true;
  }

  T* PopFront() {
    if (empty()) return nullptr;
    Node* node = head_.next_;
    head_.next_ = node->next_;
    node->clear_hook();
    --size_;
    if (empty()) {
      tail_ = &head_;
    }
    return elem_of(node);
  }

  void Clear() {
    for (Node* cur = head_.next_; cur != &head_;) {
      Node* next = cur->next_;
      cur->clear_hook();
      cur = next;
    }
    reset();
  }

  void Splice(IntrusiveQueue& other) {
    assert(&other != this);
    if (other.empty()) return;
    Node* first = other.head_.next_;
    Node* last = other.tail_;
    tail_->next_ = first;
    last->next_ = &head_;
    tail_ = last;
    size_ += other.size_;

    other.reset();
  }

  template <typename Fn>
  void ForEachSafe(Fn fn) {
    Node* prev = &head_;
    for (Node* cur = head_.next_; cur != &head_;) {
      Node* next = cur->next_;
      if (fn(*elem_of(cur))) {
        prev->next_ = next;
        if (tail_ == cur) {
          tail_ = prev;
        }
        cur->clear_hook();
        --size_;
      } else {
        prev = cur;
      }
      cur = next;
    }
  }

private:
  void reset() noexcept {
    head_.next_ = &head_;
    tail_ = &head_;
    size_ = 0;
  }

  void TakeFrom(IntrusiveQueue& other) noexcept {
    if (other.empty()) return;

    head_.next_ = other.head_.next_;
    tail_ = other.tail_;
    size_ = other.size_;
    tail_->next_ = &head_;
    other.reset();
  }

  Node head_{};
  Node* tail_{&head_};
  std::size_t size_{0};
};

}  // namespace coropact::ds
