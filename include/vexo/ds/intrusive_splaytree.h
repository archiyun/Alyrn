// Copyright (c) 2026 RomenJens. All Rights Reserved.
// SPDX-License-Identifier: MIT
//
// Intrusive top-down/bottom-up splay tree. Node storage lives in T through a
// base hook: T must publicly inherit SplayNode<T, Tag>, so the tree can recover
// T* from a node with static_cast.
//
// Unlike the red-black tree in this directory, this tree uses nullptr as the
// null leaf (no sentinel node). Node metadata is compressed with pointer
// tagging: the parent pointer shares one uintptr_t-sized field with a low-bit
// "linked" flag.
//
// Every access (Insert/Erase, and a found duplicate) splays the touched node to
// the root, giving O(log n) amortized operations. The minimum node is cached so
// earliest() is O(1); rotations, splays and joins never change which node holds
// the minimum key, so the cache only needs refreshing when the min is erased.
//
// Template parameters:
//   T     - element type; must publicly inherit SplayNode<T, Tag>
//   kLess - total order on T; must be irreflexive and transitive
//   Tag   - optional hook tag for objects that inherit multiple SplayNode hooks
//
// Elements must remain at stable addresses and their ordering keys must not
// change while linked. The tree is not thread-safe; synchronize the tree and
// all linked elements externally.
#pragma once

#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "vexo/utils/macros.h"

namespace vexo::ds {

template <typename T, auto kLess, class Tag = void>
class IntrusiveSplayTree;

template <typename T, class Tag = void>
class SplayNode {
  template <typename, auto, class>
  friend class IntrusiveSplayTree;

public:
  [[nodiscard]] bool InTree() const noexcept { return linked(); }

protected:
  VEXO_DELETE_COPY(SplayNode);

  SplayNode() = default;
  ~SplayNode() = default;

private:
  using Node = SplayNode<T, Tag>;

  static constexpr std::uintptr_t kLinked = 1;  // bit 0
  static constexpr std::uintptr_t kFlagMask = kLinked;

  std::uintptr_t parent_and_flags_{0};
  Node* left_{nullptr};
  Node* right_{nullptr};
#ifndef NDEBUG
  const void* owner_{nullptr};
#endif

  Node* parent() const noexcept { return reinterpret_cast<Node*>(parent_and_flags_ & ~kFlagMask); }
  void set_parent(Node* parent) noexcept {
    std::uintptr_t raw = reinterpret_cast<std::uintptr_t>(parent);
    assert((raw & kFlagMask) == 0);
    parent_and_flags_ = raw | (parent_and_flags_ & kFlagMask);
  }

  Node* left() const noexcept { return left_; }
  void set_left(Node* left) noexcept { left_ = left; }
  Node* right() const noexcept { return right_; }
  void set_right(Node* right) noexcept { right_ = right; }

  [[nodiscard]] bool linked() const noexcept { return (parent_and_flags_ & kLinked) != 0; }
  void set_linked(const bool linked) noexcept {
    if (linked) {
      parent_and_flags_ |= kLinked;
    } else {
      parent_and_flags_ &= ~kLinked;
    }
  }

  void clear_hook() noexcept {
    left_ = nullptr;
    right_ = nullptr;
    parent_and_flags_ = 0;
#ifndef NDEBUG
    owner_ = nullptr;
#endif
  }

#ifndef NDEBUG
  const void* owner() const noexcept { return owner_; }
  void set_owner(const void* owner) noexcept { owner_ = owner; }
#endif
};

template <typename T, class Tag = void>
concept SplayNodeBaseHook =
    std::derived_from<T, SplayNode<T, Tag>> && requires(T* elem, SplayNode<T, Tag>* node) {
      { static_cast<SplayNode<T, Tag>*>(elem) } -> std::same_as<SplayNode<T, Tag>*>;
      { static_cast<T*>(node) } -> std::same_as<T*>;
    };

template <typename T, auto kLess, class Tag>
class IntrusiveSplayTree {
public:
  VEXO_DELETE_COPY_MOVE(IntrusiveSplayTree);

  using Node = SplayNode<T, Tag>;
  static_assert(alignof(Node) >= 8,
                "SplayNode must be at least 8-byte aligned for pointer tagging");
  static_assert(SplayNodeBaseHook<T, Tag>,
                "T must publicly and non-virtually inherit SplayNode<T, Tag>");

  IntrusiveSplayTree() = default;
  ~IntrusiveSplayTree() { Clear(); }

  // O(1)
  [[nodiscard]] bool empty() const noexcept { return size_ == 0; }
  // O(1)
  [[nodiscard]] std::size_t size() const noexcept { return size_; }

  // O(n). Unlinks every element without invoking the comparator.
  void Clear() noexcept;

  // O(log n) amortized; returns false (and splays the existing node) if an
  // equal element is already in the tree.
  bool Insert(T* elem);

  // O(log n) amortized; returns false if elem was not linked in any tree.
  //
  // Precondition: if elem is linked, it must be linked in this exact tree.
  bool Erase(T* elem);

  // O(1) — cached pointer, updated on Insert and Erase.
  [[nodiscard]] T* earliest() noexcept { return min_ == nullptr ? nullptr : elem_of(min_); }
  [[nodiscard]] const T* earliest() const noexcept {
    return min_ == nullptr ? nullptr : elem_of(static_cast<const Node*>(min_));
  }

  // O(k log n) where k is the number of extracted elements.
  // Extracts (and erases) the earliest elements satisfying pred in key order.
  // Stops at the first element that fails the predicate.
  template <typename Pred>
  std::vector<T*> PopWhile(Pred pred);

  // O(k log n). Same traversal as PopWhile(pred), but avoids building a result
  // vector. Each element is erased before on_pop(elem) runs.
  template <typename Pred, typename OnPop>
  std::size_t PopWhile(Pred pred, OnPop on_pop);

  // O(n) - debug only. Verifies BST order, parent/child links, linked state,
  // subtree size and the cached minimum.
  [[nodiscard]] bool CheckInvariants() const;

private:
  struct CheckResult {
    bool ok{true};
    std::size_t count{0};
    const Node* min{nullptr};
  };

  Node* root_{nullptr};
  Node* min_{nullptr};
  std::size_t size_{0};

  static T* elem_of(Node* node) { return static_cast<T*>(node); }
  static const T* elem_of(const Node* node) { return static_cast<const T*>(node); }
  static Node* node_of(T* elem) { return static_cast<Node*>(elem); }

  // O(log n) amortized — walks the left/right spine of a subtree.
  Node* Minimum(Node* node);
  Node* Maximum(Node* node);

  void Transplant(Node* src, Node* dst);
  void LeftRotate(Node* pivot);
  void RightRotate(Node* pivot);
  void Splay(Node* node);
  Node* Join(Node* left, Node* right);

  CheckResult CheckSubtree(const Node* node, const Node* parent, const Node* lower,
                           const Node* upper) const;
};

#define ISPLAY_TMPL template <typename T, auto kLess, class Tag>
#define ISPLAY_TYPE IntrusiveSplayTree<T, kLess, Tag>

ISPLAY_TMPL
auto ISPLAY_TYPE::Minimum(Node* node) -> Node* {
  assert(node != nullptr);
  while (node->left() != nullptr) {
    node = node->left();
  }
  return node;
}

ISPLAY_TMPL
auto ISPLAY_TYPE::Maximum(Node* node) -> Node* {
  assert(node != nullptr);
  while (node->right() != nullptr) {
    node = node->right();
  }
  return node;
}

ISPLAY_TMPL
bool ISPLAY_TYPE::Insert(T* elem) {
  assert(elem != nullptr);
  Node* node = node_of(elem);
  assert(node != nullptr);

  if (node->InTree()) {
    return false;
  }
#ifndef NDEBUG
  assert(node->owner() == nullptr);
#endif
  node->set_left(nullptr);
  node->set_right(nullptr);
  node->set_parent(nullptr);
  node->set_linked(true);
#ifndef NDEBUG
  node->set_owner(this);
#endif

  if (empty()) {
    root_ = node;
    min_ = node;
    ++size_;
    return true;
  }

  Node* parent = nullptr;
  for (Node* cursor = root_; cursor != nullptr;) {
    parent = cursor;
    if (kLess(elem, elem_of(cursor))) {
      cursor = cursor->left();
    } else if (kLess(elem_of(cursor), elem)) {
      cursor = cursor->right();
    } else {
      node->clear_hook();
      Splay(cursor);
      return false;
    }
  }

  if (kLess(elem, elem_of(parent))) {
    parent->set_left(node);
  } else {
    parent->set_right(node);
  }
  node->set_parent(parent);

  if (kLess(elem, elem_of(min_))) {
    min_ = node;
  }

  Splay(node);
  ++size_;
  return true;
}

ISPLAY_TMPL
bool ISPLAY_TYPE::Erase(T* elem) {
  assert(elem != nullptr);
  Node* node = node_of(elem);
  assert(node != nullptr);

  if (!node->InTree()) {
    return false;
  }
#ifndef NDEBUG
  assert(node->owner() == this);
#endif
  assert(root_ != nullptr);

  const bool erasing_min = (node == min_);

  Splay(node);
  assert(root_ == node);

  Node* left = node->left();
  Node* right = node->right();

  if (left != nullptr) {
    left->set_parent(nullptr);
  }
  if (right != nullptr) {
    right->set_parent(nullptr);
  }

  node->clear_hook();

  root_ = Join(left, right);
  --size_;

  if (root_ == nullptr) {
    assert(size_ == 0);
    min_ = nullptr;
  } else if (erasing_min) {
    // Only the min node moving out can change the cached minimum; any other
    // erase leaves the leftmost node (and thus min_) untouched.
    min_ = Minimum(root_);
  }

  return true;
}

ISPLAY_TMPL
void ISPLAY_TYPE::Clear() noexcept {
  Node* node = root_;
  while (node != nullptr) {
    if (node->left() != nullptr) {
      node = node->left();
      continue;
    }
    if (node->right() != nullptr) {
      node = node->right();
      continue;
    }

    Node* parent = node->parent();
    if (parent != nullptr) {
      if (parent->left() == node) {
        parent->set_left(nullptr);
      } else {
        assert(parent->right() == node);
        parent->set_right(nullptr);
      }
    }
    node->clear_hook();
    node = parent;
  }

  root_ = nullptr;
  min_ = nullptr;
  size_ = 0;
}

ISPLAY_TMPL
void ISPLAY_TYPE::Transplant(Node* src, Node* dst) {
  Node* parent = src->parent();
  if (src == root_) {
    root_ = dst;
  } else if (src == parent->left()) {
    parent->set_left(dst);
  } else {
    parent->set_right(dst);
  }
  if (dst != nullptr) {
    dst->set_parent(parent);
  }
}

ISPLAY_TMPL
void ISPLAY_TYPE::LeftRotate(Node* pivot) {
  Node* new_top = pivot->right();
  pivot->set_right(new_top->left());
  if (new_top->left() != nullptr) {
    new_top->left()->set_parent(pivot);
  }
  Transplant(pivot, new_top);
  new_top->set_left(pivot);
  pivot->set_parent(new_top);
}

ISPLAY_TMPL
void ISPLAY_TYPE::RightRotate(Node* pivot) {
  Node* new_top = pivot->left();
  pivot->set_left(new_top->right());
  if (new_top->right() != nullptr) {
    new_top->right()->set_parent(pivot);
  }
  Transplant(pivot, new_top);
  new_top->set_right(pivot);
  pivot->set_parent(new_top);
}

ISPLAY_TMPL
void ISPLAY_TYPE::Splay(Node* node) {
  assert(node != nullptr);

  for (Node *parent, *grandparent; node->parent() != nullptr;) {
    parent = node->parent();
    grandparent = parent->parent();

    if (grandparent == nullptr) {
      // zig
      if (node == parent->left()) {
        RightRotate(parent);
      } else {
        LeftRotate(parent);
      }
    } else if (node == parent->left() && parent == grandparent->left()) {
      // zig-zig: left-left
      RightRotate(grandparent);
      RightRotate(parent);
    } else if (node == parent->right() && parent == grandparent->right()) {
      // zig-zig: right-right
      LeftRotate(grandparent);
      LeftRotate(parent);
    } else if (node == parent->right() && parent == grandparent->left()) {
      // zig-zag: left-right
      LeftRotate(parent);
      RightRotate(grandparent);
    } else {
      // zig-zag: right-left
      RightRotate(parent);
      LeftRotate(grandparent);
    }
  }
  root_ = node;
}

ISPLAY_TMPL
auto ISPLAY_TYPE::Join(Node* left, Node* right) -> Node* {
  if (left == nullptr) {
    if (right != nullptr) {
      right->set_parent(nullptr);
    }
    return right;  // nullptr, or right detached as its own root
  }

  left->set_parent(nullptr);
  root_ = left;

  // Splay the maximum of the left subtree to the root; it then has no right
  // child, so the entire right subtree can hang off it in order.
  Node* max_left = Maximum(left);
  Splay(max_left);

  assert(root_ == max_left);
  assert(max_left->right() == nullptr);

  max_left->set_right(right);
  if (right != nullptr) {
    right->set_parent(max_left);
  }
  return max_left;
}

ISPLAY_TMPL
template <typename Pred>
std::vector<T*> ISPLAY_TYPE::PopWhile(Pred pred) {
  std::vector<T*> result;
  while (!empty()) {
    T* elem = earliest();
    if (!pred(elem)) break;
    result.push_back(elem);
    Erase(elem);
  }
  return result;
}

ISPLAY_TMPL
template <typename Pred, typename OnPop>
std::size_t ISPLAY_TYPE::PopWhile(Pred pred, OnPop on_pop) {
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

ISPLAY_TMPL
auto ISPLAY_TYPE::CheckSubtree(const Node* node, const Node* parent, const Node* lower,
                               const Node* upper) const -> typename ISPLAY_TYPE::CheckResult {
  if (node == nullptr) return {};

  CheckResult result{};
  result.ok = false;

  if (!node->InTree()) return result;
#ifndef NDEBUG
  if (node->owner() != this) return result;
#endif
  if (node->parent() != parent) return result;

  T* elem = elem_of(const_cast<Node*>(node));
  if (kLess(elem, elem)) return result;  // irreflexive
  if (lower != nullptr && !kLess(elem_of(const_cast<Node*>(lower)), elem)) return result;
  if (upper != nullptr && !kLess(elem, elem_of(const_cast<Node*>(upper)))) return result;

  auto left = CheckSubtree(node->left(), node, lower, node);
  if (!left.ok) return result;

  auto right = CheckSubtree(node->right(), node, node, upper);
  if (!right.ok) return result;

  result.ok = true;
  result.count = left.count + right.count + 1;
  result.min = left.count == 0 ? node : left.min;
  return result;
}

ISPLAY_TMPL
bool ISPLAY_TYPE::CheckInvariants() const {
  if (root_ == nullptr) {
    return size_ == 0 && min_ == nullptr;
  }
  if (root_->parent() != nullptr) return false;

  auto result = CheckSubtree(root_, nullptr, nullptr, nullptr);
  return result.ok && result.count == size_ && result.min == min_;
}

#undef ISPLAY_TMPL
#undef ISPLAY_TYPE

}  // namespace vexo::ds
