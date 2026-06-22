// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
// Reference:
//   - Cormen, Leiserson, Rivest, Stein, "Introduction to Algorithms",
//     3rd ed., Chapter 13: Red-Black Trees.
//   - Linux Kernel rbtree implementation by Andrea Arcangeli & Michel Lespinasse
//     (Intrusive node topology, pointer tagging/recoloring optimization).
//   - GNU libstdc++ / LLVM libc++ _Rb_tree implementation
//     (Dual-purpose Header node mapping both root and cached minimum node).
//
// Design note:
//   v1 ~ v3:
//     Experimental intrusive red-black tree implementations based on CLRS.
//     These versions focused on insertion / deletion fix-up correctness and
//     kept parent, color, root, minimum as explicit fields.
//   v4:
//     Modern C++ engineering version.
//     The tree uses base intrusive hooks, C++23 concepts and static
//     helper policies. Node metadata is compressed with pointer tagging: the
//     parent pointer shares one uintptr_t-sized field with low-bit flags such as
//     color and linked state. The tree also uses a sentinel header node, inspired
//     by STL _Rb_tree implementations, where the header caches the root, minimum
//     and maximum nodes. This improves boundary handling and makes begin(),
//     min(), max() and empty checks straightforward.
//      --- 2026-06-08  Copyright (c) Arsenova
#pragma once

#include <cassert>
#include <concepts>
#include <cstdint>
#include <vector>

namespace vexo::ds {

// Intrusive red-black tree. Node storage lives in T through a base hook:
// T must publicly inherit RBTNode<T, Tag>, so the tree can recover T* from a
// node with static_cast.
//
// Each tree owns one sentinel node. The sentinel is used as the null leaf, and
// its three pointer slots also cache tree-level state:
//   parent = root
//   left   = minimum
//   right  = maximum
// This means sentinel.parent() is not a NIL-parent link. DeleteFixup receives
// the explicit parent whenever the fixup node is the sentinel.
//
// Template parameters:
//   T        - element type; must publicly inherit RBTNode<T>
//   kLess    - total order on T; must be irreflexive and transitive
//   Tag      - optional hook tag for objects that inherit multiple RBTNode hooks
//
// This tree is used as an ordered work queue (TimerQueue, deadline scheduling).
// In that pattern PopWhile (extract-all-matching in key order) dominates, so
// minimum is eagerly cached. Rotations during insert/erase never change which
// node is the minimum, so keeping the cache correct costs only a single lookup
// in the Erase path.
//
template <class T, auto kLess, class Tag = void>
class IntrusiveRBTree;

template <typename T, class Tag = void>
class RBTNode {
  template <class, auto, class>
  friend class IntrusiveRBTree;

public:
  bool InTree() const noexcept {
    return linked();
  }

protected:
  RBTNode() = default;
  RBTNode(const RBTNode&) = delete;
  RBTNode& operator=(const RBTNode&) = delete;

private:
  using Node = RBTNode<T, Tag>;

  // Unlike Linux rbtree, this implementation uses bit0 = 1 for red.
  static constexpr std::uintptr_t kRed = 1u; // bit 0
  static constexpr std::uintptr_t kLinked = 1u << 1; // bit 1
  static constexpr std::uintptr_t kReserved = 1u << 2; // bit 2, reserved
  static constexpr std::uintptr_t kFlagMask = kRed | kLinked | kReserved; // ...111

  std::uintptr_t parent_and_flags_{0u}; // parent address plus low-bit flags
  Node* left_{nullptr};
  Node* right_{nullptr};

  Node* parent() const noexcept {
    return reinterpret_cast<Node*>(parent_and_flags_ & ~kFlagMask);
  }

  void set_parent(Node* parent) noexcept {
    auto raw = reinterpret_cast<std::uintptr_t>(parent);
    assert((raw & kFlagMask) == 0);
    parent_and_flags_ = raw | (parent_and_flags_ & kFlagMask);
  }

  Node* left() const noexcept { return left_; }
  void set_left(Node* left) noexcept { left_ = left; }

  Node* right() const noexcept { return right_; }
  void set_right(Node* right) noexcept { right_ = right; }

  bool red() const noexcept {
    return (parent_and_flags_ & kRed) != 0;
  }

  void set_red(bool red) noexcept {
    if (red) {
      parent_and_flags_ |= kRed;
    } else {
      parent_and_flags_ &= ~kRed;
    }
  }

  bool linked() const noexcept { return (parent_and_flags_ & kLinked) != 0; }

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
  }
};

template <class T, class Tag = void>
concept RBTNodeBaseHook =
    std::derived_from<T, RBTNode<T, Tag>> &&
    requires(T* elem, RBTNode<T, Tag>* node) {
      { static_cast<RBTNode<T, Tag>*>(elem) } -> std::same_as<RBTNode<T, Tag>*>;
      { static_cast<T*>(node) } -> std::same_as<T*>;
    };

template <class T, auto kLess, class Tag>
class IntrusiveRBTree {
public:
  using Node = RBTNode<T, Tag>;
  static_assert(alignof(Node) >= 8,
                "RBTNode must be at least 8-byte aligned for pointer tagging");
  static_assert(RBTNodeBaseHook<T, Tag>,
                "T must publicly and non-virtually inherit RBTNode<T, Tag>");

  IntrusiveRBTree() {
    sentinel_.set_left(&sentinel_);
    sentinel_.set_right(&sentinel_);
    sentinel_.set_parent(&sentinel_);
    sentinel_.set_red(false);
  }
  ~IntrusiveRBTree() = default;

  // O(1)
  bool empty() const { return size_ == 0; }
  // O(1)
  std::size_t size() const noexcept { return size_; }
  // O(log n) worst-case; return false if elem is already in the tree.
  bool Insert(T* elem);

  // O(log n) worst-case; returns false if elem was not linked in any tree.
  //
  // Precondition: if elem is linked, it must be linked in this exact tree.
  // Passing an element linked in another IntrusiveRBTree instance is undefined
  // behavior. InTree() only says whether the element is linked somewhere.
  bool Erase(T* elem);

  // O(1) — cached pointer, updated on Insert and Erase.
  T* earliest() const { return min() == sentinel() ? nullptr : elem_of(min()); }

  // O(k log n) where k is the number of extracted elements.
  // Extracts (and erases) the earliest elements satisfying pred in key order.
  // Stops at the first element that fails the predicate.
  template <typename Pred>
  std::vector<T*> PopWhile(Pred pred);

  // O(k log n). Same traversal as PopWhile(pred), but avoids building a
  // result vector. Each element is erased before on_pop(elem) runs.
  template <typename Pred, typename OnPop>
  std::size_t PopWhile(Pred pred, OnPop on_pop);

  // O(n) - debug only. Verifies RB invariants, BST order, parent/child links,
  // linked state, subtree size and cached root/min/max state.
  bool CheckRBInvariants() const;
private:
  struct CheckResult {
    bool ok{true};
    int black_height{0};
    std::size_t count{0};
    const Node* min{nullptr};
    const Node* max{nullptr};
  };

  Node sentinel_{};
  std::size_t size_{0};
  Node* sentinel() noexcept { return &sentinel_; }
  const Node* sentinel() const noexcept { return &sentinel_; }

  Node* root() noexcept { return sentinel_.parent(); }
  const Node* root() const noexcept { return sentinel_.parent(); }
  void set_root(Node* node) noexcept {
    sentinel_.set_parent(node);     // sentinel.parent = root
    node->set_parent(sentinel());   // root.parent = sentinel
  }

  static Node* node_of(T* elem) { return static_cast<Node*>(elem); }
  static T* elem_of(Node* node) { return static_cast<T*>(node); }

  Node* min() const noexcept { return sentinel_.left(); }
  void set_min(Node* node) noexcept { sentinel_.set_left(node); }
  Node* max() const noexcept { return sentinel_.right(); }
  void set_max(Node* node) noexcept { sentinel_.set_right(node); }

  inline static void Red(Node* node) { node->set_red(true); }
  inline static void Black(Node* node) { node->set_red(false); }
  inline static bool IsRed(const Node* node) { return node->red(); }
  inline static bool IsBlack(const Node* node) { return !IsRed(node); }
  inline static void CopyColor(Node* dst, const Node* src) { dst->set_red(src->red()); }

  // O(log n) — walks left spine of subtree.
  Node* Minimum(Node* node);
  // O(log n) - walks right spine of subtree.
  Node* Maximum(Node* node);

  // Worst-case O(log n) — either descends once via Minimum, or walks parent
  // links (at most tree height).  Called once per Erase that removes min_.
  Node* Next(Node* node);
  Node* Prev(Node* node);

  CheckResult CheckSubtree(const Node* node,
                           const Node* parent,
                           const Node* lower,
                           const Node* upper) const;

  void Transplant(Node* src, Node* dst);
  void LeftRotate(Node* pivot);
  void RightRotate(Node* pivot);
  void InsertFixup(Node* node);
  void DeleteFixup(Node* node, Node* parent);
};

#define IRBT_TMPL template <typename T, auto kLess, class Tag>
#define IRBT_TYPE IntrusiveRBTree<T, kLess, Tag>

IRBT_TMPL
auto IRBT_TYPE::Minimum(Node* node) -> Node* {
  assert(node != sentinel());

  while (node->left() != sentinel()) {
    node = node->left();
  }
  return node;
}

IRBT_TMPL
auto IRBT_TYPE::Next(Node* node) -> Node* {
  assert(node != sentinel());

  if (node->right() != sentinel()) return Minimum(node->right());
  if (node == root()) return sentinel();
  while (true) {
    auto* parent = node->parent();
    if (parent == sentinel()) return sentinel();
    if (node == parent->left()) return parent;
    node = parent;
  }
}

IRBT_TMPL
auto IRBT_TYPE::Maximum(Node* node) -> Node* {
  assert(node != sentinel());
  while (node->right() != sentinel()) {
    node = node->right();
  }
  return node;
}

IRBT_TMPL
auto IRBT_TYPE::Prev(Node* node) -> Node* {
  assert(node != sentinel());

  if (node->left() != sentinel()) return Maximum(node->left());
  if (node == root()) return sentinel();
  while (true) {
    auto* parent = node->parent();
    if (parent == sentinel()) return sentinel();
    if (node == parent->right()) return parent;
    node = parent;
  }
}

// Replace subtree rooted at src with subtree rooted at dst.
IRBT_TMPL
auto IRBT_TYPE::Transplant(Node* src, Node* dst) -> void {
  assert(src != sentinel());

  auto* parent = src->parent();
  if (parent == sentinel())
    set_root(dst);
  else if (src == parent->left())
    parent->set_left(dst);
  else // src == parent->right()
    parent->set_right(dst);
  if (dst != sentinel()) {
    dst->set_parent(parent);
  }
}

// -- Rotations --
IRBT_TMPL
auto IRBT_TYPE::LeftRotate(Node* pivot) -> void {
  assert(pivot != sentinel());

  auto* new_top = pivot->right();
  pivot->set_right(new_top->left());
  if (new_top->left() != sentinel()) new_top->left()->set_parent(pivot);
  Transplant(pivot, new_top);
  new_top->set_left(pivot);
  pivot->set_parent(new_top);
}

IRBT_TMPL
auto IRBT_TYPE::RightRotate(Node* pivot) -> void {
  assert(pivot != sentinel());

  auto* new_top = pivot->left();
  pivot->set_left(new_top->right());
  if (new_top->right() != sentinel()) new_top->right()->set_parent(pivot);
  Transplant(pivot, new_top);
  new_top->set_right(pivot);
  pivot->set_parent(new_top);
}

// -- Insert and InsertFixup --
IRBT_TMPL
auto IRBT_TYPE::InsertFixup(Node* node) -> void {
  assert(node != nullptr);
  assert(node != sentinel());
  assert(node->InTree());
  assert(IsRed(node));
  assert(root() != sentinel());
  assert(IsBlack(sentinel()));

  assert(node->left() == sentinel());
  assert(node->right() == sentinel());

  while (node != root() && IsRed(node->parent())) {
    auto* parent = node->parent();

    assert(parent != nullptr);
    assert(parent != sentinel());
    assert(parent->left() == node || parent->right() == node);
    assert(IsRed(parent));

    auto* grandparent = parent->parent();

    assert(grandparent != nullptr);
    assert(grandparent != sentinel());
    assert(grandparent->left() == parent || grandparent->right() == parent);
    assert(IsBlack(grandparent));

    if (parent == grandparent->left()) {
      auto* uncle = grandparent->right();
      if (IsRed(uncle)) {
        // CLRS insert case 1: parent and uncle are red. Recolor and move the
        // violation up to the grandparent.
        Black(parent);
        Black(uncle);
        Red(grandparent);
        node = grandparent;
      } else {
        if (node == parent->right()) {
          // CLRS insert case 2: triangle shape. Rotate parent to convert to
          // case 3.
          node = parent;
          LeftRotate(node);
          parent = node->parent();
        }
        // CLRS insert case 3: line shape. Recolor and rotate grandparent.
        Black(parent);
        Red(grandparent);
        RightRotate(grandparent);
      }
    } else {
      auto* uncle = grandparent->left();
      if (IsRed(uncle)) {
        // CLRS insert case 1, mirrored.
        Black(parent);
        Black(uncle);
        Red(grandparent);
        node = grandparent;
      } else {
        if (node == parent->left()) {
          // CLRS insert case 2, mirrored.
          node = parent;
          RightRotate(node);
          parent = node->parent();
        }
        // CLRS insert case 3, mirrored.
        Black(parent);
        Red(grandparent);
        LeftRotate(grandparent);
      }
    }
  }
  Black(root());
}

IRBT_TMPL
bool IRBT_TYPE::Insert(T* elem) {
  assert(elem != nullptr);

  auto* node = node_of(elem);

  assert(node != nullptr);
  assert(node != sentinel());

  if (node->InTree()) return false;

  assert(node->left() == nullptr);
  assert(node->right() == nullptr);
  assert(node->parent() == nullptr);
  assert(IsBlack(node));

  node->set_linked(true);
  node->set_left(sentinel());
  node->set_right(sentinel());
  node->set_parent(sentinel());
  Red(node);

  auto* parent = sentinel();
  for (auto* cursor = root(); cursor != sentinel();) {
    parent = cursor;

    assert(cursor->InTree());
    assert(cursor->left() != nullptr);
    assert(cursor->right() != nullptr);

    cursor = kLess(elem, elem_of(cursor)) ? cursor->left() : cursor->right();
  }
  node->set_parent(parent);
  if (parent == sentinel()) {
    assert(root() == sentinel());
    set_root(node);
  }
  else if (kLess(elem, elem_of(parent))) {
    assert(parent->left() == sentinel());
    parent->set_left(node);
  }
  else {
    // kLess(elem_of(parent), elem)
    assert(parent->right() == sentinel());
    parent->set_right(node);
  }

  if (min() == sentinel() || kLess(elem, elem_of(min()))) {
    set_min(node);
  }
  if (max() == sentinel() || kLess(elem_of(max()), elem)) {
    set_max(node);
  }
  InsertFixup(node);
  ++size_;

  assert(root() != sentinel());
  assert(root()->parent() == sentinel());
  assert(IsBlack(root()));
  assert(min() != sentinel());
  assert(max() != sentinel());
  assert(node->InTree());

  return true;
}

// -- Delete and DeleteFixup --
// parent is the explicit parent of node. This matters when node == sentinel(),
// because sentinel.parent() stores the root cache.
IRBT_TMPL
auto IRBT_TYPE::DeleteFixup(Node* node, Node* parent) -> void {
  assert(node != nullptr);
  assert(parent != nullptr);

  while (node != root() && IsBlack(node)) {
    assert(parent != sentinel());

    if (node == parent->left()) {
      auto* sibling = parent->right();

      assert(sibling != sentinel());

      if (IsRed(sibling)) {
        // CLRS delete case 1: sibling is red. Rotate to make the sibling
        // black, then continue with one of cases 2-4.
        Black(sibling);
        Red(parent);
        LeftRotate(parent);
        sibling = parent->right();
        assert(sibling != sentinel());
      }
      auto* sibling_left = sibling->left();
      auto* sibling_right = sibling->right();
      if (IsBlack(sibling_left) && IsBlack(sibling_right)) {
        // CLRS delete case 2: sibling and both sibling children are black.
        // Push the extra blackness up to the parent.
        Red(sibling);
        node = parent;
        parent = node->parent();
      } else {
        if (IsBlack(sibling_right)) {
          // CLRS delete case 3: sibling is black, sibling's near child is red,
          // and far child is black. Rotate sibling to convert to case 4.
          Black(sibling_left);
          Red(sibling);
          RightRotate(sibling);
          sibling = parent->right();
          assert(sibling != sentinel());
        }
        // CLRS delete case 4: sibling is black and sibling's far child is red.
        // Rotate parent and recolor to remove the extra blackness.
        CopyColor(sibling, parent);
        Black(parent);
        Black(sibling->right());
        LeftRotate(parent);

        node = root();
        parent = sentinel();
      }
    } else {
      assert(node == parent->right());

      auto* sibling = parent->left();
      assert(sibling != sentinel());

      if (IsRed(sibling)) {
        // CLRS delete case 1, mirrored.
        Black(sibling);
        Red(parent);
        RightRotate(parent);
        sibling = parent->left();
        assert(sibling != sentinel());
      }
      auto* sibling_left = sibling->left();
      auto* sibling_right = sibling->right();
      if (IsBlack(sibling_left) && IsBlack(sibling_right)) {
        // CLRS delete case 2, mirrored.
        Red(sibling);
        node = parent;
        parent = node->parent();
      } else {
        if (IsBlack(sibling_left)) {
          // CLRS delete case 3, mirrored.
          Black(sibling_right);
          Red(sibling);
          LeftRotate(sibling);
          sibling = parent->left();
          assert(sibling != sentinel());
        }
        // CLRS delete case 4, mirrored.
        CopyColor(sibling, parent);
        Black(parent);
        Black(sibling->left());
        RightRotate(parent);

        node = root();
        parent = sentinel();
      }
    }
  }
  Black(node);
}

IRBT_TMPL
bool IRBT_TYPE::Erase(T* elem) {
  assert(elem != nullptr);

  auto* target = node_of(elem);

  assert(target != nullptr);
  assert(target != sentinel());

  if (!target->InTree()) return false;

  assert(root() != sentinel());
  assert(size_ > 0);

  assert(target->left() != nullptr);
  assert(target->right() != nullptr);
  assert(target->parent() != nullptr);

  if (target == min()) {
    set_min(Next(min()));
  }
  if (target == max()) {
    set_max(Prev(max()));
  }

  auto* detach = target;
  auto* orphan = sentinel();
  auto* orphan_parent = sentinel();
  bool detach_original_is_red = IsRed(detach);

  if (target->left() == sentinel()) {
    orphan = target->right();
    orphan_parent = target->parent();

    assert(orphan_parent != nullptr);
    assert(orphan_parent == sentinel() || orphan_parent->left() == target ||
           orphan_parent->right() == target);

    Transplant(target, target->right());
  } else if (target->right() == sentinel()) {
    orphan = target->left();
    orphan_parent = target->parent();

    assert(orphan_parent != nullptr);
    assert(orphan_parent == sentinel() || orphan_parent->left() == target ||
           orphan_parent->right() == target);

    Transplant(target, target->left());
  } else {
    // The target's leftChild and rightChild all exist.
    // Successor must be the minimum of the right subtree
    // and therefore has no real left child.
    assert(target->left() != sentinel());
    assert(target->right() != sentinel());

    detach = Minimum(target->right());

    assert(detach != sentinel());
    assert(detach->left() == sentinel());

    detach_original_is_red = IsRed(detach);
    orphan = detach->right();

    if (detach->parent() == target) {
      orphan_parent = detach;
      if (orphan != sentinel()) {
        orphan->set_parent(detach);
        assert(orphan->parent() == detach);
      }
    } else {
      orphan_parent = detach->parent();

      assert(orphan_parent != nullptr);
      assert(orphan_parent != sentinel());
      assert(orphan_parent->left() == detach);
      assert(detach->right() == orphan);

      Transplant(detach, orphan);

      detach->set_right(target->right());
      detach->right()->set_parent(detach);

      assert(detach->right() != sentinel());
      assert(detach->right()->parent() == detach);
    }

    Transplant(target, detach);

    assert(detach->parent() == target->parent() ||
           detach->parent() == sentinel());

    detach->set_left(target->left());
    detach->left()->set_parent(detach);

    assert(detach->left() != sentinel());
    assert(detach->left()->parent() == detach);

    CopyColor(detach, target);
  }

  assert(orphan != nullptr);
  assert(orphan_parent != nullptr);

  if (!detach_original_is_red) {
    DeleteFixup(orphan, orphan_parent);
  }
  target->clear_hook();
  --size_;

  if (size_ == 0) {
    assert(root() == sentinel());
    assert(min() == sentinel());
    assert(max() == sentinel());
  } else {
    assert(root() != sentinel());
    assert(root()->parent() == sentinel());
    assert(min() != sentinel());
    assert(max() != sentinel());
  }

  return true;
}

IRBT_TMPL
template <typename Pred>
std::vector<T*> IRBT_TYPE::PopWhile(Pred pred) {
  std::vector<T*> result;
  while (!empty()) {
    T* elem = earliest();
    if (!pred(elem)) break;
    result.push_back(elem);
    Erase(elem);
  }
  return result;
}

IRBT_TMPL
template <typename Pred, typename OnPop>
std::size_t IRBT_TYPE::PopWhile(Pred pred, OnPop on_pop) {
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

IRBT_TMPL
auto IRBT_TYPE::CheckSubtree(const Node* node,
                             const Node* parent,
                             const Node* lower,
                             const Node* upper) const
    -> typename IRBT_TYPE::CheckResult {
  if (node == sentinel()) return {};

  CheckResult result{};
  result.ok = false;

  if (node == nullptr || parent == nullptr) return result;
  if (!node->InTree()) return result;
  if (node->parent() != parent) return result;
  if (node->left() == nullptr || node->right() == nullptr) return result;

  auto* elem = elem_of(const_cast<Node*>(node));
  if (kLess(elem, elem)) return result;
  if (lower != nullptr &&
      !kLess(elem_of(const_cast<Node*>(lower)), elem)) {
    return result;
  }
  if (upper != nullptr &&
      !kLess(elem, elem_of(const_cast<Node*>(upper)))) {
    return result;
  }

  if (IsRed(node) && (IsRed(node->left()) || IsRed(node->right()))) {
    return result;
  }

  auto left = CheckSubtree(node->left(), node, lower, node);
  if (!left.ok) return result;

  auto right = CheckSubtree(node->right(), node, node, upper);
  if (!right.ok) return result;

  if (left.black_height != right.black_height) return result;

  result.ok = true;
  result.black_height = left.black_height + (IsBlack(node) ? 1 : 0);
  result.count = left.count + right.count + 1;
  result.min = left.count == 0 ? node : left.min;
  result.max = right.count == 0 ? node : right.max;
  return result;
}

IRBT_TMPL
auto IRBT_TYPE::CheckRBInvariants() const -> bool {
  if (!IsBlack(sentinel()) || sentinel()->InTree()) {
    return false;
  }

  if (root() == nullptr || min() == nullptr || max() == nullptr) {
    return false;
  }

  if (root() == sentinel()) {
    return size_ == 0 && min() == sentinel() && max() == sentinel();
  }
  if (size_ == 0 || min() == sentinel() || max() == sentinel() ||
      root()->parent() != sentinel() || IsRed(root())) {
    return false;
  }

  auto result = CheckSubtree(root(), sentinel(), nullptr, nullptr);
  return result.ok &&
         result.count == size_ &&
         result.min == min() &&
         result.max == max();
}

#undef IRBT_TMPL
#undef IRBT_TYPE

}  // namespace vexo::ds
