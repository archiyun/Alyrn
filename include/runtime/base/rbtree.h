
#pragma once

#include "runtime/base/noncopyable.h"

#include <vector>

namespace runtime::base {

// The red-black tree code is based on the algorithm described in
// the "Introduction to Algorithms" by Cormen, Leiserson ,Stein and Rivest.
template<typename T>
struct RBTNode {
  T*        owner{nullptr};
  RBTNode*  parent{nullptr};
  RBTNode*  left{nullptr};
  RBTNode*  right{nullptr};
  bool      red{false};
  bool      in_tree{false};
};


template<
    typename T,
    RBTNode<T> T::* kMember,           // Member pointer of Intrusive Node
    bool (*kLess)(const T*, const T*)  // Comparator
>
class IntrusiveRBTree : public NonCopyable {
public:
  IntrusiveRBTree() : root_{kSentinel} {}
  ~IntrusiveRBTree() = default;

  bool  Empty() const { return size_ == 0; }
  std::size_t Size() const { return size_; }
  void  Insert(T* elem);
  bool  Erase(T* elem);
  T*    Earliest() const;

  template<typename Pred>
  std::vector<T*> PopWhile(Pred pred);

  // Returns true if all RB-tree invariants hold:
  //   1. root is black
  //   2. no two consecutive red nodes
  //   3. all root-to-leaf paths have the same black height
  bool CheckRBInvariants() const;

private:
  // Returns black-height of subtree, or -1 if any invariant is violated.
  int BlackHeightOf(RBTNode<T>* node) const;
  inline static RBTNode<T> sentinel_node_{};
  inline static RBTNode<T>* const kSentinel = []()->RBTNode<T>*{
    sentinel_node_.parent = &sentinel_node_;
    sentinel_node_.left = &sentinel_node_;
    sentinel_node_.right = &sentinel_node_;
    sentinel_node_.in_tree = true;
    return &sentinel_node_;
  }();

  inline static bool IsRed(const RBTNode<T>* node) { return node->red; }
  inline static bool IsBlack(const RBTNode<T>* node) { return !IsRed(node); }
  inline static void Red(RBTNode<T>* node) { node->red = true; }
  inline static void Black(RBTNode<T>* node) { node->red = false; }
  inline static void CopyColor(RBTNode<T>* dst, RBTNode<T>* src) { dst->red = src->red; }

  static RBTNode<T>* NodeOf(T* elem) { return &(elem->*kMember);}
  static T*          ElemOf(RBTNode<T>* node) { return node->owner; }

  static RBTNode<T>* Minimum(RBTNode<T>* node);
  static RBTNode<T>* Next(RBTNode<T>* node);
  static void ResetNode(RBTNode<T>* node);

  void Transplant(RBTNode<T>* src, RBTNode<T>* dst);
  void LeftRotate(RBTNode<T>* pivot);
  void RightRotate(RBTNode<T>* pivot);
  void InsertFixup(RBTNode<T>* node);
  void DeleteFixup(RBTNode<T>* node);

  RBTNode<T>* root_{kSentinel};
  std::size_t size_{0};
};

#define IRBT_TMPL template<typename T, RBTNode<T> T::* kMember, bool (*kLess)(const T*, const T*)>
#define IRBT_TYPE IntrusiveRBTree<T, kMember, kLess>

IRBT_TMPL
RBTNode<T>* IRBT_TYPE::Minimum(RBTNode<T>* node) {
  while (node->left != kSentinel) {
    node = node->left;
  }
  return node;
}

IRBT_TMPL
RBTNode<T>* IRBT_TYPE::Next(RBTNode<T>* node) {
  if (node->right != kSentinel) {
    return Minimum(node->right);
  }
  while (true) {
    auto parent = node->parent;
    if (parent == kSentinel || node == parent) {
      return kSentinel;
    }
    if (node == parent->left) {
      return parent;
    }
    node = parent;
  }
}

// Replace subtree rooted at src with subtree rooted at dst.
IRBT_TMPL
void IRBT_TYPE::Transplant(RBTNode<T>* src, RBTNode<T>* dst) {
  if (src->parent == kSentinel) root_ = dst;
  else if (src == src->parent->left) src->parent->left = dst;
  else src->parent->right = dst;
  dst->parent = src->parent;
}

// -- Rotations --
IRBT_TMPL
void IRBT_TYPE::LeftRotate(RBTNode<T>* pivot) {
  auto new_top = pivot->right;
  pivot->right = new_top->left;
  if (new_top->left != kSentinel) new_top->left->parent = pivot;
  Transplant(pivot, new_top);
  new_top->left = pivot;
  pivot->parent = new_top;
}

IRBT_TMPL
void IRBT_TYPE::RightRotate(RBTNode<T>* pivot) {
  auto new_top = pivot->left;
  pivot->left = new_top->right;
  if (new_top->right != kSentinel) new_top->right->parent = pivot;
  Transplant(pivot, new_top);
  new_top->right = pivot;
  pivot->parent = new_top;
}

IRBT_TMPL
void IRBT_TYPE::ResetNode(RBTNode<T>* node) {
  node->left = kSentinel;
  node->right = kSentinel;
  node->parent = kSentinel;
  node->in_tree = false;
  Black(node);
}

// -- Insert and InsertFicUp --
IRBT_TMPL
void IRBT_TYPE::InsertFixup(RBTNode<T>* node) {
  while (node != root_ && IsRed(node->parent)) {
    if (node->parent == node->parent->parent->left) {
      auto uncle = node->parent->parent->right;
      if (IsRed(uncle)) {
        Black(node->parent);
        Black(uncle);
        Red(node->parent->parent);
        node = node->parent->parent;
      } else {
        // Uncle color is Black
        if (node == node->parent->right) {
          node = node->parent;
          LeftRotate(node);
        }
        Black(node->parent);
        Red(node->parent->parent);
        RightRotate(node->parent->parent);
      }
    } else {
      auto uncle = node->parent->parent->left;
      if (IsRed(uncle)) {
        Black(node->parent);
        Black(uncle);
        Red(node->parent->parent);
        node = node->parent->parent;
      } else {
        // Uncle color is Black
        if (node == node->parent->left) {
          node = node->parent;
          RightRotate(node);
        }
        Black(node->parent);
        Red(node->parent->parent);
        LeftRotate(node->parent->parent);
      }
    }
  }
  Black(root_);
}

IRBT_TMPL
void IRBT_TYPE::Insert(T* elem) {
  auto node = NodeOf(elem);
  if (node->in_tree) return;
  node->owner = elem;
  node->in_tree = true;
  node->left = kSentinel;
  node->right = kSentinel;
  node->parent = kSentinel;
  Red(node);

  auto parent = kSentinel;
  for (auto cursor = root_ ; cursor != kSentinel; ) {
    parent = cursor;
    cursor = kLess(elem, ElemOf(cursor)) ? cursor->left : cursor->right;
  }
  node->parent = parent;
  if (parent == kSentinel) root_ = node;
  else if (kLess(elem, ElemOf(parent))) parent->left = node;
  else parent->right = node;

  ++size_;
  InsertFixup(node);
}
// -- Delete and DeleteFixUp
IRBT_TMPL
void IRBT_TYPE::DeleteFixup(RBTNode<T>* node) {
  while (node != root_ && IsBlack(node)) {
    auto parent = node->parent;
    if (node == parent->left) {
      auto sibling = parent->right;
      if (IsRed(sibling)) {
        Black(sibling);
        Red(parent);
        LeftRotate(parent);
        sibling = parent->right;
      } 
      if (IsBlack(sibling->left) && IsBlack(sibling->right)) {
        Red(sibling);
        node = parent;
      } else {
        if (IsBlack(sibling->right)) {
          Black(sibling->left);
          Red(sibling);
          RightRotate(sibling);
          sibling = parent->right;
        }
        // Case 4: sibing's left child is red - rotate parent and recolor
        CopyColor(sibling, parent);
        Black(parent);
        Black(sibling->right);
        LeftRotate(parent);
        break;
      }
    } else {
      auto sibling = parent->left;
      if (IsRed(sibling)) {
        Black(sibling);
        Red(parent);
        RightRotate(parent);
        sibling = parent->left;
      }
      if (IsBlack(sibling->left) && IsBlack(sibling->right)) {
        Red(sibling);
        node = parent;
      } else {
        if (IsBlack(sibling->left)) {
          Black(sibling->right);
          Red(sibling);
          LeftRotate(sibling);
          sibling = parent->left;
        }
        CopyColor(sibling, parent);
        Black(parent);
        Black(sibling->left);
        RightRotate(parent);
        break;
      }
    }
  }
  Black(node);
}

IRBT_TMPL
bool IRBT_TYPE::Erase(T* elem) {
  auto target = NodeOf(elem);
  if (!target->in_tree) return false;
  auto detach = target;
  auto orphan = kSentinel;
  bool detach_original_is_red = IsRed(detach);
  if (target->left == kSentinel) {
    orphan = target->right;
    Transplant(target, target->right);
  } else if (target->right == kSentinel) {
    orphan = target->left;
    Transplant(target, target->left);
  } else {
    // The target's leftChild and rightChild all exist.
    detach = Minimum(target->right);
    detach_original_is_red = IsRed(detach);
    orphan = detach->right;

    if (detach->parent == target) {
      orphan->parent = detach;
    } else {
      Transplant(detach, orphan);
      detach->right = target->right;
      target->right->parent = detach;
    }
    Transplant(target, detach);
    target->left->parent = detach;
    detach->left = target->left;
    CopyColor(detach, target);
  }

  ResetNode(target);
  --size_;

  if (!detach_original_is_red) {
    DeleteFixup(orphan);
  }
  return true;
}


IRBT_TMPL
T* IRBT_TYPE::Earliest() const {
  if (Empty()) return nullptr;
  return ElemOf(Minimum(root_));
}

IRBT_TMPL
template<typename Pred>
std::vector<T*> IRBT_TYPE::PopWhile(Pred pred) {
  std::vector<T*> result;
  while (!Empty()) {
    T* elem = Earliest();
    if (!pred(elem)) break;
    result.push_back(elem);
    Erase(elem);
  }
  return result;
}

IRBT_TMPL
int IRBT_TYPE::BlackHeightOf(RBTNode<T>* node) const {
  if (node == kSentinel) return 0;
  if (IsRed(node) && (IsRed(node->left) || IsRed(node->right))) return -1;
  int lh = BlackHeightOf(node->left);
  int rh = BlackHeightOf(node->right);
  if (lh < 0 || rh < 0 || lh != rh) return -1;
  return lh + (IsBlack(node) ? 1 : 0);
}

IRBT_TMPL
bool IRBT_TYPE::CheckRBInvariants() const {
  if (root_ != kSentinel && IsRed(root_)) return false;
  if (size_ == 0) return root_ == kSentinel;
  return BlackHeightOf(root_) >= 0;
}

#undef IRBT_TMPL
#undef IRBT_TYPE
};