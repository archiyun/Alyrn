#include "runtime/net/timer.h"
#include "runtime/net/timer_rbtree.h"

namespace runtime::net {

// The sentinel is a shared black null-leaf node; its parent/left/right all
// point back to itself so boundary checks in rotate/fixup never need nullptr guards.
TimerTreeNode* TimerTree::Sentinel() {
  static TimerTreeNode sentinel {nullptr, nullptr, nullptr, nullptr, TimerTreeColor::kBlack, true};
  sentinel.parent = &sentinel;
  sentinel.left = &sentinel;
  sentinel.right = &sentinel;
  return &sentinel;
}

TimerTree::TimerTree() : sentinel_(Sentinel()), root_(sentinel_) {}

// Order by (expiration, sequence) so euqal-time timers are still distinct
bool TimerTree::Less(const Timer* lhs, const Timer* rhs) {
    if (lhs->Expiration() < rhs->Expiration()) return true;
    if (lhs->Expiration() > rhs->Expiration()) return false;
    return lhs->Sequence() < rhs->Sequence();
}
TimerTreeNode* TimerTree::NodeOf(Timer* timer) {
    return &timer->tree_node_;
}

Timer* TimerTree::Earliest() const {
  if (Empty()) return nullptr;
  return Minimum(root_)->timer_key;
} 

// -- Rotations --
void TimerTree::LeftRotate(TimerTreeNode* pivot) {
  TimerTreeNode* new_top = pivot->right;
  pivot->right = new_top->left;
  if (new_top->left != sentinel_) new_top->left->parent = pivot;
  new_top->parent = pivot->parent;
  if (pivot->parent == sentinel_) root_ = new_top;
  else if (pivot->parent->left == pivot) pivot->parent->left = new_top;
  else pivot->parent->right = new_top;
  new_top->left = pivot;
  pivot->parent = new_top;
}

void TimerTree::RightRotate(TimerTreeNode* pivot) {
  TimerTreeNode* new_top = pivot->left;
  pivot->left = new_top->right;
  if (new_top->right != sentinel_) new_top->right->parent = pivot;
  new_top->parent = pivot->parent;
  if (pivot->parent == sentinel_) root_ = new_top;
  else if (pivot->parent->right == pivot) pivot->parent->right = new_top;
  else pivot->parent->left = new_top;
  new_top->right = pivot;
  pivot->parent = new_top;
}

// -- insert and insert-fixup --

void TimerTree::InsertFixup(TimerTreeNode* node) {
  while (node != root_ && IsRed(node->parent)) {
    if (node->parent == node->parent->parent->left) {
      TimerTreeNode* uncle = node->parent->parent->right;
      if (IsRed(uncle)) {
        // Case 1: uncle is red — recolor and move up
        Black(node->parent);
        Black(uncle);
        Red(node->parent->parent);
        node = node->parent->parent;
      } else {
        if (node == node->parent->right) {
          // Case 2: node is a right child — rotate up to become left child
          node = node->parent;
          LeftRotate(node);
        }
        // Case 3: node is a left child — recolor and rotate grandparent
        Black(node->parent);
        Red(node->parent->parent);
        RightRotate(node->parent->parent);
      }
    } else {
      TimerTreeNode* uncle = node->parent->parent->left;
      if (IsRed(uncle)) {
        Black(node->parent);
        Black(uncle);
        Red(node->parent->parent);
        node = node->parent->parent;
      } else {
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

void TimerTree::Insert(Timer* timer) {
  TimerTreeNode* z = NodeOf(timer);
  if (z->in_tree) return;
  z->in_tree = true;
  z->timer_key = timer;
  z->left = sentinel_;
  z->right = sentinel_;
  Red(z);

  TimerTreeNode* y = sentinel_, *x = root_;
  while (x != sentinel_) {
    y = x;
    x = Less(timer, x->timer_key) ? x->left : x->right;
  }
  z->parent = y;
  if (y == sentinel_) root_ = z;
  else if (Less(timer, y->timer_key)) y->left = z;
  else y->right = z;
  
  ++size_;
  InsertFixup(z);
}
TimerTreeNode* TimerTree::Minimum(TimerTreeNode* node) { 
  while (node->left != Sentinel()) {
    node = node->left;
  }
  return node;
}

// -- Delete and Delete-Fixup ------

void TimerTree::DeleteFixup(TimerTreeNode* node) {
  while (node != root_ && IsBlack(node)) {
    TimerTreeNode* parent = node->parent;
    if (node == parent->left) {
      TimerTreeNode* sibling = parent->right;
      if (IsRed(sibling)) {
        // Case 1: sibling is red — rotate to get a black sibling
        Black(sibling);
        Red(parent);
        LeftRotate(parent);
        sibling = parent->right;
      }
      if (IsBlack(sibling->left) && IsBlack(sibling->right)) {
        // Case 2: sibling's children are both black — push deficit up
        Red(sibling);
        node = parent;
      } else {
        if (IsBlack(sibling->right)) {
          // Case 3: sibling's right child is black — rotate sibling to promote left child
          Black(sibling->left);
          Red(sibling);
          RightRotate(sibling);
          sibling = parent->right;
        }
        // Case 4: sibling's right child is red — rotate parent and recolor
        CopyColor(sibling, parent);
        Black(parent);
        Black(sibling->right);
        LeftRotate(parent);
        break;
      }
    } else {
      TimerTreeNode* sibling = parent->left;
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
          // Case 3: sibling's left child is black — rotate sibling to promote right child
          Black(sibling->right);
          Red(sibling);
          LeftRotate(sibling);
          sibling = parent->left;
        }
        // Case 4: sibling's left child is red — rotate parent and recolor
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
// Replace subtree rooted at u with subtree rooted at v.
void TimerTree::Transplant(TimerTreeNode* u, TimerTreeNode* v) {
  if (u->parent == sentinel_) root_ = v;
  else if (u == u->parent->left) u->parent->left = v;
  else u->parent->right = v;
  v->parent = u->parent;
}

bool TimerTree::Erase(Timer* timer) {
  TimerTreeNode* z = NodeOf(timer);
  if (!z->in_tree) return false;
  TimerTreeNode* y = z, *x;
  TimerTreeColor y_original_color = y->color;

  if (z->left == sentinel_) {
    x = z->right;
    Transplant(z, z->right);
  } else if (z->right == sentinel_) {
    x = z->left;
    Transplant(z, z->left);
  } else {
    y = Minimum(z->right);
    y_original_color = y->color;
    x = y->right;

    if (y->parent == z) {
      x->parent = y;
    } else {
      Transplant(y, y->right);
      y->right = z->right;
      y->right->parent = y;
    }
    Transplant(z, y);
    y->left = z->left;
    y->left->parent = y;
    y->color = z->color;
  }

  ResetNode(z);
  --size_;

  if (y_original_color == TimerTreeColor::kBlack) {
    DeleteFixup(x);
  }
  return true;
}

void TimerTree::ResetNode(TimerTreeNode* node) {
  node->left = sentinel_;
  node->right = sentinel_;
  node->parent = sentinel_;
  node->in_tree = false;
  node->color = TimerTreeColor::kBlack;
}
// find the next TimerTreeNode of node by in-order traversal
TimerTreeNode* TimerTree::Next(TimerTreeNode* node) {
  if (node->right != Sentinel()) {
    return Minimum(node->right);
  }
  while (true) {
    auto parent = node->parent;
    if (node == root_) {
      return nullptr;
    }
    if (node == parent->left) {
        return parent;
    }
    node = parent;
  }
}

// -- Bulk expiry --

std::vector<Timer*> TimerTree::PopExpired(runtime::time::Timestamp now) {
  std::vector<Timer*> expired;
  while (root_ != sentinel_) {
    auto t = Minimum(root_)->timer_key;
    if (t->Expiration() > now) break;
    expired.push_back(t);
    Erase(t);
  }
  return expired;
}

} // namespace runtime::net
