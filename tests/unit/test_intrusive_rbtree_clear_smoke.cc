#include <array>
#include <cstddef>
#include <iostream>
#include <vector>

#include "coropact/ds/intrusive_rbtree.h"

namespace {

struct Item : coropact::ds::RBTNode<Item> {
  int key{0};
  int id{0};
};

bool ItemLess(const Item* a, const Item* b) {
  if (a->key != b->key) return a->key < b->key;
  return a->id < b->id;
}

using ItemTree = coropact::ds::IntrusiveRBTree<Item, ItemLess>;

bool Expect(bool condition, const char* message) {
  if (!condition) {
    std::cerr << "[FAIL] " << message << '\n';
    return false;
  }
  return true;
}

bool ExpectEmpty(const ItemTree& tree, const char* context) {
  return Expect(tree.empty(), context) && Expect(tree.size() == 0, "size should be zero") &&
         Expect(tree.earliest() == nullptr, "earliest should be null") &&
         Expect(tree.CheckRBInvariants(), "empty tree invariants should hold");
}

bool TestClearEmptyTree() {
  ItemTree tree;
  tree.Clear();
  return ExpectEmpty(tree, "empty Clear should keep tree empty");
}

bool TestClearSingleNode() {
  Item item;
  item.key = 10;
  item.id = 1;
  ItemTree tree;

  tree.Insert(&item);
  tree.Clear();
  if (!ExpectEmpty(tree, "single-node Clear should empty tree") ||
      !Expect(!item.InTree(), "single-node Clear should unlink item")) {
    return false;
  }

  return Expect(tree.Insert(&item), "cleared single node should be reusable") &&
         Expect(tree.Erase(&item), "reinserted single node should erase");
}

bool TestClearManyNodes() {
  static constexpr std::array<int, 31> kInsertOrder = {15, 7,  23, 3,  11, 19, 27, 1,  5, 9,  13,
                                                       17, 21, 25, 29, 0,  2,  4,  6,  8, 10, 12,
                                                       14, 16, 18, 20, 22, 24, 26, 28, 30};

  std::vector<Item> items(kInsertOrder.size());
  ItemTree tree;

  for (std::size_t i = 0; i < items.size(); ++i) {
    items[i].key = kInsertOrder[i];
    items[i].id = static_cast<int>(i);
    tree.Insert(&items[i]);
  }

  if (!Expect(tree.size() == items.size(), "all items should be inserted") ||
      !Expect(tree.CheckRBInvariants(), "tree invariants should hold before Clear")) {
    return false;
  }

  tree.Clear();
  if (!ExpectEmpty(tree, "multi-node Clear should empty tree")) return false;

  for (const Item& item : items) {
    if (!Expect(!item.InTree(), "Clear should unlink every node")) return false;
  }

  for (auto it = items.rbegin(); it != items.rend(); ++it) {
    if (!Expect(tree.Insert(&*it), "cleared node should reinsert")) return false;
  }

  if (!Expect(tree.size() == items.size(), "all cleared items should reinsert") ||
      !Expect(tree.CheckRBInvariants(), "tree invariants should hold after reinsertion")) {
    return false;
  }

  tree.Clear();
  for (const Item& item : items) {
    if (!Expect(!item.InTree(), "second Clear should unlink every node")) return false;
  }
  return ExpectEmpty(tree, "second multi-node Clear should empty tree");
}

}  // namespace

int main() {
  if (!TestClearEmptyTree()) return 1;
  if (!TestClearSingleNode()) return 1;
  if (!TestClearManyNodes()) return 1;

  std::cout << "[PASS] intrusive_rbtree_clear_smoke_test\n";
  return 0;
}
