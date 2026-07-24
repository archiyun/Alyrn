// Copyright (c) 2026 RomenJens. All Rights Reserved.
// SPDX-License-Identifier: MIT
//
// Oracle test for IntrusiveList: drive random operations against the list and a
// std::list<int> reference in lockstep, then assert the two stay identical.
// std::list is the source of truth; any divergence in order, size, or endpoints
// fails the test. Run under asan/ubsan to also catch dangling-link bugs.

#include <cassert>
#include <cstdio>
#include <list>
#include <random>
#include <utility>
#include <vector>

#include "coropact/ds/intrusive_list.h"

using coropact::ds::IntrusiveList;
using coropact::ds::ListNode;

// Element with a stable identity (id) used to match nodes against oracle values.
struct Item : ListNode<Item> {
  Item() = default;

  int id;
};

int main() {
  {
    Item first{};
    Item second{};
    Item third{};
    Item fourth{};
    first.id = 1;
    second.id = 2;
    third.id = 3;
    fourth.id = 4;

    IntrusiveList<Item> list;
    IntrusiveList<Item> other;
    assert(!list.PushBack(nullptr));
    assert(list.PushBack(&first));
    assert(!list.PushBack(&first));
    assert(list.InsertBefore(&first, &second));
    assert(list.InsertAfter(&first, &third));

    const auto& view = list;
    assert(view.front() == &second);
    assert(view.back() == &third);
    int const_sum = 0;
    for (const Item& item : view) {
      const_sum += item.id;
    }
    assert(const_sum == 6);

    auto it = list.cbegin();
    ++it;
    list.Erase(it);
    assert(!first.InList());
    assert(list.size() == 2);

    assert(list.RemoveIf([](Item& item) { return item.id == 2; }) == 1);
    assert(!second.InList());
    assert(list.front() == &third);

    assert(other.PushBack(&fourth));
    list.Swap(other);
    assert(list.front() == &fourth);
    assert(other.front() == &third);
    assert(list.size() == 1);
    assert(other.size() == 1);

    swap(list, other);
    assert(list.front() == &third);
    assert(other.front() == &fourth);
  }

  {
    Item one{};
    Item two{};
    Item replacement{};
    one.id = 1;
    two.id = 2;
    replacement.id = 3;

    IntrusiveList<Item> source;
    assert(source.PushBack(&one));
    assert(source.PushBack(&two));

    IntrusiveList<Item> moved(std::move(source));
    assert(source.empty());
    assert(moved.size() == 2);
    assert(moved.front() == &one);
    assert(moved.back() == &two);

    assert(source.PushBack(&replacement));
    moved = std::move(source);
    assert(source.empty());
    assert(moved.size() == 1);
    assert(moved.front() == &replacement);
    assert(moved.back() == &replacement);
    assert(!one.InList());
    assert(!two.InList());

    moved = std::move(moved);
    assert(moved.size() == 1);
    assert(moved.front() == &replacement);

    IntrusiveList<Item> empty;
    moved = std::move(empty);
    assert(moved.empty());
    assert(!replacement.InList());
  }

  constexpr int N = 2000;        // pool size; ids are unique so oracle.remove(id) is exact
  constexpr int times = 500000;  // number of random operations

  // Nodes are owned by this pool; the list only borrows pointers into it.
  std::vector<Item> pool(N);
  for (int i = 0; i < N; ++i) pool[i].id = i;

  IntrusiveList<Item> il;
  std::list<int> oracle;  // reference model: holds the ids in the same order
  std::mt19937 rng(12345);

  // Full equivalence check: same size, same forward order, same endpoints.
  auto check = [&] {
    assert(il.size() == oracle.size());
    auto oit = oracle.begin();
    for (auto& x : il) {
      assert(x.id == *oit);
      ++oit;
    }
    assert(oit == oracle.end());
    if (oracle.empty()) {
      assert(il.empty() && il.front() == nullptr && il.back() == nullptr);
    } else {
      assert(il.front()->id == oracle.front());
      assert(il.back()->id == oracle.back());
    }
  };

  for (int step = 0; step < times; ++step) {
    int op = rng() % 7;
    switch (op) {
      case 0: {  // PushFront: only link nodes not already in the list (idempotent)
        int id = rng() % N;
        if (!pool[id].InList()) {
          assert(il.PushFront(&pool[id]));
          oracle.push_front(id);
        }
        break;
      }
      case 1: {  // PushBack
        int id = rng() % N;
        if (!pool[id].InList()) {
          assert(il.PushBack(&pool[id]));
          oracle.push_back(id);
        }
        break;
      }
      case 2: {  // Erase(T*): return value must reflect whether the node was linked
        int id = rng() % N;
        bool in = pool[id].InList();
        bool erased = il.Erase(&pool[id]);
        assert(erased == in);
        if (in) oracle.remove(id);
        break;
      }
      case 3: {  // PopFront: nullptr iff empty, else matches the oracle's front
        Item* f = il.PopFront();
        if (oracle.empty()) {
          assert(f == nullptr);
        } else {
          assert(f && f->id == oracle.front());
          oracle.pop_front();
        }
        break;
      }
      case 4: {  // PopBack
        Item* b = il.PopBack();
        if (oracle.empty()) {
          assert(b == nullptr);
        } else {
          assert(b && b->id == oracle.back());
          oracle.pop_back();
        }
        break;
      }
      case 5: {  // ForEachSafe: erase even ids mid-traversal; mirror on the oracle
        il.ForEachSafe([](Item& x) { return x.id % 2 == 0; });
        for (auto it = oracle.begin(); it != oracle.end();) {
          if (*it % 2 == 0)
            it = oracle.erase(it);
          else
            ++it;
        }
        break;
      }
      case 6: {  // MoveToFront: relink an existing node to the front (size unchanged)
        int id = rng() % N;
        if (pool[id].InList()) {
          il.MoveToFront(&pool[id]);
          oracle.remove(id);
          oracle.push_front(id);
        }
        break;
      }
    }
    if (step % 1000 == 0) check();  // periodic full comparison
  }
  check();  // final comparison after all operations
  printf("intrusive_list oracle test passed\n");
  return 0;
}
