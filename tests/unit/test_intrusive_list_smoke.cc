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

#include "alyrn/detail/ds/intrusive_list.h"

using alyrn::detail::ds::IntrusiveList;
using alyrn::detail::ds::ListNode;

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
    bool inserted = list.PushBack(nullptr);
    assert(!inserted);
    inserted = list.PushBack(&first);
    assert(inserted);
    inserted = list.PushBack(&first);
    assert(!inserted);
    inserted = list.PushFront(&second);
    assert(inserted);
    inserted = list.PushBack(&third);
    assert(inserted);

    const auto& view = list;
    assert(view.Front() == &second);
    assert(view.Back() == &third);
    int const_sum = 0;
    for (const Item& item : view) {
      const_sum += item.id;
    }
    assert(const_sum == 6);

    bool erased = list.Erase(&second);
    assert(erased);
    assert(!second.InList());
    assert(list.Front() == &first);

    inserted = other.PushBack(&fourth);
    assert(inserted);
    list.Splice(other);
    assert(list.Front() == &first);
    assert(list.Back() == &fourth);
    assert(other.Empty());
  }

  {
    Item one{};
    Item two{};
    Item replacement{};
    one.id = 1;
    two.id = 2;
    replacement.id = 3;

    IntrusiveList<Item> source;
    bool inserted = source.PushBack(&one);
    assert(inserted);
    inserted = source.PushBack(&two);
    assert(inserted);

    IntrusiveList<Item> moved(std::move(source));
    assert(source.Empty());
    assert(moved.Front() == &one);
    assert(moved.Back() == &two);

    inserted = source.PushBack(&replacement);
    assert(inserted);
    moved = std::move(source);
    assert(source.Empty());
    assert(moved.Front() == &replacement);
    assert(moved.Back() == &replacement);
    assert(!one.InList());
    assert(!two.InList());

    // Keep the self-move contract test while avoiding a compiler diagnostic
    // intended for accidental production self-assignment.
    IntrusiveList<Item>* volatile self = &moved;
    moved = std::move(*self);
    assert(moved.Front() == &replacement);

    IntrusiveList<Item> empty;
    moved = std::move(empty);
    assert(moved.Empty());
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
    auto oit = oracle.begin();
    for (auto& x : il) {
      assert(x.id == *oit);
      ++oit;
    }
    assert(oit == oracle.end());
    if (oracle.empty()) {
      assert(il.Empty() && il.Front() == nullptr && il.Back() == nullptr);
    } else {
      assert(il.Front()->id == oracle.front());
      assert(il.Back()->id == oracle.back());
    }
  };

  for (int step = 0; step < times; ++step) {
    int op = rng() % 6;
    switch (op) {
      case 0: {  // PushFront: only link nodes not already in the list (idempotent)
        int id = rng() % N;
        if (!pool[id].InList()) {
          bool inserted = il.PushFront(&pool[id]);
          assert(inserted);
          oracle.push_front(id);
        }
        break;
      }
      case 1: {  // PushBack
        int id = rng() % N;
        if (!pool[id].InList()) {
          bool inserted = il.PushBack(&pool[id]);
          assert(inserted);
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
      case 4: {  // ForEachSafe: erase even ids mid-traversal; mirror on the oracle
        il.ForEachSafe([](Item& x) { return x.id % 2 == 0; });
        for (auto it = oracle.begin(); it != oracle.end();) {
          if (*it % 2 == 0)
            it = oracle.erase(it);
          else
            ++it;
        }
        break;
      }
      case 5: {  // Keep the random operation distribution stable.
        int id = rng() % N;
        bool in = pool[id].InList();
        bool erased = il.Erase(&pool[id]);
        assert(erased == in);
        if (in) oracle.remove(id);
        break;
      }
    }
    if (step % 1000 == 0) check();  // periodic full comparison
  }
  check();  // final comparison after all operations
  printf("intrusive_list oracle test passed\n");
  return 0;
}
