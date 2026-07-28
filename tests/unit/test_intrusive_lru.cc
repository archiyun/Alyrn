// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <cassert>
#include <cstdio>

#include "coropact/cache/intrusive_lru.h"
#include "coropact/ds/intrusive_list.h"

namespace {

struct GlobalTag {};
struct PeerTag {};

struct Item : coropact::ds::ListNode<Item, GlobalTag>,
              coropact::ds::ListNode<Item, PeerTag> {
  explicit Item(int value) : value(value) {}

  int value;
};

}  // namespace

int main() {
  using GlobalLru = coropact::cache::IntrusiveLRU<Item, GlobalTag>;
  using PeerList = coropact::ds::IntrusiveList<Item, PeerTag>;

  Item first(1);
  Item second(2);
  Item third(3);
  GlobalLru global;
  PeerList peer_a;
  PeerList peer_b;

  bool inserted = global.PushMRU(&first);
  assert(inserted);
  inserted = peer_a.PushBack(&first);
  assert(inserted);
  inserted = global.PushMRU(&second);
  assert(inserted);
  inserted = peer_a.PushBack(&second);
  assert(inserted);
  inserted = global.PushMRU(&third);
  assert(inserted);
  inserted = peer_b.PushBack(&third);
  assert(inserted);

  assert(global.Oldest() == &first);
  assert(global.Newest() == &third);

  // Acquiring from a peer removes the same object from the global LRU.
  Item* popped = peer_a.PopBack();
  assert(popped == &second);
  bool erased = global.Erase(&second);
  assert(erased);
  assert(global.Oldest() == &first);

  // Global eviction removes the corresponding peer entry as well.
  popped = global.PopLRU();
  assert(popped == &first);
  erased = peer_a.Erase(&first);
  assert(erased);
  popped = global.PopLRU();
  assert(popped == &third);
  erased = peer_b.Erase(&third);
  assert(erased);
  assert(global.Empty());
  assert(peer_a.Empty());
  assert(peer_b.Empty());

  // A removed node can be inserted again and touched to MRU.
  inserted = global.PushMRU(&first);
  assert(inserted);
  inserted = peer_a.PushBack(&first);
  assert(inserted);
  global.Touch(&first);
  assert(global.Newest() == &first);
  erased = global.Erase(&first);
  assert(erased);
  erased = peer_a.Erase(&first);
  assert(erased);

  std::puts("intrusive_lru_test passed");
  return 0;
}
