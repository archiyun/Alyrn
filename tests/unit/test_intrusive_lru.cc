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

  assert(global.PushMRU(&first));
  assert(peer_a.PushBack(&first));
  assert(global.PushMRU(&second));
  assert(peer_a.PushBack(&second));
  assert(global.PushMRU(&third));
  assert(peer_b.PushBack(&third));

  assert(global.Oldest() == &first);
  assert(global.Newest() == &third);

  // Acquiring from a peer removes the same object from the global LRU.
  assert(peer_a.PopBack() == &second);
  assert(global.Erase(&second));
  assert(global.Oldest() == &first);

  // Global eviction removes the corresponding peer entry as well.
  assert(global.PopLRU() == &first);
  assert(peer_a.Erase(&first));
  assert(global.PopLRU() == &third);
  assert(peer_b.Erase(&third));
  assert(global.Empty());
  assert(peer_a.empty());
  assert(peer_b.empty());

  // A removed node can be inserted again and touched to MRU.
  assert(global.PushMRU(&first));
  assert(peer_a.PushBack(&first));
  global.Touch(&first);
  assert(global.Newest() == &first);
  assert(global.Erase(&first));
  assert(peer_a.Erase(&first));

  std::puts("intrusive_lru_test passed");
  return 0;
}
