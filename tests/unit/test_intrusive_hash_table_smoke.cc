// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Oracle test for IntrusiveHashTable: drive random operations against the
// table and a std::unordered_map<int, Item*> reference in lockstep, then
// assert the two stay identical (size, membership, exact pointer identity,
// per-node InTable state). One run uses a deliberately colliding hash so that
// chain surgery (head/middle/tail unlink), rehash relinking and Clear() are
// all exercised on long multi-node buckets. Run under asan/ubsan to also
// catch dangling-hook bugs (stale pprev_ after Erase/Clear/Rehash).

#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <random>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "runtime/ds/intrusive_hash_table.h"

using runtime::ds::HashNode;
using runtime::ds::IntrusiveHashTable;

[[noreturn]] void Fail(const char* expression, int line) {
  std::fprintf(stderr, "FAIL line %d: %s\n", line, expression);
  std::abort();
}

#define CHECK(expression) ((expression) ? static_cast<void>(0) : Fail(#expression, __LINE__))

// Element with a stable identity (key) used to match nodes against the oracle.
struct Item : HashNode<Item> {
  int key = -1;
};

inline constexpr auto kKeyOf = [](const Item* it) { return it->key; };
using ItemTable = IntrusiveHashTable<Item, kKeyOf>;

static_assert(!std::is_copy_constructible_v<ItemTable>);
static_assert(!std::is_copy_assignable_v<ItemTable>);
static_assert(!std::is_move_constructible_v<ItemTable>);
static_assert(!std::is_move_assignable_v<ItemTable>);

// Deliberately bad hash: collapses everything onto 8 buckets regardless of
// table growth, forcing long chains.
struct Mod8Hash {
  std::size_t operator()(int k) const noexcept { return static_cast<std::size_t>(k) % 8; }
};

void EmptyReserveConstAndReuseTest() {
  Item item;
  item.key = 11;
  ItemTable table;
  const ItemTable& const_table = table;

  static_assert(std::is_same_v<decltype(const_table.Find(0)), const Item*>);

  CHECK(table.empty());
  CHECK(table.size() == 0);
  CHECK(table.bucket_count() == 0);
  CHECK(table.Find(11) == nullptr);
  CHECK(!table.Contains(11));
  CHECK(!table.Erase(&item));
  CHECK(table.CheckInvariants());

  table.Reserve(1);
  CHECK(table.bucket_count() == 16);
  table.Reserve(17);
  CHECK(table.bucket_count() == 32);
  table.Reserve(2);
  CHECK(table.bucket_count() == 32);
  CHECK(table.CheckInvariants());

  CHECK(table.Insert(&item));
  CHECK(const_table.Find(11) == &item);
  CHECK(const_table.Contains(11));
  table.Clear();
  CHECK(!item.InTable());
  CHECK(table.empty());
  CHECK(table.bucket_count() == 32);
  CHECK(table.CheckInvariants());

  CHECK(table.Insert(&item));
  CHECK(table.Erase(&item));
  CHECK(table.CheckInvariants());
}

void GrowthBoundaryTest() {
  std::vector<Item> pool(65);
  ItemTable table;

  for (int i = 0; i < static_cast<int>(pool.size()); ++i) {
    pool[i].key = i;
    CHECK(table.Insert(&pool[i]));
    if (i == 0) CHECK(table.bucket_count() == 16);
    if (i == 15) CHECK(table.bucket_count() == 16);
    if (i == 16) CHECK(table.bucket_count() == 32);
    if (i == 31) CHECK(table.bucket_count() == 32);
    if (i == 32) CHECK(table.bucket_count() == 64);
    if (i == 64) CHECK(table.bucket_count() == 128);
    CHECK(table.CheckInvariants());
  }

  for (int i = 0; i < static_cast<int>(pool.size()); ++i) {
    CHECK(table.Find(i) == &pool[i]);
  }
  table.Clear();
}

void DestructorUnlinksTest() {
  Item a;
  Item b;
  a.key = 1;
  b.key = 2;

  {
    ItemTable table;
    CHECK(table.Insert(&a));
    CHECK(table.Insert(&b));
    CHECK(a.InTable() && b.InTable());
  }

  CHECK(!a.InTable() && !b.InTable());

  ItemTable replacement;
  CHECK(replacement.Insert(&a));
  CHECK(replacement.Insert(&b));
  CHECK(replacement.CheckInvariants());
  replacement.Clear();
}

template <class Hash>
void RunOracle(unsigned seed, int pool_size, int steps) {
  // Nodes are owned by this pool; the table only borrows pointers into it.
  std::vector<Item> pool(pool_size);
  for (int i = 0; i < pool_size; ++i) pool[i].key = i;

  IntrusiveHashTable<Item, kKeyOf, Hash> table;
  std::unordered_map<int, Item*> oracle;  // reference model: key -> node
  std::mt19937 rng(seed);

  // Full equivalence check: same size, and for every pool node the same
  // membership, the same pointer from Find, and a matching hook state.
  auto check = [&] {
    CHECK(table.size() == oracle.size());
    CHECK(table.empty() == oracle.empty());
    CHECK(table.CheckInvariants());
    for (auto& it : pool) {
      auto found = oracle.find(it.key);
      Item* expect = (found == oracle.end()) ? nullptr : found->second;
      CHECK(table.Find(it.key) == expect);
      CHECK(table.Contains(it.key) == (expect != nullptr));
      CHECK(it.InTable() == (expect != nullptr));
    }
  };

  for (int step = 0; step < steps; ++step) {
    int op = static_cast<int>(rng() % 8);
    int id = static_cast<int>(rng() % static_cast<unsigned>(pool_size));
    Item* item = &pool[id];
    switch (op) {
      case 0:
      case 1:
      case 2: {  // Insert: must succeed iff the node is currently unlinked
        bool in = item->InTable();
        bool inserted = table.Insert(item);
        CHECK(inserted == !in);
        if (inserted) oracle.emplace(item->key, item);
        break;
      }
      case 3:
      case 4: {  // Erase(T*): return value must mirror the linked state
        bool in = item->InTable();
        bool erased = table.Erase(item);
        CHECK(erased == in);
        if (in) oracle.erase(item->key);
        break;
      }
      case 5: {  // Find on a random key (may miss)
        auto found = oracle.find(id);
        Item* expect = (found == oracle.end()) ? nullptr : found->second;
        CHECK(table.Find(id) == expect);
        break;
      }
      case 6: {  // Reserve mid-flight: must not lose or duplicate elements
        table.Reserve(rng() % 256);
        break;
      }
      case 7: {  // occasional Clear: every node must come back unhooked
        if (rng() % 16 == 0) {
          table.Clear();
          CHECK(table.empty() && table.size() == 0);
          CHECK(table.CheckInvariants());
          for (auto& [key, node] : oracle) {
            CHECK(!node->InTable());
            CHECK(table.Find(key) == nullptr);
          }
          oracle.clear();
        }
        break;
      }
    }
    if (step % 1000 == 0) check();  // periodic full comparison
  }
  check();  // final comparison after all operations
}

// Deterministic chain surgery in a single bucket: with Mod8Hash, keys 0/8/16
// always share a bucket. Unlink from the middle, the head and the tail of the
// chain, re-inserting between steps.
void ChainSurgeryTest() {
  IntrusiveHashTable<Item, kKeyOf, Mod8Hash> table;
  Item a, b, c;
  a.key = 0;
  b.key = 8;
  c.key = 16;

  CHECK(table.Insert(&a) && table.Insert(&b) && table.Insert(&c));
  CHECK(table.size() == 3);  // chain (head to tail): c -> b -> a
  CHECK(table.CheckInvariants());

  CHECK(table.Erase(&b));  // middle
  CHECK(table.Find(0) == &a && table.Find(16) == &c);
  CHECK(table.Find(8) == nullptr && !b.InTable());
  CHECK(table.CheckInvariants());

  CHECK(table.Insert(&b));
  CHECK(table.Erase(&b));  // head (most recently inserted)
  CHECK(table.Find(0) == &a && table.Find(16) == &c);
  CHECK(table.CheckInvariants());

  CHECK(table.Erase(&a));  // tail
  CHECK(table.Find(0) == nullptr && table.Find(16) == &c);
  CHECK(table.CheckInvariants());

  CHECK(table.Erase(&c));  // only remaining element
  CHECK(table.empty() && !a.InTable() && !b.InTable() && !c.InTable());
  CHECK(table.CheckInvariants());
}

// Documented multiset semantics: duplicate keys are the caller's problem.
// Without an intervening rehash, head insertion returns the newest match.
void DuplicateKeyTest() {
  IntrusiveHashTable<Item, kKeyOf> table;
  Item a, b;
  a.key = 7;
  b.key = 7;

  CHECK(table.Insert(&a));
  CHECK(table.Insert(&b));  // same key, distinct node: allowed
  CHECK(table.size() == 2);
  CHECK(table.Find(7) == &b);  // newest first
  CHECK(table.Erase(&b));
  CHECK(table.Find(7) == &a);
  CHECK(table.Erase(&a));
  CHECK(table.empty());

  // Re-inserting the same node twice must fail without touching size.
  CHECK(table.Insert(&a));
  CHECK(!table.Insert(&a));
  CHECK(table.size() == 1);
  CHECK(table.Erase(&a) && !table.Erase(&a));
  CHECK(table.CheckInvariants());
}

// One object living in two tables at once through distinct hook tags.
struct TagA {};
struct TagB {};
struct MultiItem : HashNode<MultiItem, TagA>, HashNode<MultiItem, TagB> {
  int key = -1;
};
inline constexpr auto kMultiKeyOf = [](const MultiItem* m) { return m->key; };

void MultiTagTest() {
  IntrusiveHashTable<MultiItem, kMultiKeyOf, std::hash<int>, std::equal_to<>, TagA> table_a;
  IntrusiveHashTable<MultiItem, kMultiKeyOf, std::hash<int>, std::equal_to<>, TagB> table_b;
  MultiItem m;
  m.key = 42;

  CHECK(table_a.Insert(&m) && table_b.Insert(&m));
  CHECK(table_a.Find(42) == &m && table_b.Find(42) == &m);
  CHECK(table_a.CheckInvariants() && table_b.CheckInvariants());

  CHECK(table_a.Erase(&m));  // hooks are independent per Tag
  CHECK(table_a.Find(42) == nullptr);
  CHECK(table_b.Find(42) == &m);
  CHECK(table_b.Erase(&m));
  CHECK(table_a.CheckInvariants() && table_b.CheckInvariants());
}

int main() {
  EmptyReserveConstAndReuseTest();
  GrowthBoundaryTest();
  DestructorUnlinksTest();
  ChainSurgeryTest();
  DuplicateKeyTest();
  MultiTagTest();

  // Colliding hash: long chains, chain-heavy Rehash/Clear.
  RunOracle<Mod8Hash>(12345, 256, 100000);
  // Realistic hash: spread-out buckets, repeated growth.
  RunOracle<std::hash<int>>(67890, 2000, 300000);

  printf("intrusive_hash_table smoke test passed\n");
  return 0;
}
