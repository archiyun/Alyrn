// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
// Reference:
//   - Linux kernel hlist implementation by Linus Torvalds,
//     "include/linux/types.h" (hlist_head / hlist_node) and
//     "include/linux/list.h".
//     -> hlist: single-pointer bucket heads, pprev (pointer-to-pointer) unlink.
#pragma once

#include <bit>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <functional>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

#include "coropact/utils/macros.h"

namespace coropact::ds {

// Intrusive chained hash table. Node storage lives in T through a base hook:
// T must publicly inherit HashNode<T, Tag>, so the table can recover T* from
// a node with static_cast.
//
// Buckets follow the kernel hlist layout: a bucket head is a single Node*,
// and every node carries pprev_, the address of whatever points at it (the
// bucket slot or the previous node's next_). Erase therefore unlinks in O(1)
// without ever touching buckets_, at the cost of no backward traversal.
//
// Template parameters:
//   T      - element type; must publicly and non-virtually inherit
//            HashNode<T, Tag>
//   kKeyOf - key projection, invocable on const T*; must be pure
//   Hash   - hash functor over the key type; must be stateless (a fresh
//            Hash{} is constructed per call)
//   Eq     - key equality; must be stateless, transparent by default
//   Tag    - optional hook tag for objects living in multiple tables
//
// Semantics and caveats:
//   - Keys are NOT deduplicated (kernel-hlist semantics): two distinct
//     objects with equal keys may both be inserted. Find returns the first
//     matching node in its bucket; a rehash may reorder equal-key nodes.
//     Callers needing uniqueness must Find first.
//   - The table never owns elements; a node must outlive its membership.
//   - Bucket count is always a power of two; growth doubles at load factor 1.
//   - Rehash first computes every destination bucket before it relinks any
//     hook, so allocation, hash, or key-projection exceptions leave the table
//     unchanged.
template <typename T, auto kKeyOf,
          class Hash =
              std::hash<std::remove_cvref_t<decltype(kKeyOf(static_cast<const T*>(nullptr)))>>,
          class Eq = std::equal_to<>, class Tag = void>
class IntrusiveHashTable;

template <typename T, class Tag = void>
class HashNode {
public:
  template <typename, auto, class, class, class>
  friend class IntrusiveHashTable;

  // Only says "linked into some table", not which one.
  [[nodiscard]]
  bool InTable() const noexcept {
    return pprev_ != nullptr;
  }

protected:
  COROPACT_DELETE_COPY(HashNode);
  HashNode() = default;
  ~HashNode() = default;

private:
  using Node = HashNode<T, Tag>;

  Node* next_{nullptr};
  Node** pprev_{nullptr};  // address of the pointer that points at this node

  void clear_hook() noexcept {
    next_ = nullptr;
    pprev_ = nullptr;
  }
};

template <class T, class Tag = void>
concept HashNodeBaseHook =
    std::derived_from<T, HashNode<T, Tag>> && requires(T* elem, HashNode<T, Tag>* node) {
      { static_cast<HashNode<T, Tag>*>(elem) } -> std::same_as<HashNode<T, Tag>*>;
      { static_cast<T*>(node) } -> std::same_as<T*>;
    };

template <class T, auto kKeyOf, class Hash, class Eq, class Tag>
class IntrusiveHashTable {
public:
  COROPACT_DELETE_COPY_MOVE(IntrusiveHashTable);

  using Node = HashNode<T, Tag>;
  using Key = std::remove_cvref_t<decltype(kKeyOf(static_cast<const T*>(nullptr)))>;
  static_assert(HashNodeBaseHook<T, Tag>,
                "T must publicly and non-virtually inherit HashNode<T, Tag>");

  IntrusiveHashTable() = default;
  ~IntrusiveHashTable() noexcept { Clear(); }

  [[nodiscard]]
  bool Empty() const noexcept {
    return size_ == 0;
  }

  [[nodiscard]]
  std::size_t Size() const noexcept {
    return size_;
  }

  [[nodiscard]]
  std::size_t BucketCount() const noexcept {
    return buckets_.size();
  }

  // Returns false for nullptr or when elem is already linked.
  [[nodiscard]]
  bool Insert(T* elem);

  // Returns false for nullptr or if elem is not linked.
  //
  // Precondition: if elem is linked, it must be linked in this exact table.
  // InTable() only reports whether the hook is linked somewhere.
  [[nodiscard]]
  bool Erase(T* elem);

  [[nodiscard]]
  T* Find(const Key& key) noexcept;

  [[nodiscard]]
  const T* Find(const Key& key) const noexcept;

  [[nodiscard]]
  bool Contains(const Key& key) const {
    return Find(key) != nullptr;
  }

  void Reserve(std::size_t n);

  void Clear();

  // O(n) - debug only. Verifies bucket placement, pprev links, hook state,
  // element count and the power-of-two bucket invariant.
  [[nodiscard]]
  bool CheckInvariants() const;

private:
  static constexpr std::size_t kMinBuckets = 16;  // 2^4
  static constexpr std::size_t kMaxBucketCount =
      std::size_t{1} << (std::numeric_limits<std::size_t>::digits - 1);

  // Node pprev_ pointers alias the slots of this vector: any reallocation
  // outside Rehash() (which relinks every node) would leave them dangling.
  std::vector<Node*> buckets_;
  std::size_t size_{0};

  static Node* node_of(T* elem) { return static_cast<Node*>(elem); }
  static T* elem_of(Node* node) { return static_cast<T*>(node); }
  static const T* elem_of(const Node* node) { return static_cast<const T*>(node); }

  [[nodiscard]]
  std::size_t BucketIdx(std::size_t h) const {
    assert(std::has_single_bit(buckets_.size()));
    return h & (buckets_.size() - 1);
  }

  void InsertHead(Node** head, Node* node);
  void Rehash(std::size_t new_bucket_count);
};

#define IHT_TMPL template <class T, auto kKeyOf, class Hash, class Eq, class Tag>
#define IHT_TYPE IntrusiveHashTable<T, kKeyOf, Hash, Eq, Tag>

IHT_TMPL
void IHT_TYPE::InsertHead(Node** head, Node* node) {
  Node* first = *head;
  node->next_ = first;
  if (first != nullptr) {
    first->pprev_ = &node->next_;
  }
  *head = node;
  node->pprev_ = head;
}

IHT_TMPL
bool IHT_TYPE::Insert(T* elem) {
  if (elem == nullptr) return false;
  Node* node = node_of(elem);
  if (node->InTable()) return false;
  if (size_ >= buckets_.size()) {
    if (buckets_.empty()) {
      Rehash(kMinBuckets);
    } else {
      Reserve(buckets_.size() + 1);
    }
  }
  InsertHead(&buckets_[BucketIdx(Hash{}(kKeyOf(elem)))], node);
  ++size_;
  return true;
}

IHT_TMPL
bool IHT_TYPE::Erase(T* elem) {
  if (elem == nullptr) return false;
  Node* node = node_of(elem);
  if (!node->InTable()) return false;
  *(node->pprev_) = node->next_;
  if (node->next_ != nullptr) {
    node->next_->pprev_ = node->pprev_;
  }
  node->clear_hook();
  --size_;
  return true;
}

IHT_TMPL
T* IHT_TYPE::Find(const Key& key) noexcept {
  return const_cast<T*>(std::as_const(*this).Find(key));
}

IHT_TMPL
const T* IHT_TYPE::Find(const Key& key) const noexcept {
  if (buckets_.empty()) return nullptr;
  for (const Node* n = buckets_[BucketIdx(Hash{}(key))]; n != nullptr; n = n->next_) {
    if (Eq{}(kKeyOf(elem_of(n)), key)) {
      return const_cast<const T*>(elem_of(n));
    }
  }
  return nullptr;
}

IHT_TMPL
void IHT_TYPE::Clear() {
  for (Node*& head : buckets_) {
    for (Node* n = head; n != nullptr;) {
      Node* next = n->next_;
      n->clear_hook();
      n = next;
    }
    head = nullptr;
  }
  size_ = 0;
}

IHT_TMPL
void IHT_TYPE::Reserve(std::size_t n) {
  const std::size_t requested = n < kMinBuckets ? kMinBuckets : n;
  if (requested > kMaxBucketCount) {
    throw std::length_error("IntrusiveHashTable bucket count is not representable");
  }
  const std::size_t want = std::bit_ceil(requested);
  if (want > buckets_.size()) {
    Rehash(want);
  }
}

IHT_TMPL
void IHT_TYPE::Rehash(std::size_t new_bucket_count) {
  assert(std::has_single_bit(new_bucket_count));
  assert(new_bucket_count >= kMinBuckets);

  std::vector<Node*> fresh(new_bucket_count, nullptr);
  std::vector<std::size_t> destinations;
  destinations.reserve(size_);
  const std::size_t mask = new_bucket_count - 1;

  // Hashing and key projection may throw. Do not modify any hook until every
  // destination is known and all fallible allocations have succeeded.
  for (Node* head : buckets_) {
    for (Node* node = head; node != nullptr; node = node->next_) {
      destinations.push_back(Hash{}(kKeyOf(elem_of(node))) & mask);
    }
  }

  std::size_t destination = 0;
  for (Node* head : buckets_) {
    for (Node* node = head; node != nullptr;) {
      Node* next = node->next_;
      InsertHead(&fresh[destinations[destination]], node);
      ++destination;
      node = next;
    }
  }
  assert(destination == size_);
  buckets_ = std::move(fresh);
}

IHT_TMPL
bool IHT_TYPE::CheckInvariants() const {
  if (buckets_.empty()) return size_ == 0;
  if (buckets_.size() < kMinBuckets || !std::has_single_bit(buckets_.size())) {
    return false;
  }

  std::size_t count = 0;
  for (std::size_t bucket = 0; bucket < buckets_.size(); ++bucket) {
    const Node* previous = nullptr;
    for (const Node* node = buckets_[bucket]; node != nullptr; node = node->next_) {
      if (!node->InTable()) return false;

      const void* expected_pprev = previous == nullptr ? static_cast<const void*>(&buckets_[bucket])
                                                       : static_cast<const void*>(&previous->next_);
      if (static_cast<const void*>(node->pprev_) != expected_pprev) {
        return false;
      }
      if (BucketIdx(Hash{}(kKeyOf(elem_of(node)))) != bucket) {
        return false;
      }
      if (++count > size_) return false;
      previous = node;
    }
  }
  return count == size_;
}

#undef IHT_TMPL
#undef IHT_TYPE

}  // namespace coropact::ds
