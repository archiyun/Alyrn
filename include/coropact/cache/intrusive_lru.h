// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>

#include "coropact/ds/intrusive_list.h"
#include "coropact/utils/macros.h"

namespace coropact::cache {

// IntrusiveLRU provides LRU ordering for externally owned nodes. It does not
// allocate, destroy, lock, or evict nodes on behalf of the caller. The caller
// owns the node lifetime and must remove a node before destroying it.
//
// Nodes are inserted at the back (most recently used) and removed from the
// front (least recently used). The Tag lets one object participate in more
// than one intrusive list, for example a global LRU and a per-peer queue.
template <typename T, class Tag = void>
class IntrusiveLRU {
public:
  COROPACT_DELETE_COPY_MOVE(IntrusiveLRU);

  IntrusiveLRU() = default;

  [[nodiscard]] bool Empty() const noexcept { return list_.empty(); }
  [[nodiscard]] std::size_t Size() const noexcept { return list_.size(); }

  // Returns false for nullptr or when the node is already linked through this
  // hook. IntrusiveList intentionally does not own the node.
  bool PushMRU(T* node) {
    if (node == nullptr) return false;
    return list_.PushBack(node);
  }

  // Removes and returns the least recently used node.
  T* PopLRU() { return list_.PopFront(); }

  [[nodiscard]] T* Oldest() noexcept { return list_.front(); }
  [[nodiscard]] const T* Oldest() const noexcept { return list_.front(); }
  [[nodiscard]] T* Newest() noexcept { return list_.back(); }
  [[nodiscard]] const T* Newest() const noexcept { return list_.back(); }

  // Moves an existing node to the most recently used position.
  // Precondition: node is linked in this exact LRU.
  void Touch(T* node) { list_.MoveToBack(node); }

  // Removes a node without destroying it.
  // Precondition: if linked, node is linked in this exact LRU.
  bool Erase(T* node) { return node != nullptr && list_.Erase(node); }

  // Unlinks every node. The caller remains responsible for destroying them.
  void Clear() { list_.Clear(); }

private:
  coropact::ds::IntrusiveList<T, Tag> list_;
};

}  // namespace coropact::cache
