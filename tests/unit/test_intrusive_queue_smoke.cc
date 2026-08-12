// SPDX-License-Identifier: MIT

#include <cstdio>
#include <utility>

#include "coropact/ds/intrusive_queue.h"

namespace {

struct Item : coropact::ds::QueueNode<Item> {
  explicit Item(int value) : value(value) {}

  int value;
};

}  // namespace

int main() {
  const auto require = [](bool condition, const char* message) {
    if (!condition) {
      std::fprintf(stderr, "FAIL: %s\n", message);
    }
    return condition;
  };

  Item one(1);
  Item two(2);
  Item three(3);
  Item four(4);

  coropact::ds::IntrusiveQueue<Item> queue;
  if (!require(queue.Empty(), "new queue must be empty")) return 1;
  if (!require(!queue.PushBack(nullptr), "PushBack(nullptr) must fail without mutation") ||
      !require(!queue.PushFront(nullptr), "PushFront(nullptr) must fail without mutation")) {
    return 1;
  }
  if (!require(queue.PushBack(&two), "PushBack must link an unlinked node") ||
      !require(queue.PushFront(&one), "PushFront must link an unlinked node") ||
      !require(queue.PushBack(&three), "PushBack must append an unlinked node") ||
      !require(!queue.PushBack(&three), "PushBack must reject a linked node") ||
      !require(queue.Size() == 3, "queue size must match linked nodes") ||
      !require(queue.Front() == &one, "queue front must preserve insertion order") ||
      !require(queue.Back() == &three, "queue back must preserve insertion order")) {
    return 1;
  }

  queue.ForEachSafe([](Item& item) { return item.value % 2 == 0; });
  if (!require(!two.InQueue(), "ForEachSafe must unlink removed nodes") ||
      !require(queue.Size() == 2, "ForEachSafe must update queue size") ||
      !require(queue.Front() == &one, "ForEachSafe must preserve front") ||
      !require(queue.Back() == &three, "ForEachSafe must preserve back")) {
    return 1;
  }

  coropact::ds::IntrusiveQueue<Item> moved(std::move(queue));
  if (!require(queue.Empty(), "move construction must empty source queue") ||
      !require(moved.Front() == &one, "move construction must preserve front") ||
      !require(moved.Back() == &three, "move construction must preserve back")) {
    return 1;
  }

  coropact::ds::IntrusiveQueue<Item> suffix;
  if (!require(suffix.PushBack(&four), "suffix queue must accept unlinked node")) return 1;
  moved.Splice(suffix);
  if (!require(suffix.Empty(), "Splice must empty source queue") ||
      !require(moved.Size() == 3, "Splice must transfer all nodes") ||
      !require(moved.Back() == &four, "Splice must append source nodes")) {
    return 1;
  }
  moved.Splice(moved);
  if (!require(moved.Size() == 3, "self splice must preserve queue size") ||
      !require(moved.Front() == &one, "self splice must preserve queue front") ||
      !require(moved.Back() == &four, "self splice must preserve queue back")) {
    return 1;
  }

  coropact::ds::IntrusiveQueue<Item> assigned;
  assigned = std::move(moved);
  if (!require(moved.Empty(), "move assignment must empty source queue") ||
      !require(assigned.Size() == 3, "move assignment must preserve queue size") ||
      !require(assigned.PopFront() == &one, "PopFront must return first node") ||
      !require(assigned.PopFront() == &three, "PopFront must return second node") ||
      !require(assigned.PopFront() == &four, "PopFront must return third node") ||
      !require(assigned.PopFront() == nullptr, "PopFront must return null for empty queue") ||
      !require(assigned.Empty(), "drained queue must be empty") ||
      !require(assigned.Front() == nullptr, "empty queue front must be null") ||
      !require(assigned.Back() == nullptr, "empty queue back must be null")) {
    return 1;
  }

  return 0;
}
