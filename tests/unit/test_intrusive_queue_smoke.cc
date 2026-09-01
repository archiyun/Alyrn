// SPDX-License-Identifier: MIT

#include <cstdio>

#include "alyrn/detail/intrusive_queue.h"

namespace {

struct Item : alyrn::detail::QueueNode<Item> {
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

  Item two(2);
  Item three(3);
  Item four(4);

  alyrn::detail::IntrusiveQueue<Item> queue;
  if (!require(queue.Empty(), "new queue must be empty")) return 1;
  if (!require(!queue.PushBack(nullptr), "PushBack(nullptr) must fail without mutation")) return 1;
  if (!require(queue.PushBack(&two), "PushBack must link an unlinked node") ||
      !require(queue.PushBack(&three), "PushBack must append an unlinked node") ||
      !require(!queue.PushBack(&three), "PushBack must reject a linked node") ||
      !require(queue.Front() == &two, "queue front must preserve insertion order")) {
    return 1;
  }

  queue.ForEachSafe([](Item& item) { return item.value % 2 == 0; });
  if (!require(!two.InQueue(), "ForEachSafe must unlink removed nodes") ||
      !require(queue.Front() == &three, "ForEachSafe must preserve front")) {
    return 1;
  }

  alyrn::detail::IntrusiveQueue<Item> suffix;
  if (!require(suffix.PushBack(&four), "suffix queue must accept unlinked node")) return 1;
  queue.Splice(suffix);
  if (!require(suffix.Empty(), "Splice must empty source queue") ||
      !require(queue.Front() == &three, "Splice must preserve queue front")) {
    return 1;
  }
  queue.Splice(queue);
  if (!require(queue.Front() == &three, "self splice must preserve queue front")) {
    return 1;
  }

  if (!require(queue.PopFront() == &three, "PopFront must return first node") ||
      !require(queue.PopFront() == &four, "PopFront must return third node") ||
      !require(queue.PopFront() == nullptr, "PopFront must return null for empty queue") ||
      !require(queue.Empty(), "drained queue must be empty") ||
      !require(queue.Front() == nullptr, "empty queue front must be null")) {
    return 1;
  }

  return 0;
}
