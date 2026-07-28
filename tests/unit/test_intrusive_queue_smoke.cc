// Copyright (c) 2026 Arsenova.
// SPDX-License-Identifier: MIT

#include <cassert>
#include <utility>

#include "coropact/ds/intrusive_queue.h"

namespace {

struct Item : coropact::ds::QueueNode<Item> {
  explicit Item(int value) : value(value) {}

  int value;
};

}  // namespace

int main() {
  Item one(1);
  Item two(2);
  Item three(3);
  Item four(4);

  coropact::ds::IntrusiveQueue<Item> queue;
  assert(queue.Empty());
  assert(queue.PushBack(&two));
  assert(queue.PushFront(&one));
  assert(queue.PushBack(&three));
  assert(!queue.PushBack(&three));
  assert(queue.Size() == 3);
  assert(queue.Front() == &one);
  assert(queue.Back() == &three);

  queue.ForEachSafe([](Item& item) { return item.value % 2 == 0; });
  assert(!two.InQueue());
  assert(queue.Size() == 2);
  assert(queue.Front() == &one);
  assert(queue.Back() == &three);

  coropact::ds::IntrusiveQueue<Item> moved(std::move(queue));
  assert(queue.Empty());
  assert(moved.Front() == &one);
  assert(moved.Back() == &three);

  coropact::ds::IntrusiveQueue<Item> suffix;
  assert(suffix.PushBack(&four));
  moved.Splice(suffix);
  assert(suffix.Empty());
  assert(moved.Size() == 3);
  assert(moved.Back() == &four);

  coropact::ds::IntrusiveQueue<Item> assigned;
  assigned = std::move(moved);
  assert(moved.Empty());
  assert(assigned.Size() == 3);
  assert(assigned.PopFront() == &one);
  assert(assigned.PopFront() == &three);
  assert(assigned.PopFront() == &four);
  assert(assigned.PopFront() == nullptr);
  assert(assigned.Empty());
  assert(assigned.Front() == nullptr);
  assert(assigned.Back() == nullptr);

  return 0;
}
