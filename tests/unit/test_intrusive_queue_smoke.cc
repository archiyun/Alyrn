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
  assert(queue.empty());
  assert(queue.PushBack(&two));
  assert(queue.PushFront(&one));
  assert(queue.PushBack(&three));
  assert(!queue.PushBack(&three));
  assert(queue.size() == 3);
  assert(queue.front() == &one);
  assert(queue.back() == &three);

  queue.ForEachSafe([](Item& item) { return item.value % 2 == 0; });
  assert(!two.InQueue());
  assert(queue.size() == 2);
  assert(queue.front() == &one);
  assert(queue.back() == &three);

  coropact::ds::IntrusiveQueue<Item> moved(std::move(queue));
  assert(queue.empty());
  assert(moved.front() == &one);
  assert(moved.back() == &three);

  coropact::ds::IntrusiveQueue<Item> suffix;
  assert(suffix.PushBack(&four));
  moved.Splice(suffix);
  assert(suffix.empty());
  assert(moved.size() == 3);
  assert(moved.back() == &four);

  coropact::ds::IntrusiveQueue<Item> assigned;
  assigned = std::move(moved);
  assert(moved.empty());
  assert(assigned.size() == 3);
  assert(assigned.PopFront() == &one);
  assert(assigned.PopFront() == &three);
  assert(assigned.PopFront() == &four);
  assert(assigned.PopFront() == nullptr);
  assert(assigned.empty());
  assert(assigned.front() == nullptr);
  assert(assigned.back() == nullptr);

  return 0;
}
