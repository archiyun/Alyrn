#include <concepts>
#include <cstdint>
#include <iostream>
#include <vector>

#include "coropact/ds/intrusive_quadheap.h"
#include "coropact/ds/intrusive_rbtree.h"
#include "coropact/time/timer.h"
#include "coropact/time/timer_tree.h"

namespace {

static_assert(
    std::derived_from<coropact::time::Timer, coropact::ds::RBTNode<coropact::time::Timer>>);
static_assert(
    std::derived_from<coropact::time::Timer, coropact::ds::HeapNode<coropact::time::Timer>>);

bool Expect(bool condition, const char* message) {
  if (!condition) {
    std::cerr << "[FAIL] " << message << '\n';
    return false;
  }
  return true;
}

bool TestOrdersByExpirationThenSequence() {
  const auto base = coropact::time::Deadline{};
  const auto early_deadline = base + coropact::time::Seconds(1);
  const auto late_deadline = base + coropact::time::Seconds(2);
  coropact::time::Timer first([] {}, late_deadline, coropact::time::Duration::zero());
  coropact::time::Timer second([] {}, late_deadline, coropact::time::Duration::zero());
  coropact::time::Timer early([] {}, early_deadline, coropact::time::Duration::zero());
  coropact::time::TimerTree timers;

  timers.Insert(&second);
  timers.Insert(&first);
  timers.Insert(&early);

  if (!Expect(timers.Size() == 3, "tree should contain all timers") ||
      !Expect(timers.Earliest() == &early, "earliest expiration should sort first") ||
      !Expect(timers.CheckRBInvariants(), "tree invariants should hold after insertion")) {
    return false;
  }

  if (!Expect(timers.Erase(&early), "earliest timer should be erasable") ||
      !Expect(!early.InTree(), "erased timer hook should be unlinked") ||
      !Expect(timers.Earliest() == &first, "equal deadlines should use timer sequence order")) {
    return false;
  }

  return Expect(timers.CheckRBInvariants(), "tree invariants should hold after erase");
}

bool TestPopWhileUnlinksAndPreservesOrder() {
  const auto base = coropact::time::Deadline{};
  const auto deadline = base + coropact::time::Seconds(3);
  coropact::time::Timer first([] {}, deadline, coropact::time::Duration::zero());
  coropact::time::Timer second([] {}, deadline, coropact::time::Duration::zero());
  coropact::time::Timer later([] {}, base + coropact::time::Seconds(4),
                              coropact::time::Duration::zero());
  coropact::time::TimerTree timers;

  timers.Insert(&later);
  timers.Insert(&second);
  timers.Insert(&first);

  std::vector<std::int64_t> popped_sequences;
  const std::size_t popped = timers.PopWhile(
      [deadline](const coropact::time::Timer* timer) { return timer->expiration() <= deadline; },
      [&](coropact::time::Timer* timer) {
        if (!timer->InTree()) {
          popped_sequences.push_back(timer->sequence());
        }
      });

  if (!Expect(popped == 2, "PopWhile should remove matching timers") ||
      !Expect(popped_sequences.size() == 2, "callback should observe unlinked timers") ||
      !Expect(popped_sequences[0] == first.sequence(),
              "first equal-deadline timer order mismatch") ||
      !Expect(popped_sequences[1] == second.sequence(),
              "second equal-deadline timer order mismatch") ||
      !Expect(timers.Earliest() == &later, "non-matching timer should remain in the tree")) {
    return false;
  }

  return Expect(timers.CheckRBInvariants(), "tree invariants should hold after PopWhile");
}

bool TestTimerCanBeReinsertedAfterRestart() {
  const auto base = coropact::time::Deadline{};
  coropact::time::Timer repeating([] {}, base + coropact::time::Seconds(5),
                                  coropact::time::Milliseconds(10));
  coropact::time::TimerTree timers;

  timers.Insert(&repeating);
  if (!Expect(timers.Erase(&repeating), "repeating timer should be erasable")) {
    return false;
  }

  repeating.Restart(base + coropact::time::Seconds(6));
  timers.Insert(&repeating);

  return Expect(repeating.InTree(), "restarted timer should be linked") &&
         Expect(timers.Earliest() == &repeating,
                "restarted timer should be available as earliest") &&
         Expect(timers.CheckRBInvariants(), "tree invariants should hold after reinsertion");
}

}  // namespace

int main() {
  if (!TestOrdersByExpirationThenSequence()) return 1;
  if (!TestPopWhileUnlinksAndPreservesOrder()) return 1;
  if (!TestTimerCanBeReinsertedAfterRestart()) return 1;

  std::cout << "[PASS] timer_tree_smoke_test\n";
  return 0;
}
