#include <concepts>
#include <cstdint>
#include <iostream>
#include <vector>

#include "runtime/ds/intrusive_rbtree.h"
#include "runtime/time/timer.h"
#include "runtime/time/timer_tree.h"
#include "runtime/time/timestamp.h"

namespace {

static_assert(
    std::derived_from<runtime::time::Timer,
                      runtime::ds::RBTNode<runtime::time::Timer>>);

bool Expect(bool condition, const char* message) {
  if (!condition) {
    std::cerr << "[FAIL] " << message << '\n';
    return false;
  }
  return true;
}

bool TestOrdersByExpirationThenSequence() {
  const runtime::time::Timestamp early_deadline(1'000'000);
  const runtime::time::Timestamp late_deadline(2'000'000);
  runtime::time::Timer first([] {}, late_deadline, 0.0);
  runtime::time::Timer second([] {}, late_deadline, 0.0);
  runtime::time::Timer early([] {}, early_deadline, 0.0);
  runtime::time::TimerTree timers;

  timers.Insert(&second);
  timers.Insert(&first);
  timers.Insert(&early);

  if (!Expect(timers.size() == 3, "tree should contain all timers") ||
      !Expect(timers.earliest() == &early,
              "earliest expiration should sort first") ||
      !Expect(timers.CheckRBInvariants(),
              "tree invariants should hold after insertion")) {
    return false;
  }

  if (!Expect(timers.Erase(&early), "earliest timer should be erasable") ||
      !Expect(!early.InTree(), "erased timer hook should be unlinked") ||
      !Expect(timers.earliest() == &first,
              "equal deadlines should use timer sequence order")) {
    return false;
  }

  return Expect(timers.CheckRBInvariants(),
                "tree invariants should hold after erase");
}

bool TestPopWhileUnlinksAndPreservesOrder() {
  const runtime::time::Timestamp deadline(3'000'000);
  runtime::time::Timer first([] {}, deadline, 0.0);
  runtime::time::Timer second([] {}, deadline, 0.0);
  runtime::time::Timer later(
      [] {}, runtime::time::Timestamp(4'000'000), 0.0);
  runtime::time::TimerTree timers;

  timers.Insert(&later);
  timers.Insert(&second);
  timers.Insert(&first);

  std::vector<std::int64_t> popped_sequences;
  const std::size_t popped = timers.PopWhile(
      [deadline](const runtime::time::Timer* timer) {
        return timer->expiration() <= deadline;
      },
      [&](runtime::time::Timer* timer) {
        if (!timer->InTree()) {
          popped_sequences.push_back(timer->sequence());
        }
      });

  if (!Expect(popped == 2, "PopWhile should remove matching timers") ||
      !Expect(popped_sequences.size() == 2,
              "callback should observe unlinked timers") ||
      !Expect(popped_sequences[0] == first.sequence(),
              "first equal-deadline timer order mismatch") ||
      !Expect(popped_sequences[1] == second.sequence(),
              "second equal-deadline timer order mismatch") ||
      !Expect(timers.earliest() == &later,
              "non-matching timer should remain in the tree")) {
    return false;
  }

  return Expect(timers.CheckRBInvariants(),
                "tree invariants should hold after PopWhile");
}

bool TestTimerCanBeReinsertedAfterRestart() {
  runtime::time::Timer repeating(
      [] {}, runtime::time::Timestamp(5'000'000), 0.01);
  runtime::time::TimerTree timers;

  timers.Insert(&repeating);
  if (!Expect(timers.Erase(&repeating), "repeating timer should be erasable")) {
    return false;
  }

  repeating.Restart(runtime::time::Timestamp(6'000'000));
  timers.Insert(&repeating);

  return Expect(repeating.InTree(), "restarted timer should be linked") &&
         Expect(timers.earliest() == &repeating,
                "restarted timer should be available as earliest") &&
         Expect(timers.CheckRBInvariants(),
                "tree invariants should hold after reinsertion");
}

}  // namespace

int main() {
  if (!TestOrdersByExpirationThenSequence()) return 1;
  if (!TestPopWhileUnlinksAndPreservesOrder()) return 1;
  if (!TestTimerCanBeReinsertedAfterRestart()) return 1;

  std::cout << "[PASS] timer_tree_smoke_test\n";
  return 0;
}
