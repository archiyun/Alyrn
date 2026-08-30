#include <concepts>
#include <cstdint>
#include <iostream>
#include <vector>

#include "alyrn/detail/intrusive_rbtree.h"
#include "alyrn/detail/timer.h"
#include "alyrn/detail/timer_index.h"

namespace {

static_assert(
    std::derived_from<alyrn::detail::Timer,
                      alyrn::detail::RBTreeNode<alyrn::detail::Timer>>);

bool Expect(bool condition, const char* message) {
  if (!condition) {
    std::cerr << "[FAIL] " << message << '\n';
    return false;
  }
  return true;
}

bool TestOrdersByExpirationThenSequence() {
  const auto base = alyrn::time::Deadline{};
  const auto early_deadline = base + alyrn::time::Seconds(1);
  const auto late_deadline = base + alyrn::time::Seconds(2);
  alyrn::detail::Timer first([] {}, late_deadline, alyrn::time::Duration::zero());
  alyrn::detail::Timer second([] {}, late_deadline, alyrn::time::Duration::zero());
  alyrn::detail::Timer early([] {}, early_deadline, alyrn::time::Duration::zero());
  alyrn::detail::TimerTree timers;

  if (!Expect(timers.Insert(&second), "insert second") ||
      !Expect(timers.Insert(&first), "insert first") ||
      !Expect(timers.Insert(&early), "insert early")) {
    return false;
  }

  if (!Expect(timers.Size() == 3, "tree should contain all timers") ||
      !Expect(timers.Earliest() == &early, "earliest expiration should sort first") ||
      !Expect(timers.CheckInvariants(), "tree invariants should hold after insertion")) {
    return false;
  }

  if (!Expect(timers.Erase(&early), "earliest timer should be erasable") ||
      !Expect(!early.InTree(), "erased timer hook should be unlinked") ||
      !Expect(timers.Earliest() == &first, "equal deadlines should use timer sequence order")) {
    return false;
  }

  return Expect(timers.CheckInvariants(), "tree invariants should hold after erase");
}

bool TestExtractPrefixUnlinksAndPreservesOrder() {
  const auto base = alyrn::time::Deadline{};
  const auto deadline = base + alyrn::time::Seconds(3);
  alyrn::detail::Timer first([] {}, deadline, alyrn::time::Duration::zero());
  alyrn::detail::Timer second([] {}, deadline, alyrn::time::Duration::zero());
  alyrn::detail::Timer later([] {}, base + alyrn::time::Seconds(4),
                                   alyrn::time::Duration::zero());
  alyrn::detail::TimerTree timers;

  if (!Expect(timers.Insert(&later), "insert later") ||
      !Expect(timers.Insert(&second), "insert second") ||
      !Expect(timers.Insert(&first), "insert first")) {
    return false;
  }

  std::vector<std::int64_t> popped_sequences;
  const std::size_t popped = timers.ExtractPrefix(
      [deadline](const alyrn::detail::Timer* timer) {
        return timer->expiration() <= deadline;
      },
      [&](alyrn::detail::Timer* timer) {
        if (!timer->InTree()) {
          popped_sequences.push_back(timer->sequence());
        }
      });

  if (!Expect(popped == 2, "ExtractPrefix should remove matching timers") ||
      !Expect(popped_sequences.size() == 2, "callback should observe unlinked timers") ||
      !Expect(popped_sequences[0] == first.sequence(),
              "first equal-deadline timer order mismatch") ||
      !Expect(popped_sequences[1] == second.sequence(),
              "second equal-deadline timer order mismatch") ||
      !Expect(timers.Earliest() == &later, "non-matching timer should remain in the tree")) {
    return false;
  }

  return Expect(timers.CheckInvariants(), "tree invariants should hold after ExtractPrefix");
}

bool TestTimerCanBeReinsertedAfterRestart() {
  const auto base = alyrn::time::Deadline{};
  alyrn::detail::Timer repeating([] {}, base + alyrn::time::Seconds(5),
                                       alyrn::time::Milliseconds(10));
  alyrn::detail::TimerTree timers;

  if (!Expect(timers.Insert(&repeating), "insert repeating")) {
    return false;
  }
  if (!Expect(timers.Erase(&repeating), "repeating timer should be erasable")) {
    return false;
  }

  repeating.Restart(base + alyrn::time::Seconds(6));
  if (!Expect(timers.Insert(&repeating), "reinsert repeating")) {
    return false;
  }

  return Expect(repeating.InTree(), "restarted timer should be linked") &&
         Expect(timers.Earliest() == &repeating,
                "restarted timer should be available as earliest") &&
         Expect(timers.CheckInvariants(), "tree invariants should hold after reinsertion");
}

}  // namespace

int main() {
  if (!TestOrdersByExpirationThenSequence()) return 1;
  if (!TestExtractPrefixUnlinksAndPreservesOrder()) return 1;
  if (!TestTimerCanBeReinsertedAfterRestart()) return 1;

  std::cout << "[PASS] timer_tree_smoke_test\n";
  return 0;
}
