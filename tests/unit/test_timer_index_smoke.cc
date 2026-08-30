#include <cstdint>
#include <iostream>
#include <vector>

#include "alyrn/detail/timer.h"
#include "alyrn/detail/timer_index.h"

namespace {

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
  alyrn::detail::TimerIndex timers;

  if (!Expect(timers.Empty(), "fresh index should be empty")) {
    return false;
  }

  timers.Insert(&second);
  timers.Insert(&first);
  timers.Insert(&early);

  if (!Expect(timers.Size() == 3, "index should contain all timers") ||
      !Expect(timers.Earliest() == &early, "earliest expiration should sort first") ||
      !Expect(early.InTree(), "inserted timer should link the tree hook")) {
    return false;
  }

  if (!Expect(timers.Erase(&early), "earliest timer should be erasable") ||
      !Expect(!early.InTree(), "erased timer hook should be unlinked") ||
      !Expect(timers.Earliest() == &first, "equal deadlines should use timer sequence order")) {
    return false;
  }

  return true;
}

bool TestPopWhileUnlinksAndPreservesOrder() {
  const auto base = alyrn::time::Deadline{};
  const auto deadline = base + alyrn::time::Seconds(3);
  alyrn::detail::Timer first([] {}, deadline, alyrn::time::Duration::zero());
  alyrn::detail::Timer second([] {}, deadline, alyrn::time::Duration::zero());
  alyrn::detail::Timer later([] {}, base + alyrn::time::Seconds(4),
                                   alyrn::time::Duration::zero());
  alyrn::detail::TimerIndex timers;

  timers.Insert(&later);
  timers.Insert(&second);
  timers.Insert(&first);

  std::vector<std::int64_t> popped_sequences;
  const std::size_t popped = timers.PopWhile(
      [deadline](const alyrn::detail::Timer* timer) {
        return timer->expiration() <= deadline;
      },
      [&](alyrn::detail::Timer* timer) {
        if (!timer->InTree()) {
          popped_sequences.push_back(timer->sequence());
        }
      });

  return Expect(popped == 2, "PopWhile should remove matching timers") &&
         Expect(popped_sequences.size() == 2, "callback should observe unlinked timers") &&
         Expect(popped_sequences[0] == first.sequence(),
                "first equal-deadline timer order mismatch") &&
         Expect(popped_sequences[1] == second.sequence(),
                "second equal-deadline timer order mismatch") &&
         Expect(timers.Earliest() == &later, "non-matching timer should remain in the index");
}

bool TestTimerCanBeReinsertedAfterRestart() {
  const auto base = alyrn::time::Deadline{};
  alyrn::detail::Timer repeating([] {}, base + alyrn::time::Seconds(5),
                                       alyrn::time::Milliseconds(10));
  alyrn::detail::TimerIndex timers;

  timers.Insert(&repeating);
  if (!Expect(timers.Erase(&repeating), "repeating timer should be erasable")) {
    return false;
  }

  repeating.Restart(base + alyrn::time::Seconds(6));
  timers.Insert(&repeating);

  return Expect(repeating.InTree(), "restarted timer should be linked") &&
         Expect(timers.Earliest() == &repeating, "restarted timer should be available as earliest");
}

}  // namespace

int main() {
  if (!TestOrdersByExpirationThenSequence() || !TestPopWhileUnlinksAndPreservesOrder() ||
      !TestTimerCanBeReinsertedAfterRestart())
    return 1;

  std::cout << "[PASS] timer_index_smoke_test\n";
  return 0;
}
