#include <cstdint>
#include <iostream>
#include <vector>

#include "coropact/time/timer.h"
#include "coropact/time/timer_index.h"

namespace {

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
  coropact::time::TimerIndex timers;

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
  const auto base = coropact::time::Deadline{};
  const auto deadline = base + coropact::time::Seconds(3);
  coropact::time::Timer first([] {}, deadline, coropact::time::Duration::zero());
  coropact::time::Timer second([] {}, deadline, coropact::time::Duration::zero());
  coropact::time::Timer later([] {}, base + coropact::time::Seconds(4),
                              coropact::time::Duration::zero());
  coropact::time::TimerIndex timers;

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

  return Expect(popped == 2, "PopWhile should remove matching timers") &&
         Expect(popped_sequences.size() == 2, "callback should observe unlinked timers") &&
         Expect(popped_sequences[0] == first.sequence(),
                "first equal-deadline timer order mismatch") &&
         Expect(popped_sequences[1] == second.sequence(),
                "second equal-deadline timer order mismatch") &&
         Expect(timers.Earliest() == &later, "non-matching timer should remain in the index");
}

bool TestTimerCanBeReinsertedAfterRestart() {
  const auto base = coropact::time::Deadline{};
  coropact::time::Timer repeating([] {}, base + coropact::time::Seconds(5),
                                  coropact::time::Milliseconds(10));
  coropact::time::TimerIndex timers;

  timers.Insert(&repeating);
  if (!Expect(timers.Erase(&repeating), "repeating timer should be erasable")) {
    return false;
  }

  repeating.Restart(base + coropact::time::Seconds(6));
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
