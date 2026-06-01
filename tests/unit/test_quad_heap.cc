#include <cstdint>
#include <iostream>
#include <vector>

#include "runtime/base/quad_heap.h"

namespace {

struct TimerJob {
  int id;
  int64_t deadline_ms;
  runtime::base::HeapNode<TimerJob> heap_node;
};

bool TimerJobLess(const TimerJob* a, const TimerJob* b) {
  if (a->deadline_ms != b->deadline_ms) {
    return a->deadline_ms < b->deadline_ms;
  }
  return a->id < b->id;
}

using TimerHeap = runtime::base::IntrusiveQuadHeap<TimerJob, &TimerJob::heap_node, TimerJobLess>;

bool Expect(bool condition, const char* message) {
  if (!condition) {
    std::cerr << "[FAIL] " << message << '\n';
    return false;
  }
  return true;
}

bool TestInsertEraseAndPopWhile() {
  TimerHeap heap;
  std::vector<TimerJob> jobs = {
      {1, 30, {}}, {2, 10, {}}, {3, 20, {}}, {4, 10, {}}, {5, 40, {}},
  };

  for (auto& job : jobs) {
    heap.Insert(&job);
  }

  if (!Expect(heap.size() == jobs.size(), "heap size after insert")) return false;
  if (!Expect(heap.earliest() == &jobs[1], "earliest after insert")) return false;

  heap.Insert(&jobs[1]);
  if (!Expect(heap.size() == jobs.size(), "duplicate insert should be ignored")) {
    return false;
  }

  if (!Expect(heap.Erase(&jobs[1]), "erase existing element")) return false;
  if (!Expect(!heap.Erase(&jobs[1]), "erase missing element")) return false;
  if (!Expect(heap.earliest() == &jobs[3], "earliest after erase")) return false;

  auto popped = heap.PopWhile([](const TimerJob* job) { return job->deadline_ms <= 30; });

  std::vector<TimerJob*> expected = {&jobs[3], &jobs[2], &jobs[0]};
  if (!Expect(popped == expected, "PopWhile returns sorted matching elements")) {
    return false;
  }
  if (!Expect(heap.size() == 1, "heap size after PopWhile")) return false;
  if (!Expect(heap.earliest() == &jobs[4], "remaining earliest after PopWhile")) {
    return false;
  }
  return true;
}

bool TestCrossHeapEraseIsIgnored() {
  TimerJob a{1, 10, {}};
  TimerJob b{2, 20, {}};
  TimerHeap first;
  TimerHeap second;

  first.Insert(&a);
  second.Insert(&b);

  if (!Expect(!second.Erase(&a), "cross-heap erase should fail")) return false;
  if (!Expect(first.size() == 1, "cross-heap erase should preserve source")) {
    return false;
  }
  if (!Expect(second.size() == 1, "cross-heap erase should preserve target")) {
    return false;
  }
  return first.Erase(&a) && second.Erase(&b);
}

}  // namespace

int main() {
  if (!TestInsertEraseAndPopWhile()) return 1;
  if (!TestCrossHeapEraseIsIgnored()) return 1;

  std::cout << "[PASS] quad_heap_test\n";
  return 0;
}
