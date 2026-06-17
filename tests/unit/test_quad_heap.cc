#include <cstdint>
#include <deque>
#include <iostream>
#include <random>
#include <vector>

#include "runtime/ds/intrusive_quadheap.h"

namespace {

// Base-hook intrusive node: TimerJob inherits HeapNode<TimerJob>, so the heap
// recovers TimerJob* via static_cast. Inheriting a non-copyable hook makes
// TimerJob non-movable, so tests keep them in std::deque (stable addresses).
struct TimerJob : runtime::ds::HeapNode<TimerJob> {
  TimerJob(int id_, int64_t deadline) : id(id_), deadline_ms(deadline) {}
  int id;
  int64_t deadline_ms;
};

bool TimerJobLess(const TimerJob* a, const TimerJob* b) {
  if (a->deadline_ms != b->deadline_ms) {
    return a->deadline_ms < b->deadline_ms;
  }
  return a->id < b->id;
}

using TimerHeap = runtime::ds::IntrusiveQuadHeap<TimerJob, &TimerJobLess>;

bool Expect(bool condition, const char* message) {
  if (!condition) {
    std::cerr << "[FAIL] " << message << '\n';
    return false;
  }
  return true;
}

bool TestEmpty() {
  TimerHeap heap;
  if (!Expect(heap.empty(), "fresh heap is empty")) return false;
  if (!Expect(heap.size() == 0, "fresh heap size is 0")) return false;
  if (!Expect(heap.earliest() == nullptr, "earliest() on empty is nullptr")) {
    return false;
  }
  return true;
}

bool TestInsertEraseAndPopWhile() {
  TimerHeap heap;
  std::deque<TimerJob> jobs;
  jobs.emplace_back(1, 30);
  jobs.emplace_back(2, 10);
  jobs.emplace_back(3, 20);
  jobs.emplace_back(4, 10);
  jobs.emplace_back(5, 40);

  for (auto& job : jobs) {
    if (!Expect(heap.Insert(&job), "Insert returns true for new element")) {
      return false;
    }
    if (!Expect(job.InHeap(), "InHeap() is true after Insert")) return false;
  }

  if (!Expect(heap.size() == jobs.size(), "heap size after insert")) return false;
  // deadlines 30,10,20,10,40 -> min deadline 10, tie broken by smaller id (2).
  if (!Expect(heap.earliest() == &jobs[1], "earliest after insert")) return false;

  if (!Expect(!heap.Insert(&jobs[1]), "duplicate Insert returns false")) {
    return false;
  }
  if (!Expect(heap.size() == jobs.size(), "duplicate insert keeps size")) {
    return false;
  }

  if (!Expect(heap.Erase(&jobs[1]), "erase existing element")) return false;
  if (!Expect(!jobs[1].InHeap(), "InHeap() is false after Erase")) return false;
  if (!Expect(!heap.Erase(&jobs[1]), "erase missing element returns false")) {
    return false;
  }
  if (!Expect(heap.earliest() == &jobs[3], "earliest after erase")) return false;

  auto popped =
      heap.PopWhile([](const TimerJob* job) { return job->deadline_ms <= 30; });

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

// The second PopWhile overload: no result vector, runs on_pop per element.
bool TestPopWhileOnPop() {
  TimerHeap heap;
  std::deque<TimerJob> jobs;
  for (int i = 0; i < 6; ++i) jobs.emplace_back(i, i * 10);  // deadlines 0..50
  for (auto& job : jobs) heap.Insert(&job);

  std::vector<int> seen;
  std::size_t count = heap.PopWhile(
      [](const TimerJob* job) { return job->deadline_ms < 30; },
      [&](TimerJob* job) { seen.push_back(job->id); });

  if (!Expect(count == 3, "on_pop overload returns popped count")) return false;
  if (!Expect((seen == std::vector<int>{0, 1, 2}), "on_pop sees key order")) {
    return false;
  }
  if (!Expect(heap.size() == 3, "remaining size after on_pop PopWhile")) {
    return false;
  }
  return true;
}

bool TestEraseUnlinkedReturnsFalse() {
  TimerHeap heap;
  TimerJob never_inserted{99, 5};
  if (!Expect(!heap.Erase(&never_inserted), "erase unlinked returns false")) {
    return false;
  }
  if (!Expect(!never_inserted.InHeap(), "unlinked element is not InHeap")) {
    return false;
  }
  return true;
}

// Randomized differential test: against a brute-force sorted reference, the
// heap must always surrender the minimum, across interleaved inserts/erases.
bool TestRandomizedStress() {
  std::mt19937 rng(0xC0FFEE);
  for (int trial = 0; trial < 300; ++trial) {
    TimerHeap heap;
    std::deque<TimerJob> jobs;
    int n = static_cast<int>(rng() % 120) + 1;
    for (int i = 0; i < n; ++i) {
      jobs.emplace_back(i, static_cast<int64_t>(rng() % 500));
    }
    for (auto& job : jobs) heap.Insert(&job);
    if (!Expect(heap.size() == static_cast<std::size_t>(n), "stress: size")) {
      return false;
    }

    // Erase a random ~1/3 of elements.
    std::size_t erased = 0;
    for (int i = 0; i < n; ++i) {
      if (rng() % 3 == 0) {
        if (!Expect(heap.Erase(&jobs[i]), "stress: erase linked")) return false;
        ++erased;
      }
    }
    if (!Expect(heap.size() == static_cast<std::size_t>(n) - erased,
                "stress: size after erase")) {
      return false;
    }

    // Drain: earliest() must be non-decreasing under TimerJobLess.
    const TimerJob* prev = nullptr;
    while (!heap.empty()) {
      TimerJob* top = heap.earliest();
      if (prev != nullptr &&
          !Expect(!TimerJobLess(top, prev), "stress: monotonic drain")) {
        return false;
      }
      prev = top;
      if (!Expect(heap.Erase(top), "stress: drain erase")) return false;
    }
    if (!Expect(heap.empty(), "stress: empty after drain")) return false;
  }
  return true;
}

}  // namespace

int main() {
  if (!TestEmpty()) return 1;
  if (!TestInsertEraseAndPopWhile()) return 1;
  if (!TestPopWhileOnPop()) return 1;
  if (!TestEraseUnlinkedReturnsFalse()) return 1;
  if (!TestRandomizedStress()) return 1;

  std::cout << "[PASS] quad_heap_test\n";
  return 0;
}
