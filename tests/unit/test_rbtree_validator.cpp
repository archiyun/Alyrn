// 对数器：用 std::set 作为标准答案，逐操作与 IntrusiveRBTree 对比，
// 同时每步验证红黑树结构不变量。
//
// 验证项：
//   1. tree.Size()     == oracle.size()
//   2. tree.Earliest() == *oracle.begin()  (按 JobLess 排序最小值)
//   3. PopWhile 弹出结果与 oracle 一致（顺序、内容）
//   4. 每步 CheckRBInvariants() 均为 true
//
// 编译
// cmake -B build-tests 2>&1 | tail -3 && \
cmake --build build-tests --target rbtree_validator -j$(nproc) 2>&1 | tail -8
// 运行
// ./build-tests/tests/rbtree_validator

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <random>
#include <set>
#include <vector>

#include "runtime/base/rbtree.h"

// ----------------------------------------------------------------
// Element type
// ----------------------------------------------------------------

struct Job {
  int         id;
  int64_t     deadline_ms;
  runtime::base::RBTNode<Job> node;
};

bool JobLess(const Job* a, const Job* b) {
  if (a->deadline_ms != b->deadline_ms) return a->deadline_ms < b->deadline_ms;
  return a->id < b->id;
}

using JobTree = runtime::base::IntrusiveRBTree<Job, &Job::node, JobLess>;

struct JobCmp {
  bool operator()(const Job* a, const Job* b) const { return JobLess(a, b); }
};

// ----------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------

static int g_checks = 0;
static int g_failures = 0;

#define CHECK(cond, msg) do { \
  ++g_checks; \
  if (!(cond)) { ++g_failures; std::printf("FAIL op=%d: %s\n", op_idx, msg); } \
} while(0)

static void verify(const JobTree& tree,
                   const std::set<Job*, JobCmp>& oracle,
                   int op_idx) {
  CHECK(tree.Size() == oracle.size(), "size mismatch");
  CHECK(tree.CheckRBInvariants(), "RB invariants violated");
  if (!oracle.empty()) {
    CHECK(tree.Earliest() == *oracle.begin(), "Earliest() mismatch");
  } else {
    CHECK(tree.Earliest() == nullptr, "Earliest() should be null on empty tree");
  }
}

// ----------------------------------------------------------------
// Main validator
// ----------------------------------------------------------------

int main() {
  constexpr int kPoolSize = 20000;
  constexpr int kNumOps   = 10000000;    
  constexpr int kDeadlineRange = 10000;

  std::mt19937 rng(42);

  // Pre-allocate job pool with fixed deadlines
  std::vector<Job> pool(kPoolSize);
  for (int i = 0; i < kPoolSize; i++) {
    pool[i].id          = i;
    pool[i].deadline_ms = rng() % kDeadlineRange;
  }

  JobTree tree;
  std::set<Job*, JobCmp> oracle;
  // active_vec tracks which jobs are currently in the tree (for O(1) random pick)
  std::vector<Job*> active;

  auto remove_from_active = [&](Job* j) {
    auto it = std::find(active.begin(), active.end(), j);
    if (it != active.end()) { *it = active.back(); active.pop_back(); }
  };

  std::printf("Running %d operations on pool of %d jobs...\n", kNumOps, kPoolSize);

  for (int op_idx = 0; op_idx < kNumOps; ++op_idx) {
    int r = rng() % 100;

    if (r < 55 || active.empty()) {
      // ---- Insert ----
      Job* j = &pool[rng() % kPoolSize];
      if (!j->node.in_tree) {
        tree.Insert(j);
        oracle.insert(j);
        active.push_back(j);
      }

    } else if (r < 85) {
      // ---- Erase ----
      Job* j = active[rng() % active.size()];
      tree.Erase(j);
      oracle.erase(j);
      remove_from_active(j);

    } else {
      // ---- PopWhile (deadline <= threshold) ----
      int64_t threshold = rng() % kDeadlineRange;

      auto popped = tree.PopWhile([threshold](const Job* j) {
        return j->deadline_ms <= threshold;
      });

      // Oracle: extract same elements in order
      std::vector<Job*> oracle_popped;
      while (!oracle.empty() && (*oracle.begin())->deadline_ms <= threshold) {
        oracle_popped.push_back(*oracle.begin());
        oracle.erase(oracle.begin());
      }

      CHECK(popped.size() == oracle_popped.size(), "PopWhile size mismatch");
      for (size_t i = 0; i < std::min(popped.size(), oracle_popped.size()); i++) {
        CHECK(popped[i] == oracle_popped[i], "PopWhile element mismatch");
      }

      for (Job* j : popped) remove_from_active(j);
    }

    verify(tree, oracle, op_idx);
  }

  // ---- Final drain: compare full sorted order ----
  std::printf("Draining remaining %zu elements...\n", tree.Size());
  std::vector<Job*> drain_tree, drain_oracle(oracle.begin(), oracle.end());
  while (!tree.Empty()) {
    drain_tree.push_back(tree.Earliest());
    tree.Erase(tree.Earliest());
  }
  int drain_ok = 1;
  if (drain_tree.size() == drain_oracle.size()) {
    for (size_t i = 0; i < drain_tree.size(); i++) {
      if (drain_tree[i] != drain_oracle[i]) { drain_ok = 0; break; }
    }
  } else {
    drain_ok = 0;
  }
  if (!drain_ok) { ++g_failures; std::printf("FAIL: final drain order mismatch\n"); }
  ++g_checks;

  std::printf("\n%d checks, %d failures\n", g_checks, g_failures);
  return g_failures == 0 ? 0 : 1;
}
