// 对数器：用 std::set 作为标准答案，逐操作与 IntrusiveSplayTree 对比，
// 同时每步验证 BST 结构不变量与缓存 min。
//
// 验证项：
//   1. tree.size()     == oracle.size()
//   2. tree.earliest() == *oracle.begin()  (按 JobLess 排序最小值)
//   3. PopWhile 弹出结果与 oracle 一致（顺序、内容）
//   4. 每步 CheckInvariants() 均为 true
//   5. 未入树 Erase 返回 false，重复 Insert 返回 false 且不改变树
//   6. 有序插删、反向删除、内外交替删除、密集重复 key + PopWhile churn 均与 oracle 一致
//   7. PopWhile(pred, on_pop) 在回调前已删除节点，且不需要结果 vector
//
// 注意：Release 构建不存 per-node tree owner。跨树 Erase 仍是调用方违反
// 前置条件；Debug 构建会通过 owner assert 尽早捕获该错误。
//
// 编译
// cmake -B build-tests
// cmake --build build-tests --target splaytree_validator -j$(nproc)
// 运行
// ./build-tests/tests/splaytree_validator

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <random>
#include <set>
#include <type_traits>
#include <vector>

#include "vexo/ds/intrusive_splaytree.h"

// ----------------------------------------------------------------
// Element type
// ----------------------------------------------------------------

struct Job : vexo::ds::SplayNode<Job> {
  Job() = default;

  int     id;
  int64_t deadline_ms;
};

bool JobLess(const Job* a, const Job* b) {
  if (a->deadline_ms != b->deadline_ms) return a->deadline_ms < b->deadline_ms;
  return a->id < b->id;
}

using JobTree = vexo::ds::IntrusiveSplayTree<Job, JobLess>;

static_assert(!std::is_copy_constructible_v<JobTree>);
static_assert(!std::is_copy_assignable_v<JobTree>);
static_assert(!std::is_move_constructible_v<JobTree>);
static_assert(!std::is_move_assignable_v<JobTree>);

struct JobCmp {
  bool operator()(const Job* a, const Job* b) const { return JobLess(a, b); }
};

struct SecondaryHook {};

struct MultiHookJob : vexo::ds::SplayNode<MultiHookJob>,
                     vexo::ds::SplayNode<MultiHookJob, SecondaryHook> {
  MultiHookJob() = default;

  int key = 0;
};

bool MultiHookJobLess(const MultiHookJob* a, const MultiHookJob* b) {
  return a->key < b->key;
}

using PrimaryMultiHookTree =
    vexo::ds::IntrusiveSplayTree<MultiHookJob, MultiHookJobLess>;
using SecondaryMultiHookTree =
    vexo::ds::IntrusiveSplayTree<MultiHookJob, MultiHookJobLess, SecondaryHook>;

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
  CHECK(tree.size() == oracle.size(), "size mismatch");
  CHECK(tree.CheckInvariants(), "BST invariants violated");
  if (!oracle.empty()) {
    CHECK(tree.earliest() == *oracle.begin(), "earliest() mismatch");
  } else {
    CHECK(tree.earliest() == nullptr, "earliest() should be null on empty tree");
  }
}

static bool CheckAt(bool cond, const char* where, const char* msg) {
  ++g_checks;
  if (!cond) {
    ++g_failures;
    std::printf("FAIL @%s: %s\n", where, msg);
    return false;
  }
  return true;
}

static bool full_check(const JobTree& tree,
                       const std::set<Job*, JobCmp>& oracle,
                       const char* where) {
  bool ok = true;
  ok &= CheckAt(tree.size() == oracle.size(), where, "size mismatch");
  ok &= CheckAt(tree.CheckInvariants(), where, "BST invariants violated");
  if (oracle.empty()) {
    ok &= CheckAt(tree.earliest() == nullptr, where, "earliest should be null");
  } else {
    ok &= CheckAt(tree.earliest() == *oracle.begin(), where, "earliest mismatch");
  }
  return ok;
}

static std::vector<Job> MakeOrderedPool(int size);

static bool lifecycle_test() {
  auto pool = MakeOrderedPool(32);
  {
    JobTree tree;
    for (Job& job : pool) {
      tree.Insert(&job);
    }

    if (!CheckAt(tree.size() == pool.size(), "lifecycle-clear",
                 "tree should contain every element before Clear")) {
      return false;
    }
    tree.Clear();
    if (!CheckAt(tree.empty(), "lifecycle-clear", "Clear should empty the tree") ||
        !CheckAt(tree.earliest() == nullptr, "lifecycle-clear",
                 "Clear should reset earliest")) {
      return false;
    }
    for (Job& job : pool) {
      if (!CheckAt(!job.InTree(), "lifecycle-clear",
                   "Clear should unlink every element")) {
        return false;
      }
    }

    for (Job& job : pool) {
      tree.Insert(&job);
    }
  }

  for (Job& job : pool) {
    if (!CheckAt(!job.InTree(), "lifecycle-destructor",
                 "destructor should unlink every element")) {
      return false;
    }
  }

  JobTree replacement;
  for (Job& job : pool) {
    if (!CheckAt(replacement.Insert(&job), "lifecycle-reinsert",
                 "elements should be reusable after tree destruction")) {
      return false;
    }
  }
  return CheckAt(replacement.CheckInvariants(), "lifecycle-reinsert",
                 "replacement tree invariants should hold");
}

static bool tag_test() {
  MultiHookJob first;
  MultiHookJob second;
  first.key = 1;
  second.key = 2;

  PrimaryMultiHookTree primary;
  SecondaryMultiHookTree secondary;
  if (!CheckAt(primary.Insert(&first), "tag", "primary hook insert failed") ||
      !CheckAt(secondary.Insert(&first), "tag", "secondary hook insert failed") ||
      !CheckAt(primary.Insert(&second), "tag", "second primary insert failed") ||
      !CheckAt(secondary.Insert(&second), "tag", "second secondary insert failed")) {
    return false;
  }

  if (!CheckAt(primary.size() == 2 && secondary.size() == 2, "tag",
               "tagged hooks should maintain independent memberships") ||
      !CheckAt(primary.CheckInvariants() && secondary.CheckInvariants(), "tag",
               "tagged trees should satisfy invariants")) {
    return false;
  }

  primary.Clear();
  secondary.Clear();
  using PrimaryHook = vexo::ds::SplayNode<MultiHookJob>;
  using SecondaryHookNode = vexo::ds::SplayNode<MultiHookJob, SecondaryHook>;
  return CheckAt(!static_cast<PrimaryHook*>(&first)->InTree() &&
                     !static_cast<SecondaryHookNode*>(&first)->InTree() &&
                     !static_cast<PrimaryHook*>(&second)->InTree() &&
                     !static_cast<SecondaryHookNode*>(&second)->InTree(),
                 "tag",
                 "both tagged hooks should be clearable");
}

static std::vector<Job> MakeOrderedPool(int size) {
  std::vector<Job> pool(size);
  for (int i = 0; i < size; ++i) {
    pool[i].id = i;
    pool[i].deadline_ms = i;
  }
  return pool;
}

static bool pattern_test() {
  static constexpr int kSizes[] = {
      1, 2, 3, 5, 8, 16, 32, 64, 128, 256, 512, 1024};

  for (int size : kSizes) {
    {  // ascending insert, ascending delete
      auto pool = MakeOrderedPool(size);
      JobTree tree;
      std::set<Job*, JobCmp> oracle;

      for (int i = 0; i < size; ++i) {
        tree.Insert(&pool[i]);
        oracle.insert(&pool[i]);
        if (!full_check(tree, oracle, "asc-ins")) return false;
      }
      for (int i = 0; i < size; ++i) {
        tree.Erase(&pool[i]);
        oracle.erase(&pool[i]);
        if (!full_check(tree, oracle, "asc-del")) return false;
      }
    }

    {  // ascending insert, descending delete
      auto pool = MakeOrderedPool(size);
      JobTree tree;
      std::set<Job*, JobCmp> oracle;

      for (int i = 0; i < size; ++i) {
        tree.Insert(&pool[i]);
        oracle.insert(&pool[i]);
      }
      for (int i = size - 1; i >= 0; --i) {
        tree.Erase(&pool[i]);
        oracle.erase(&pool[i]);
        if (!full_check(tree, oracle, "asc-ins/desc-del")) return false;
      }
    }

    {  // descending insert, ascending delete
      auto pool = MakeOrderedPool(size);
      JobTree tree;
      std::set<Job*, JobCmp> oracle;

      for (int i = size - 1; i >= 0; --i) {
        tree.Insert(&pool[i]);
        oracle.insert(&pool[i]);
      }
      for (int i = 0; i < size; ++i) {
        tree.Erase(&pool[i]);
        oracle.erase(&pool[i]);
        if (!full_check(tree, oracle, "desc-ins/asc-del")) return false;
      }
    }

    {  // outer-in delete (alternating min/max)
      auto pool = MakeOrderedPool(size);
      JobTree tree;
      std::set<Job*, JobCmp> oracle;

      for (int i = 0; i < size; ++i) {
        tree.Insert(&pool[i]);
        oracle.insert(&pool[i]);
      }

      int lo = 0;
      int hi = size - 1;
      bool from_lo = false;
      while (lo <= hi) {
        const int index = from_lo ? lo++ : hi--;
        from_lo = !from_lo;
        tree.Erase(&pool[index]);
        oracle.erase(&pool[index]);
        if (!full_check(tree, oracle, "outer-in-del")) return false;
      }
    }
  }

  return true;
}

static bool callback_pop_test() {
  auto pool = MakeOrderedPool(8);
  JobTree tree;
  std::set<Job*, JobCmp> oracle;

  for (Job& job : pool) {
    tree.Insert(&job);
    oracle.insert(&job);
  }

  std::vector<Job*> popped;
  bool callback_ok = true;
  const std::size_t count = tree.PopWhile(
      [](const Job* job) { return job->deadline_ms <= 4; },
      [&](Job* job) {
        callback_ok &= CheckAt(!job->InTree(), "callback-pop",
                               "callback saw linked node");
        popped.push_back(job);
      });

  std::vector<Job*> expected;
  for (Job* job : oracle) {
    if (job->deadline_ms > 4) break;
    expected.push_back(job);
  }

  if (!CheckAt(callback_ok, "callback-pop", "callback failed")) return false;
  if (!CheckAt(count == expected.size(), "callback-pop", "count mismatch")) {
    return false;
  }
  if (!CheckAt(popped.size() == expected.size(), "callback-pop",
               "popped size mismatch")) {
    return false;
  }
  for (std::size_t i = 0; i < popped.size(); ++i) {
    if (!CheckAt(popped[i] == expected[i], "callback-pop",
                 "popped order mismatch")) {
      return false;
    }
  }
  for (Job* job : expected) {
    oracle.erase(job);
  }

  return full_check(tree, oracle, "callback-pop");
}

static bool churn_test(uint64_t seed, int pool_size, int ops, int keyspace) {
  std::mt19937_64 rng(seed);
  std::vector<Job> pool(pool_size);
  for (int i = 0; i < pool_size; ++i) {
    pool[i].id = i;
    pool[i].deadline_ms = static_cast<int64_t>(rng() % keyspace);
  }

  JobTree tree;
  std::set<Job*, JobCmp> oracle;
  std::uniform_int_distribution<int> pick(0, pool_size - 1);

  for (int step = 0; step < ops; ++step) {
    const int r = static_cast<int>(rng() % 100);
    if (r < 55) {
      Job* job = &pool[pick(rng)];
      if (!job->InTree()) {
        tree.Insert(job);
        oracle.insert(job);
      } else {
        tree.Erase(job);
        oracle.erase(job);
      }
      if (!full_check(tree, oracle, "churn-ie")) return false;
      continue;
    }

    const int64_t threshold = static_cast<int64_t>(rng() % keyspace);
    std::vector<Job*> popped;
    bool callback_ok = true;
    const std::size_t popped_count = tree.PopWhile(
        [threshold](const Job* job) {
          return job->deadline_ms <= threshold;
        },
        [&](Job* job) {
          callback_ok &= CheckAt(!job->InTree(), "churn-pop",
                                 "callback saw linked node");
          popped.push_back(job);
        });

    std::vector<Job*> expected;
    for (Job* job : oracle) {
      if (job->deadline_ms > threshold) break;
      expected.push_back(job);
    }

    if (!callback_ok) return false;
    if (!CheckAt(popped_count == expected.size(), "churn-pop",
                 "PopWhile callback count mismatch")) {
      return false;
    }
    if (!CheckAt(popped.size() == expected.size(), "churn-pop",
                 "PopWhile size mismatch")) {
      return false;
    }
    for (std::size_t i = 0; i < popped.size(); ++i) {
      if (!CheckAt(popped[i] == expected[i], "churn-pop",
                   "PopWhile order mismatch")) {
        return false;
      }
    }
    for (Job* job : expected) {
      oracle.erase(job);
    }

    if (!full_check(tree, oracle, "churn-pop")) return false;
  }

  return true;
}

static bool churn_campaigns() {
  int total = 0;
  int failures = 0;

  for (uint64_t seed = 1; seed <= 32; ++seed) {
    ++total;
    if (!churn_test(seed, 200, 2000, 30)) ++failures;

    ++total;
    if (!churn_test(seed + 9000, 400, 3000, 200000)) ++failures;

    if (failures > 0) break;
  }

  std::printf("churn campaigns: %d/%d passed, %d failed\n",
              total - failures, total, failures);
  return failures == 0;
}

// ----------------------------------------------------------------
// Main validator
// ----------------------------------------------------------------

int main() {
  constexpr int kPoolSize = 20000;
  constexpr int kNumOps   = 2000000;
  constexpr int kDeadlineRange = 10000;

  if (!lifecycle_test()) {
    std::printf("LIFECYCLE TESTS FAILED\n");
    return 1;
  }

  if (!tag_test()) {
    std::printf("TAG TESTS FAILED\n");
    return 1;
  }

  if (!pattern_test()) {
    std::printf("PATTERN TESTS FAILED\n");
    return 1;
  }
  std::printf("pattern tests: PASS\n");

  if (!callback_pop_test()) {
    std::printf("CALLBACK POP TESTS FAILED\n");
    return 1;
  }

  if (!churn_campaigns()) {
    std::printf("CHURN TESTS FAILED\n");
    return 1;
  }

  std::mt19937 rng(42);

  // Pre-allocate job pool with fixed deadlines
  std::vector<Job> pool(kPoolSize);
  for (int i = 0; i < kPoolSize; i++) {
    pool[i].id          = i;
    pool[i].deadline_ms = rng() % kDeadlineRange;
  }

  JobTree tree;
  std::set<Job*, JobCmp> oracle;
  // active tracks which jobs are currently in the tree (for O(1) random pick)
  std::vector<Job*> active;

  {
    Job a{};
    a.id = -1;
    a.deadline_ms = 10;

    int op_idx = -1;
    CHECK(!tree.Erase(&a), "erase unlinked element should fail");
    CHECK(tree.Insert(&a), "first insert should succeed");
    CHECK(!tree.Insert(&a), "duplicate insert should return false");
    CHECK(tree.size() == 1, "duplicate insert should be ignored");
    CHECK(tree.earliest() == &a, "single inserted element should be earliest");
    CHECK(tree.CheckInvariants(), "single-node tree invariants");
    CHECK(tree.Erase(&a), "erase linked element should succeed");
    CHECK(tree.empty(), "tree should be empty after erasing only element");
  }

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
      if (!j->InTree()) {
        CHECK(tree.Insert(j), "insert of unlinked element should succeed");
        oracle.insert(j);
        active.push_back(j);
      } else {
        CHECK(!tree.Insert(j), "insert of linked element should return false");
      }

    } else if (r < 85) {
      // ---- Erase ----
      Job* j = active[rng() % active.size()];
      CHECK(tree.Erase(j), "erase of linked element should succeed");
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
  std::printf("Draining remaining %zu elements...\n", tree.size());
  std::vector<Job*> drain_tree, drain_oracle(oracle.begin(), oracle.end());
  while (!tree.empty()) {
    drain_tree.push_back(tree.earliest());
    tree.Erase(tree.earliest());
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
