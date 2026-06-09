// Demo: IntrusiveRBTree 使用示例
//
// 侵入式红黑树：元素通过公开继承 RBTNode<T> 提供节点存储，
// 树本身不为元素分配内存。
// 场景：一个按截止时间调度的任务调度器。
// 任务按照 deadline_ms 排序，树顶总是最早的任务。

// 测试方法
// 编译:
// cmake --build build --target demo_rbtree -j2
// 运行:
// ./build/examples/foundation/demo_rbtree
//
#include <cstdint>
#include <cstdio>
#include <string>

#include "runtime/ds/intrusive_rbtree.h"

using namespace runtime::ds;

// ------------------------------------------------------------
// 1. 定义任务元素类型，并公开继承红黑树节点
// ------------------------------------------------------------

struct Job : RBTNode<Job> {
  Job(const char* job_name, int64_t deadline, int job_priority)
      : name(job_name), deadline_ms(deadline), priority(job_priority) {}

  std::string name;     // 任务名称
  int64_t deadline_ms;  // 排序关键字：截止时间（毫秒）
  int priority;         // 额外信息，不用于排序
};

// ------------------------------------------------------------
// 2. 定义严格弱序比较函数
// ------------------------------------------------------------

bool JobLess(const Job* a, const Job* b) {
  if (a->deadline_ms != b->deadline_ms)
    return a->deadline_ms < b->deadline_ms;  // 按 deadline 升序排序
  return a->name < b->name;                  // 相同 deadline 用 name 打破平局
}

// ------------------------------------------------------------
// 3. 定义树类型别名
// ------------------------------------------------------------

using JobTree = IntrusiveRBTree<Job, JobLess>;

// ------------------------------------------------------------
// 4. 演示
// ------------------------------------------------------------

int main() {
  JobTree tree;

  // 定义若干任务
  Job jobs[] = {
    {"send_email",    300, 1},
    {"backup_db",     100, 3},
    {"health_check",  200, 2},
    {"gmail",         500, 4},
    {"flush_cache",   100, 2},  // 与 backup_db deadline 相同，用 name 打破平局
  };

  // 插入任务到树中
  for (auto& j : jobs) {
    tree.Insert(&j);
  }
  std::printf("backup_db in tree: %s\n", jobs[1].InTree() ? "yes" : "no");

  // 查看截止时间最早的任务
  Job* earliest = tree.earliest();
  std::printf("Earliest: %s (deadline=%lld)\n",
              earliest->name.c_str(), (long long)earliest->deadline_ms);
  // → backup_db (deadline=100)

  // 删除树中间的任务
  tree.Erase(&jobs[2]);  // 删除 health_check
  std::printf("Erased health_check\n");

  // 弹出所有截止时间 <= 200 的任务
  auto due = tree.PopWhile([](const Job* j) {
    return j->deadline_ms <= 200;
  });

  std::printf("Due jobs (deadline <= 200):\n");
  for (Job* j : due) {
    std::printf("  %s (deadline=%lld)\n",
                j->name.c_str(), (long long)j->deadline_ms);
  }
  // → backup_db (100), flush_cache (100)

  // 树中剩余任务数量
  std::printf("Remaining in tree: %zu\n", tree.size());
  // → 1 (send_email)

  // 回调版本不创建结果 vector；回调执行前，节点已经从树中移除。
  tree.PopWhile(
      [](const Job*) { return true; },
      [](Job* j) {
        std::printf("Drained: %s (in tree: %s)\n",
                    j->name.c_str(), j->InTree() ? "yes" : "no");
      });

  std::printf("Tree empty: %s\n", tree.empty() ? "yes" : "no");
  return 0;
}
