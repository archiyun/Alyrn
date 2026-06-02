// Demo: IntrusiveRBTree 使用示例
//
// 侵入式的 红黑树结构， 使用的方法非常简单。
// 类里包含 红黑树节点 成员即可。
// 场景：一个按截止时间调度的任务调度器。
// 任务按照 deadline_ms 排序，树顶总是最早的任务。

// 测试方法
// 编译:
// cmake --B --build
//
#include <cstdint>
#include <cstdio>
#include <string>

#include "runtime/ds/intrusive_rbtree.h"

using namespace runtime::ds;

// ------------------------------------------------------------
// 1. 定义任务元素类型，并在其中嵌入红黑树节点
// ------------------------------------------------------------

struct Job {
  std::string name;       // 任务名称
  int64_t     deadline_ms;   // 排序关键字：截止时间（毫秒）
  int         priority;      // 额外信息，不用于排序
  // 往你需要内嵌类的存入红黑树的节点， 不需要考虑它的存放顺序或位置， 内部维护好了
  RBTNode<Job> tree_node;    // intrusive 节点 —— 树不做堆分配
};

// ------------------------------------------------------------
// 2. 定义比较函数（必须是普通函数指针, 为了编译器能内联采用传地址的写法）
// ------------------------------------------------------------

bool JobLess(const Job* a, const Job* b) {
  if (a->deadline_ms != b->deadline_ms)
    return a->deadline_ms < b->deadline_ms;  // 按 deadline 升序排序
  return a->name < b->name;                  // 相同 deadline 用 name 打破平局
}

// ------------------------------------------------------------
// 3. 定义树类型别名
// ------------------------------------------------------------

using JobTree = IntrusiveRBTree<Job, &Job::tree_node, JobLess>;

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
    {"flush_cache",   100, 2},  // 与 backup_db deadline 相同，用 name 打破平局
  };

  // 插入任务到树中
  for (auto& j : jobs) {
    tree.Insert(&j);
  }

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
  // → flush_cache (100), backup_db (100)

  // 树中剩余任务数量
  std::printf("Remaining in tree: %zu\n", tree.size());
  // → 1 (send_email)

  return 0;
}
