#include "runtime/time/timestamp.h"
#include "runtime/task/task.h"

namespace runtime::task {

std::atomic<int> Task::id_counter_{0}; // 全局任务id自增器

Task::Task(Func f)
    : func_(f),
      id_(id_counter_++) {}

void Task::Run() {
    if (cancelled_) return;
    if (deadline_.Valid() && runtime::time::Timestamp::Now() > deadline_) return;
    func_();
}

} // namespace runtime::task