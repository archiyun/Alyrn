#include "runtime/task/task.h"

namespace runtime::task {

Task::Task(uint64_t id, std::string name, TaskPriority priority, Func func)
  : id(id),
    name(std::move(name)),
    priority(priority),
    func(std::move(func)) {}

}  // namespace runtime::task
