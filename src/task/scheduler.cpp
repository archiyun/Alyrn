#include "runtime/task/scheduler.h"

#include <stdexcept>

namespace runtime::task {

Scheduler::Scheduler(std::size_t worker_count, std::size_t max_queue_size) 
    : pool_(worker_count),
      max_queue_size_(max_queue_size) {}

void Scheduler::Submit(Task task) {
    if (max_queue_size_ > 0 && pending_.load() >= max_queue_size_) {
        throw std::runtime_error("Scheduler: queuefull, task rejected");
    }

    ++pending_;
    [[maybe_unused]] auto fut = pool_.enqueue(
        [t = std::move(task), this]() mutable {
            --pending_;
            t.Run();
        });
}
}