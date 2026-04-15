#include "runtime/task/thread_pool.h"

namespace runtime::task {

ThreadPool::ThreadPool(std::size_t thread_count) {
  if (thread_count == 0) {
    thread_count = std::thread::hardware_concurrency();
  }

  if (thread_count == 0) {
    thread_count = 4;
  }

  workers_.reserve(thread_count);

  for (std::size_t i = 0; i < thread_count; ++i) {
    workers_.emplace_back([this](std::stop_token) {
      while (true) {
        Task task;

        {
          std::unique_lock<std::mutex> lk(mutex_);

          cv_.wait(lk, [this] { return stop_ || !tasks_.empty(); });

          if (stop_ && tasks_.empty()) {
            return;
          }

          task = std::move(tasks_.front());
          tasks_.pop();
        }

        task();
        --task_count_;
      }
    });
  }
}

ThreadPool::~ThreadPool() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_ = true;
  }
  cv_.notify_all();

  // std::jthread joins on destruction; clearing early releases resources
  // as soon as shutdown finishes.
  workers_.clear();
}

} // namespace runtime::task
