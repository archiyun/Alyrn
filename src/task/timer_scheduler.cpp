#include "runtime/task/timer_scheduler.h"

#include "runtime/task/task.h"
#include "runtime/time/timestamp.h"

#include <chrono>

namespace runtime::task {

TimerScheduler::TimerScheduler()
    : thread_([this](std::stop_token st) { Run(st); }) {}

void TimerScheduler::Schedule(std::weak_ptr<Task> task, uint32_t timeout_ms) {
  const double timeout_sec = static_cast<double>(timeout_ms) / 1000.0;
  const auto deadline =
      runtime::time::AddTime(runtime::time::Timestamp::Now(), timeout_sec);
  {
    std::lock_guard lk{mutex_};
    heap_.push({deadline, std::move(task)});
  }
  cv_.notify_one();
}

void TimerScheduler::Run(std::stop_token stoken) {
  std::unique_lock lk(mutex_);

  while (!stoken.stop_requested()) {
    if (heap_.empty()) {
      cv_.wait(lk, stoken, [this] { return !heap_.empty(); });
      continue;
    }

    const auto next_deadline = heap_.top().deadline;

    // Convert Timestamp to chrono time_point for wait_until.
    const auto chrono_tp = std::chrono::system_clock::time_point{
        std::chrono::microseconds(next_deadline.MicrosecondsSinceEpoch())};

    // Sleep until deadline, or wake early if a new entry with an earlier
    // deadline was pushed (pred returns true → timed_out = false).
    const bool timed_out =
        !cv_.wait_until(lk, stoken, chrono_tp, [this, &next_deadline] {
          return heap_.empty() || heap_.top().deadline < next_deadline;
        });

    if (!timed_out)
      continue; // new earlier entry or stop
    if (stoken.stop_requested())
      break;

    // Fire all entries whose deadline has passed.
    while (!heap_.empty() &&
           heap_.top().deadline <= runtime::time::Timestamp::Now()) {
      auto entry = heap_.top(); // copy (weak_ptr copy is cheap)
      heap_.pop();
      lk.unlock();
      FireTimeout(entry.task);
      lk.lock();
    }
  }
}

void TimerScheduler::FireTimeout(const std::weak_ptr<Task> &weak_task) {
  auto task = weak_task.lock();
  if (!task)
    return;

  const auto state = task->state.load(std::memory_order_acquire);
  if (state != TaskState::kPending && state != TaskState::kRunning)
    return;

  task->timeout_triggered_.store(true, std::memory_order_release);
  task->cancel_source.Cancel();
}
}; // namespace runtime::task
