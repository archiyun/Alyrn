#pragma once

#include "runtime/base/noncopyable.h"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace runtime::task {

class ThreadPool : public runtime::base::NonCopyable {
public:
    explicit ThreadPool(std::size_t thread_count = 0L);
    ~ThreadPool();

    template <typename Func, typename... Args>
    [[nodiscard]] auto enqueue(Func&& f, Args&&... args)
        -> std::future<std::invoke_result_t<Func, Args...>>;

private:
    using Task = std::function<void()>;

    std::vector<std::jthread> workers_;      // 工作线程
    std::queue<Task> tasks_;                 // 任务队列
    std::mutex mutex_;                       // 保护任务队列
    std::condition_variable condition_;      // 队列为空时让工作线程阻塞等待
    bool stop_{false};                       // 线程池是否进入停止流程
    std::atomic<std::size_t> task_count_{0}; // 预留给监控统计
};

template <class F, class... Args>
auto ThreadPool::enqueue(F&& f, Args&&... args)
    -> std::future<std::invoke_result_t<F, Args...>>
{
    using ReturnType = std::invoke_result_t<F, Args...>;

    auto task = std::make_shared<std::packaged_task<ReturnType()>>(
        [f = std::forward<F>(f),
         ... args = std::forward<Args>(args)]() mutable {
            return std::invoke(std::move(f), std::move(args)...);
        });

    std::future<ReturnType> fut = task->get_future();

    {
        std::unique_lock<std::mutex> lock(mutex_);

        if (stop_) {
            throw std::runtime_error("enqueue on stopped ThreadPool");
        }

        tasks_.emplace([task] {
            (*task)();
        });
        ++task_count_;
    }

    condition_.notify_one();

    return fut;
}

}  // namespace runtime::task
